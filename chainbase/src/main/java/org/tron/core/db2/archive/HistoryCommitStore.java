package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only, checksummed commit log with constant resident state.
 *
 * <p>All records in one generation have the same descriptor participant set and therefore the
 * same encoded length. Contiguous epochs can be addressed directly without retaining one object
 * or creating one directory entry per block.
 */
public final class HistoryCommitStore implements Closeable, CommittedHistoryAuthority {

  private static final String FILE_NAME = "commit.log";

  private final Path directory;
  private final Path logPath;
  private final HistoryCommitMarkerCodec codec;
  private final FileChannel channel;
  private final DirectorySync postForceHook;
  private HistoryCommitMarker head;
  private long uncertainFrom = -1;
  private long uncertainThrough = -1;
  private long firstEpoch = -1;
  private long recordCount;
  private int recordLength;
  private long startupScannedRecords;

  public HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec)
      throws IOException {
    this(archiveDirectory, codec, null, ignored -> { });
  }

  HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec,
      DirectorySync postForceHook) throws IOException {
    this(archiveDirectory, codec, null, postForceHook);
  }

  HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec,
      ArchiveHistoryScanAnchor checkpoint) throws IOException {
    this(archiveDirectory, codec, checkpoint, ignored -> { });
  }

  HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec,
      ArchiveHistoryScanAnchor checkpoint, DirectorySync postForceHook) throws IOException {
    this.directory = archiveDirectory.resolve("commits");
    this.logPath = directory.resolve(FILE_NAME);
    this.codec = codec;
    this.postForceHook = postForceHook;
    Files.createDirectories(directory);
    boolean created = !Files.exists(logPath);
    this.channel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
    if (created) {
      HistorySegmentStore.syncDirectory(directory);
    }
    scanAndRepairTruncatedTail(checkpoint);
    channel.position(channel.size());
  }

  public synchronized void commit(HistoryCommitMarker marker) throws IOException {
    commitAll(java.util.Collections.singletonList(marker));
  }

  public synchronized void commitAll(List<HistoryCommitMarker> batch) throws IOException {
    if (batch.isEmpty()) {
      return;
    }
    if (uncertainFrom >= 0) {
      throw new IllegalStateException("A previous commit record has uncertain durability");
    }
    HistoryCommitMarker previous = head;
    List<byte[]> encodedBatch = new ArrayList<>(batch.size());
    int expectedLength = recordLength;
    for (HistoryCommitMarker marker : batch) {
      validateNext(previous, marker);
      byte[] encoded = codec.encode(marker);
      if (expectedLength != 0 && encoded.length != expectedLength) {
        throw new IllegalArgumentException(
            "Commit record length changed inside one archive generation");
      }
      expectedLength = encoded.length;
      encodedBatch.add(encoded);
      previous = marker;
    }
    long offset = channel.size();
    channel.position(offset);
    for (byte[] encoded : encodedBatch) {
      writeFully(channel, ByteBuffer.wrap(encoded));
    }
    uncertainFrom = batch.get(0).getMeta().getEpoch();
    uncertainThrough = batch.get(batch.size() - 1).getMeta().getEpoch();
    try {
      channel.force(true);
      postForceHook.sync(directory);
    } catch (IOException failure) {
      throw failure;
    }
    if (recordCount == 0) {
      firstEpoch = batch.get(0).getMeta().getEpoch();
      recordLength = expectedLength;
    }
    recordCount += batch.size();
    head = batch.get(batch.size() - 1);
    uncertainFrom = -1;
    uncertainThrough = -1;
  }

  public synchronized void removeHead(BlockSnapshotMeta expected) throws IOException {
    if (uncertainFrom >= 0) {
      throw new IllegalStateException("Cannot revert a commit record with uncertain durability");
    }
    if (head == null || !head.getMeta().equals(expected)) {
      throw new IllegalStateException("Only the committed history head can be reverted");
    }
    long newCount = recordCount - 1;
    channel.truncate(newCount * (long) recordLength);
    channel.force(true);
    recordCount = newCount;
    if (newCount == 0) {
      head = null;
      firstEpoch = -1;
      recordLength = 0;
    } else {
      head = readOrdinal(newCount - 1);
    }
    channel.position(channel.size());
  }

  /** Durably removes every committed marker after {@code lastEpoch}. */
  public synchronized void truncateAfter(long lastEpoch) throws IOException {
    if (uncertainFrom >= 0) {
      throw new IllegalStateException("Cannot truncate commit records with uncertain durability");
    }
    HistoryCommitMarker last = get(lastEpoch);
    if (last == null) {
      throw new IllegalArgumentException("Commit truncation target is outside the committed prefix");
    }
    long newCount = lastEpoch - firstEpoch + 1;
    if (newCount == recordCount) {
      return;
    }
    channel.truncate(newCount * (long) recordLength);
    channel.force(true);
    postForceHook.sync(directory);
    recordCount = newCount;
    head = last;
    channel.position(channel.size());
  }

  @Override
  public synchronized HistoryCommitMarker head() {
    return head;
  }

  public synchronized long size() {
    return recordCount;
  }

  @Override
  public synchronized long firstEpoch() {
    return firstEpoch;
  }

  /** Returns one atomic height-coverage snapshot of the validated contiguous commit log. */
  @Override
  public synchronized HistoryCoverage coverage() {
    return head == null ? null : new HistoryCoverage(firstEpoch, recordCount,
        head.getMeta().getEpoch(), head.getMeta().getBlockHash());
  }

  /** Materializes the committed prefix. Do not use this method in the scale ingestion path. */
  public synchronized List<HistoryCommitMarker> getMarkers() {
    if (recordCount > Integer.MAX_VALUE) {
      throw new IllegalStateException("Commit prefix is too large to materialize");
    }
    List<HistoryCommitMarker> markers = new ArrayList<>((int) recordCount);
    try {
      for (long ordinal = 0; ordinal < recordCount; ordinal++) {
        markers.add(readOrdinal(ordinal));
      }
      return markers;
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to materialize commit prefix", failure);
    }
  }

  @Override
  public synchronized HistoryCommitMarker get(long epoch) {
    if (recordCount == 0 || epoch < firstEpoch || epoch - firstEpoch >= recordCount) {
      return null;
    }
    try {
      return readOrdinal(epoch - firstEpoch);
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to read history commit epoch " + epoch,
          failure);
    }
  }

  public synchronized boolean mayContain(long epoch) {
    return get(epoch) != null
        || (uncertainFrom >= 0 && epoch >= uncertainFrom && epoch <= uncertainThrough);
  }

  Path getLogPath() {
    return logPath;
  }

  int getRecordLength() {
    return recordLength;
  }

  long getStartupScannedRecords() {
    return startupScannedRecords;
  }

  private void scanAndRepairTruncatedTail(ArchiveHistoryScanAnchor checkpoint)
      throws IOException {
    long offset = 0;
    HistoryCommitMarker previous = null;
    int expectedLength = 0;
    long count = 0;
    long size = channel.size();
    if (checkpoint != null) {
      expectedLength = checkpoint.getCommitRecordLength();
      count = checkpoint.getRecordCount();
      firstEpoch = checkpoint.getFirstEpoch();
      long checkpointOffset = (count - 1) * (long) expectedLength;
      if (checkpointOffset < 0 || checkpointOffset + expectedLength > size) {
        throw new ArchivePersistenceException(
            "History scan anchor is outside the committed history log");
      }
      byte[] checkpointRecord = read(checkpointOffset, expectedLength);
      startupScannedRecords++;
      if (!java.util.Arrays.equals(checkpointRecord, checkpoint.getEncodedMarker())) {
        throw new ArchivePersistenceException(
            "History scan anchor does not match the committed history log");
      }
      previous = codec.decode(checkpointRecord);
      offset = checkpointOffset + expectedLength;
    }
    while (offset < size) {
      long remaining = size - offset;
      if (remaining < HistoryCommitMarkerCodec.HEADER_LENGTH) {
        truncateTail(offset);
        size = offset;
        break;
      }
      byte[] prefix = read(offset, HistoryCommitMarkerCodec.HEADER_LENGTH);
      int length;
      try {
        length = codec.recordLength(prefix);
      } catch (IllegalArgumentException invalidHeader) {
        throw new ArchivePersistenceException(
            "Committed history log contains an invalid record header at " + offset,
            invalidHeader);
      }
      if (length > remaining) {
        truncateTail(offset);
        size = offset;
        break;
      }
      if (expectedLength != 0 && length != expectedLength) {
        throw new ArchivePersistenceException(
            "Commit record length changes inside one archive generation");
      }
      HistoryCommitMarker marker;
      try {
        marker = codec.decode(read(offset, length));
        validateNext(previous, marker);
      } catch (IllegalArgumentException invalidRecord) {
        throw new ArchivePersistenceException(
            "Committed history log contains an invalid record at " + offset, invalidRecord);
      }
      if (count == 0) {
        firstEpoch = marker.getMeta().getEpoch();
        expectedLength = length;
      }
      previous = marker;
      count++;
      startupScannedRecords++;
      offset += length;
    }
    recordLength = expectedLength;
    recordCount = count;
    head = previous;
  }

  private void truncateTail(long offset) throws IOException {
    channel.truncate(offset);
    channel.force(true);
  }

  private HistoryCommitMarker readOrdinal(long ordinal) throws IOException {
    if (ordinal < 0 || ordinal >= recordCount) {
      throw new IllegalArgumentException("Commit ordinal is outside the committed prefix");
    }
    return codec.decode(read(ordinal * (long) recordLength, recordLength));
  }

  private byte[] read(long offset, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    channel.position(offset);
    readFully(channel, buffer);
    return buffer.array();
  }

  private static void validateNext(HistoryCommitMarker previous,
      HistoryCommitMarker current) {
    if (previous == null) {
      if (current.getPreviousEpoch() >= current.getMeta().getEpoch()) {
        throw new IllegalArgumentException("Invalid base commit marker previous epoch");
      }
      return;
    }
    if (current.getMeta().getEpoch() != previous.getMeta().getEpoch() + 1
        || current.getPreviousEpoch() != previous.getMeta().getEpoch()
        || current.getMeta().getBlockNumber() != previous.getMeta().getBlockNumber() + 1
        || !java.util.Arrays.equals(current.getMeta().getParentHash(),
        previous.getMeta().getBlockHash())) {
      throw new IllegalArgumentException("Non-contiguous history commit marker");
    }
  }

  private static void writeFully(FileChannel target, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      target.write(buffer);
    }
  }

  private static void readFully(FileChannel source, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (source.read(buffer) < 0) {
        throw new IOException("Unexpected end of history commit log");
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    channel.close();
  }

  @FunctionalInterface
  interface DirectorySync {
    void sync(Path directory) throws IOException;
  }
}
