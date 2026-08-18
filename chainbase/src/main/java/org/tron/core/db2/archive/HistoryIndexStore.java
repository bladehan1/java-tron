package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Append-only authoritative {@code state_history.idx}. */
public final class HistoryIndexStore implements Closeable {

  private final Path archiveDirectory;
  private final Path indexPath;
  private final HistoryIndexCodec codec;
  private final FileChannel channel;
  private ScanResult scanResult;

  public HistoryIndexStore(Path archiveDirectory, HistoryIndexCodec codec) throws IOException {
    this.archiveDirectory = archiveDirectory;
    this.indexPath = archiveDirectory.resolve("state_history.idx");
    this.codec = codec;
    Files.createDirectories(archiveDirectory);
    boolean created = !Files.exists(indexPath);
    channel = FileChannel.open(indexPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
    if (created) {
      HistorySegmentStore.syncDirectory(archiveDirectory);
    }
    scanResult = scan();
    channel.position(channel.size());
  }

  public synchronized HistoryIndexLocation append(HistoryIndexRecord record) throws IOException {
    if (scanResult.getInvalidTailOffset() != null) {
      throw new IllegalStateException("History index has an invalid tail");
    }
    if (scanResult.getHead() != null) {
      validateContinuity(scanResult.getHead().getRecord().getMeta(), record.getMeta());
    }
    byte[] encoded = codec.encode(record);
    long offset = channel.size();
    channel.position(offset);
    writeFully(channel, ByteBuffer.wrap(encoded));
    HistoryIndexLocation location = new HistoryIndexLocation(offset, encoded.length,
        sha256(encoded));
    scanResult = new ScanResult(scanResult.getRecordCount() + 1,
        new ScannedIndexRecord(record, location), null, null);
    return location;
  }

  public synchronized void sync() throws IOException {
    channel.force(true);
    HistorySegmentStore.syncDirectory(archiveDirectory);
  }

  public synchronized HistoryIndexRecord read(HistoryIndexLocation location) throws IOException {
    if (location.endOffset() > channel.size()) {
      throw new IllegalArgumentException("History index location is outside file bounds");
    }
    ByteBuffer buffer = ByteBuffer.allocate(location.getRecordLength());
    channel.position(location.getOffset());
    readFully(channel, buffer);
    byte[] encoded = buffer.array();
    if (!Arrays.equals(sha256(encoded), location.getDigest())) {
      throw new IllegalArgumentException("History index location digest mismatch");
    }
    return codec.decode(encoded);
  }

  public synchronized ScanResult getScanResult() {
    return scanResult;
  }

  public synchronized ScanResult rescan() throws IOException {
    scanResult = scan();
    return scanResult;
  }

  public synchronized void truncateInvalidTail() throws IOException {
    if (scanResult.getInvalidTailOffset() == null) {
      return;
    }
    channel.truncate(scanResult.getInvalidTailOffset());
    channel.force(true);
    scanResult = scan();
    channel.position(channel.size());
  }

  public synchronized void truncateAfter(HistoryIndexLocation last) throws IOException {
    long length = last == null ? 0 : last.endOffset();
    channel.truncate(length);
    channel.force(true);
    scanResult = scan();
    channel.position(channel.size());
  }

  private ScanResult scan() throws IOException {
    long recordCount = 0;
    ScannedIndexRecord head = null;
    Long invalidOffset = null;
    String invalidReason = null;
    long offset = 0;
    BlockSnapshotMeta previous = null;
    while (offset < channel.size()) {
      long remaining = channel.size() - offset;
      if (remaining < 12) {
        invalidOffset = offset;
        invalidReason = "truncated index header";
        break;
      }
      ByteBuffer prefix = ByteBuffer.allocate(12);
      channel.position(offset);
      readFully(channel, prefix);
      int recordLength;
      try {
        recordLength = codec.recordLength(prefix.array());
      } catch (IllegalArgumentException e) {
        invalidOffset = offset;
        invalidReason = e.getMessage();
        break;
      }
      if (recordLength > remaining) {
        invalidOffset = offset;
        invalidReason = "truncated index record";
        break;
      }
      ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
      channel.position(offset);
      readFully(channel, recordBuffer);
      byte[] encoded = recordBuffer.array();
      HistoryIndexRecord record;
      try {
        record = codec.decode(encoded);
        validateContinuity(previous, record.getMeta());
      } catch (IllegalArgumentException e) {
        invalidOffset = offset;
        invalidReason = e.getMessage();
        break;
      }
      HistoryIndexLocation location = new HistoryIndexLocation(offset, recordLength,
          sha256(encoded));
      head = new ScannedIndexRecord(record, location);
      recordCount++;
      previous = record.getMeta();
      offset += recordLength;
    }
    return new ScanResult(recordCount, head, invalidOffset, invalidReason);
  }

  private void validateContinuity(BlockSnapshotMeta previous, BlockSnapshotMeta current) {
    if (previous == null) {
      return;
    }
    if (current.getEpoch() != previous.getEpoch() + 1
        || current.getBlockNumber() != previous.getBlockNumber() + 1
        || !Arrays.equals(current.getParentHash(), previous.getBlockHash())) {
      throw new IllegalArgumentException("non-contiguous history index metadata");
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        throw new IOException("Unexpected end of history index");
      }
    }
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    channel.close();
  }

  public static final class ScannedIndexRecord {
    private final HistoryIndexRecord record;
    private final HistoryIndexLocation location;

    private ScannedIndexRecord(HistoryIndexRecord record, HistoryIndexLocation location) {
      this.record = record;
      this.location = location;
    }

    public HistoryIndexRecord getRecord() {
      return record;
    }

    public HistoryIndexLocation getLocation() {
      return location;
    }
  }

  public static final class ScanResult {
    private final long recordCount;
    private final ScannedIndexRecord head;
    private final Long invalidTailOffset;
    private final String invalidReason;

    private ScanResult(long recordCount, ScannedIndexRecord head, Long invalidTailOffset,
        String invalidReason) {
      this.recordCount = recordCount;
      this.head = head;
      this.invalidTailOffset = invalidTailOffset;
      this.invalidReason = invalidReason;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public ScannedIndexRecord getHead() {
      return head;
    }

    public Long getInvalidTailOffset() {
      return invalidTailOffset;
    }

    public String getInvalidReason() {
      return invalidReason;
    }
  }
}
