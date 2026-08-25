package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Atomic startup intent identifying one validated history truncation target. */
final class ArchiveTruncationIntent {

  private static final int MAGIC = 0x54415449; // TATI
  private static final short VERSION = 1;
  private static final String FILE_NAME = "truncation.intent";
  private static final String TEMP_FILE_NAME = "truncation.intent.tmp";
  private static final int HEADER_LENGTH = 36;
  private static final int MAX_LENGTH = 2 * 1024 * 1024;

  private final long firstEpoch;
  private final long recordCount;
  private final int recordLength;
  private final HistoryCommitMarker marker;
  private final byte[] encodedMarker;

  private ArchiveTruncationIntent(long firstEpoch, long recordCount, int recordLength,
      HistoryCommitMarker marker, byte[] encodedMarker) {
    this.firstEpoch = firstEpoch;
    this.recordCount = recordCount;
    this.recordLength = recordLength;
    this.marker = marker;
    this.encodedMarker = Arrays.copyOf(encodedMarker, encodedMarker.length);
  }

  static ArchiveTruncationIntent prepare(Path archiveDirectory, HistoryCommitStore commits,
      HistoryIndexStore index, HistorySegmentStore bodies, long targetEpoch,
      HistoryCommitMarkerCodec markerCodec) throws IOException {
    return prepare(archiveDirectory, commits, index, bodies, targetEpoch, markerCodec,
        temporary -> { });
  }

  static ArchiveTruncationIntent prepare(Path archiveDirectory, HistoryCommitStore commits,
      HistoryIndexStore index, HistorySegmentStore bodies, long targetEpoch,
      HistoryCommitMarkerCodec markerCodec, FaultHook faultHook) throws IOException {
    HistoryCommitMarker marker = commits.get(targetEpoch);
    if (marker == null) {
      throw new ArchivePersistenceException(
          "Archive truncation intent target is outside committed history");
    }
    HistoryIndexRecord indexRecord = index.read(marker.getIndexLocation());
    BlockReverseDiff body = bodies.read(marker.getHistoryLocation());
    if (!marker.getMeta().equals(indexRecord.getMeta())
        || !marker.getMeta().equals(body.getMeta())
        || !sameLocation(marker.getHistoryLocation(), indexRecord.getHistoryLocation())) {
      throw new ArchivePersistenceException("Archive truncation intent target is inconsistent");
    }
    long count = targetEpoch - commits.firstEpoch() + 1;
    byte[] markerBytes = markerCodec.encode(marker);
    ArchiveTruncationIntent intent = new ArchiveTruncationIntent(commits.firstEpoch(), count,
        commits.getRecordLength(), marker, markerBytes);
    intent.persist(archiveDirectory, faultHook);
    return intent;
  }

  static ArchiveTruncationIntent load(Path archiveDirectory,
      HistoryCommitMarkerCodec markerCodec) throws IOException {
    Path path = archiveDirectory.resolve(FILE_NAME);
    if (!Files.exists(path)) {
      return null;
    }
    try {
      return decode(Files.readAllBytes(path), markerCodec);
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive truncation intent is corrupt", invalid);
    }
  }

  void clear(Path archiveDirectory) throws IOException {
    Files.deleteIfExists(archiveDirectory.resolve(FILE_NAME));
    HistorySegmentStore.syncDirectory(archiveDirectory);
  }

  ArchiveHistoryScanAnchor persistCheckpoint(Path archiveDirectory,
      HistoryCommitMarkerCodec markerCodec) throws IOException {
    return ArchiveHistoryScanAnchor.persist(archiveDirectory, firstEpoch, recordCount,
        recordLength, marker, markerCodec);
  }

  long commitEndOffset() {
    return recordCount * (long) recordLength;
  }

  long markerOffset() {
    return (recordCount - 1) * (long) recordLength;
  }

  long getRecordCount() {
    return recordCount;
  }

  HistoryCommitMarker getMarker() {
    return marker;
  }

  byte[] getEncodedMarker() {
    return Arrays.copyOf(encodedMarker, encodedMarker.length);
  }

  private void persist(Path archiveDirectory, FaultHook faultHook) throws IOException {
    byte[] encoded = encode();
    Files.createDirectories(archiveDirectory);
    Path temporary = archiveDirectory.resolve(TEMP_FILE_NAME);
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      writeFully(channel, ByteBuffer.wrap(encoded));
      channel.force(true);
    }
    faultHook.afterTemporaryForce(temporary);
    try {
      Files.move(temporary, archiveDirectory.resolve(FILE_NAME),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Archive filesystem does not support atomic truncation intent", unsupported);
    }
    HistorySegmentStore.syncDirectory(archiveDirectory);
  }

  private byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      output.writeLong(firstEpoch);
      output.writeLong(recordCount);
      output.writeInt(recordLength);
      output.writeInt(encodedMarker.length);
      output.write(encodedMarker);
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = payload.length + Integer.BYTES;
      if (length > MAX_LENGTH) {
        throw new IllegalArgumentException("Archive truncation intent is too large");
      }
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected truncation intent encoding failure", impossible);
    }
  }

  private static ArchiveTruncationIntent decode(byte[] encoded,
      HistoryCommitMarkerCodec markerCodec) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES
        || encoded.length > MAX_LENGTH) {
      throw new IllegalArgumentException("Archive truncation intent length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Archive truncation intent checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported archive truncation intent header");
      }
      long firstEpoch = input.readLong();
      long recordCount = input.readLong();
      int recordLength = input.readInt();
      int markerLength = input.readInt();
      if (firstEpoch < 0 || recordCount <= 0 || recordLength <= 0
          || markerLength != recordLength || markerLength > input.available() - Integer.BYTES) {
        throw new IllegalArgumentException("Invalid archive truncation intent fields");
      }
      byte[] markerBytes = new byte[markerLength];
      input.readFully(markerBytes);
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Archive truncation intent payload mismatch");
      }
      HistoryCommitMarker marker = markerCodec.decode(markerBytes);
      if (firstEpoch + recordCount - 1 != marker.getMeta().getEpoch()) {
        throw new IllegalArgumentException("Archive truncation intent ordinal mismatch");
      }
      return new ArchiveTruncationIntent(firstEpoch, recordCount, recordLength, marker,
          markerBytes);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Archive truncation intent is truncated", invalid);
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private static boolean sameLocation(HistoryLocation expected, HistoryLocation actual) {
    return expected.getSegmentId() == actual.getSegmentId()
        && expected.getOffset() == actual.getOffset()
        && expected.getRecordLength() == actual.getRecordLength()
        && expected.getBodyChecksum() == actual.getBodyChecksum()
        && Arrays.equals(expected.getBodyDigest(), actual.getBodyDigest());
  }

  @FunctionalInterface
  interface FaultHook {
    void afterTemporaryForce(Path temporary) throws IOException;
  }
}
