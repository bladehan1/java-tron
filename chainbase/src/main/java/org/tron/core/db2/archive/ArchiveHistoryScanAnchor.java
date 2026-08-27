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

/** Atomic scan anchor for a previously validated committed history prefix. */
final class ArchiveHistoryScanAnchor {

  private static final int MAGIC = 0x54415341; // TASA
  private static final short VERSION = 1;
  private static final int HEADER_LENGTH = 36;
  private static final int MAX_LENGTH = 2 * 1024 * 1024;
  private static final String FILE_NAME = "history.scan-anchor";
  private static final String TEMP_FILE_NAME = "history.scan-anchor.tmp";

  private final long firstEpoch;
  private final long recordCount;
  private final int commitRecordLength;
  private final HistoryCommitMarker marker;
  private final byte[] encodedMarker;

  private ArchiveHistoryScanAnchor(long firstEpoch, long recordCount, int commitRecordLength,
      HistoryCommitMarker marker, byte[] encodedMarker) {
    this.firstEpoch = firstEpoch;
    this.recordCount = recordCount;
    this.commitRecordLength = commitRecordLength;
    this.marker = marker;
    this.encodedMarker = Arrays.copyOf(encodedMarker, encodedMarker.length);
  }

  static ArchiveHistoryScanAnchor load(Path archiveDirectory,
      HistoryCommitMarkerCodec markerCodec) throws IOException {
    Path path = archiveDirectory.resolve(FILE_NAME);
    if (!Files.exists(path)) {
      return null;
    }
    long size = Files.size(path);
    if (size < HEADER_LENGTH + Integer.BYTES || size > MAX_LENGTH) {
      throw new ArchivePersistenceException("Archive history scan anchor length is invalid");
    }
    byte[] encoded = Files.readAllBytes(path);
    try {
      return decode(encoded, markerCodec);
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive history scan anchor is corrupt", invalid);
    }
  }

  static ArchiveHistoryScanAnchor persist(Path archiveDirectory, long firstEpoch,
      long recordCount, int commitRecordLength, HistoryCommitMarker marker,
      HistoryCommitMarkerCodec markerCodec) throws IOException {
    if (marker == null || recordCount <= 0 || commitRecordLength <= 0
        || firstEpoch + recordCount - 1 != marker.getMeta().getEpoch()) {
      throw new IllegalArgumentException("Invalid archive history scan anchor state");
    }
    byte[] markerBytes = markerCodec.encode(marker);
    if (markerBytes.length != commitRecordLength) {
      throw new IllegalArgumentException("Scan anchor commit record length mismatch");
    }
    byte[] encoded = encode(firstEpoch, recordCount, commitRecordLength, markerBytes);
    Files.createDirectories(archiveDirectory);
    Path temporary = archiveDirectory.resolve(TEMP_FILE_NAME);
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      writeFully(channel, ByteBuffer.wrap(encoded));
      channel.force(true);
    }
    Path target = archiveDirectory.resolve(FILE_NAME);
    try {
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Archive filesystem does not support atomic history scan anchor replacement",
          unsupported);
    }
    HistorySegmentStore.syncDirectory(archiveDirectory);
    return new ArchiveHistoryScanAnchor(firstEpoch, recordCount, commitRecordLength, marker,
        markerBytes);
  }

  long getFirstEpoch() {
    return firstEpoch;
  }

  long getRecordCount() {
    return recordCount;
  }

  int getCommitRecordLength() {
    return commitRecordLength;
  }

  HistoryCommitMarker getMarker() {
    return marker;
  }

  byte[] getEncodedMarker() {
    return Arrays.copyOf(encodedMarker, encodedMarker.length);
  }

  ArchiveHistoryScanAnchor forHistoryLocation(HistoryLocation historyLocation) {
    HistoryCommitMarker laneMarker = new HistoryCommitMarker(marker.getMeta(),
        marker.getPreviousEpoch(), historyLocation, marker.getIndexLocation(),
        marker.getBatchId(), marker.getDatabases());
    return new ArchiveHistoryScanAnchor(firstEpoch, recordCount, commitRecordLength,
        laneMarker, encodedMarker);
  }

  private static byte[] encode(long firstEpoch, long recordCount, int commitRecordLength,
      byte[] markerBytes) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      output.writeLong(firstEpoch);
      output.writeLong(recordCount);
      output.writeInt(commitRecordLength);
      output.writeInt(markerBytes.length);
      output.write(markerBytes);
      output.flush();
      byte[] withoutChecksum = bytes.toByteArray();
      int length = withoutChecksum.length + Integer.BYTES;
      if (length > MAX_LENGTH) {
        throw new IllegalArgumentException("Archive history scan anchor is too large");
      }
      ByteBuffer.wrap(withoutChecksum).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(withoutChecksum);
      output.writeInt(crc32c(withoutChecksum));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected scan anchor encoding failure", impossible);
    }
  }

  private static ArchiveHistoryScanAnchor decode(byte[] encoded,
      HistoryCommitMarkerCodec markerCodec) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES
        || encoded.length > MAX_LENGTH) {
      throw new IllegalArgumentException("History scan anchor length is invalid");
    }
    int checksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    byte[] withoutChecksum = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    if (checksum != crc32c(withoutChecksum)) {
      throw new IllegalArgumentException("History scan anchor checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported history scan anchor header");
      }
      long firstEpoch = input.readLong();
      long recordCount = input.readLong();
      int recordLength = input.readInt();
      int markerLength = input.readInt();
      if (firstEpoch < 0 || recordCount <= 0 || recordLength <= 0
          || markerLength != recordLength || markerLength > input.available() - Integer.BYTES) {
        throw new IllegalArgumentException("Invalid history scan anchor fields");
      }
      byte[] markerBytes = new byte[markerLength];
      input.readFully(markerBytes);
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("History scan anchor payload mismatch");
      }
      HistoryCommitMarker marker = markerCodec.decode(markerBytes);
      if (firstEpoch + recordCount - 1 != marker.getMeta().getEpoch()) {
        throw new IllegalArgumentException("History scan anchor ordinal mismatch");
      }
      return new ArchiveHistoryScanAnchor(firstEpoch, recordCount, recordLength, marker,
          markerBytes);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("History scan anchor is truncated", invalid);
    }
  }

  private static int crc32c(byte[] bytes) {
    return Hashing.crc32c().hashBytes(bytes).asInt();
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }
}
