package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Byte-exact primitives for the catalogless five-lane append-file v4 format. */
final class StateArchiveCataloglessFormatV4 {

  static final short MAJOR_VERSION = 4;
  static final short MINOR_VERSION = 0;
  static final int FILE_DESCRIPTOR_LENGTH = 64;
  static final long SEGMENT_TARGET_BYTES = 2L * 1024 * 1024 * 1024;
  private static final byte[] FORMAT_DIGEST = Hashing.sha256().hashString(
      "archive-state/catalogless-five-lane/v4;frame=v3-full;bidx=v3-32;"
          + "segment-header=v3;segment-link=descriptor-sha256;"
          + "names=store_first-end/v1;file-descriptor=64;"
          + "segment-target=2147483648;overshoot=one-frame",
      StandardCharsets.US_ASCII).asBytes();
  private static final int[] LANE_IDS = {0, 4, 5, 13, 22};

  private StateArchiveCataloglessFormatV4() {
  }

  static byte[] formatDigest() {
    return Arrays.copyOf(FORMAT_DIGEST, FORMAT_DIGEST.length);
  }

  static int[] laneIds() {
    return Arrays.copyOf(LANE_IDS, LANE_IDS.length);
  }

  static byte[] encodeFileDescriptor(FileDescriptor descriptor) {
    FileDescriptor admitted = Objects.requireNonNull(descriptor, "descriptor");
    admitted.validate();
    ByteBuffer bytes = ByteBuffer.allocate(FILE_DESCRIPTOR_LENGTH);
    bytes.putInt((int) admitted.fileId).putInt(admitted.flags)
        .putLong(admitted.firstRecordBlockNumber).putLong(admitted.endBlockNumber)
        .putLong(admitted.dataFileBytes).putLong(admitted.recordCount)
        .putLong(admitted.changedFrameCount).putLong(admitted.entryCount);
    bytes.putInt(crc32c(bytes.array(), 0, 56)).putInt(0);
    return bytes.array();
  }

  static FileDescriptor decodeFileDescriptor(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    require(encoded.length == FILE_DESCRIPTOR_LENGTH, "file descriptor length mismatch");
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    FileDescriptor descriptor = new FileDescriptor(Integer.toUnsignedLong(bytes.getInt()),
        bytes.getInt(), bytes.getLong(), bytes.getLong(), bytes.getLong(), bytes.getLong(),
        bytes.getLong(), bytes.getLong());
    require(bytes.getInt() == crc32c(encoded, 0, 56), "file descriptor checksum mismatch");
    require(bytes.getInt() == 0, "file descriptor reserved bytes mismatch");
    descriptor.validate();
    return descriptor;
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  static final class FileDescriptor {
    private final long fileId;
    private final int flags;
    private final long firstRecordBlockNumber;
    private final long endBlockNumber;
    private final long dataFileBytes;
    private final long recordCount;
    private final long changedFrameCount;
    private final long entryCount;

    FileDescriptor(long fileId, int flags, long firstRecordBlockNumber, long endBlockNumber,
        long dataFileBytes, long recordCount, long changedFrameCount, long entryCount) {
      this.fileId = fileId;
      this.flags = flags;
      this.firstRecordBlockNumber = firstRecordBlockNumber;
      this.endBlockNumber = endBlockNumber;
      this.dataFileBytes = dataFileBytes;
      this.recordCount = recordCount;
      this.changedFrameCount = changedFrameCount;
      this.entryCount = entryCount;
      validate();
    }

    private void validate() {
      require(fileId >= 0 && fileId <= 0xffffffffL, "file descriptor id mismatch");
      require(flags == 0, "file descriptor flags mismatch");
      require(firstRecordBlockNumber >= 0 && endBlockNumber >= firstRecordBlockNumber,
          "file descriptor block range mismatch");
      long expectedCount;
      try {
        expectedCount = Math.addExact(Math.subtractExact(endBlockNumber,
            firstRecordBlockNumber), 1L);
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException("file descriptor record count overflow", e);
      }
      require(recordCount == expectedCount, "file descriptor record count mismatch");
      require(dataFileBytes >= StateArchiveFileFormatV3.PART_HEADER_LENGTH,
          "file descriptor data length mismatch");
      require(changedFrameCount >= 0 && changedFrameCount <= recordCount,
          "file descriptor changed frame count mismatch");
      require(entryCount >= 0, "file descriptor entry count mismatch");
    }

    long getFileId() {
      return fileId;
    }

    long getFirstRecordBlockNumber() {
      return firstRecordBlockNumber;
    }

    long getEndBlockNumber() {
      return endBlockNumber;
    }

    long getDataFileBytes() {
      return dataFileBytes;
    }

    long getRecordCount() {
      return recordCount;
    }

    long getChangedFrameCount() {
      return changedFrameCount;
    }

    long getEntryCount() {
      return entryCount;
    }
  }
}
