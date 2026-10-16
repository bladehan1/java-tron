package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Exact 512-byte V5 segment header and predecessor-chain validation. */
final class StateArchiveSegmentHeaderV5 {

  private static final int DIGEST_OFFSET = 476;
  private static final int CRC_OFFSET = 508;
  private static final byte[] HEADER_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-SEGMENT-HEADER-V5\0");
  private static final byte[] FIRST_PREVIOUS_HEADER_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-FIRST-SEGMENT-HEADER-V5\0");
  private static final byte[] FIRST_PREVIOUS_FRAME_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-FIRST-SEGMENT-FRAME-V5\0");

  private final int laneId;
  private final long fileId;
  private final long firstBlockNumber;
  private final byte[] indexHeaderDigest;
  private final byte[] previousHeaderDigest;
  private final byte[] previousTerminalFrameDigest;
  private final byte[] startHistoryDigest;
  private final byte[] headerDigest;

  private StateArchiveSegmentHeaderV5(int laneId, long fileId, long firstBlockNumber,
      byte[] indexHeaderDigest, byte[] previousHeaderDigest,
      byte[] previousTerminalFrameDigest, byte[] startHistoryDigest, byte[] headerDigest) {
    StateArchiveGethFormatV5.requireLane(laneId);
    requireU32(fileId, "file ID");
    requireNonNegative(firstBlockNumber, "first block");
    this.laneId = laneId;
    this.fileId = fileId;
    this.firstBlockNumber = firstBlockNumber;
    this.indexHeaderDigest = requireHash(indexHeaderDigest, "index header digest");
    this.previousHeaderDigest = requireHash(previousHeaderDigest, "previous header digest");
    this.previousTerminalFrameDigest = requireHash(previousTerminalFrameDigest,
        "previous frame digest");
    this.startHistoryDigest = requireHash(startHistoryDigest, "start history digest");
    this.headerDigest = headerDigest == null ? null : requireHash(headerDigest, "header digest");
  }

  static StateArchiveSegmentHeaderV5 first(int laneId, long firstBlockNumber,
      byte[] indexHeaderDigest, byte[] startHistoryDigest) {
    return new StateArchiveSegmentHeaderV5(laneId, 0, firstBlockNumber,
        indexHeaderDigest, firstPreviousHeaderDigest(laneId, firstBlockNumber),
        firstPreviousFrameDigest(laneId, firstBlockNumber), startHistoryDigest, null);
  }

  static StateArchiveSegmentHeaderV5 next(StateArchiveSegmentHeaderV5 previous,
      byte[] previousTerminalFrameDigest, long firstBlockNumber, byte[] startHistoryDigest) {
    if (previous.fileId == 0xffffffffL) {
      throw new IllegalArgumentException("State Archive V5 segment file ID overflow");
    }
    return new StateArchiveSegmentHeaderV5(previous.laneId, previous.fileId + 1,
        firstBlockNumber, previous.indexHeaderDigest, previous.digest(),
        previousTerminalFrameDigest, startHistoryDigest, null);
  }

  byte[] encode() {
    ByteBuffer bytes = ByteBuffer.allocate(StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
    bytes.putInt(StateArchiveGethFormatV5.SEGMENT_FILE_MAGIC)
        .putShort(StateArchiveGethFormatV5.MAJOR_VERSION)
        .putShort(StateArchiveGethFormatV5.MINOR_VERSION)
        .putInt(StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH)
        .putShort((short) laneId).putShort((short) 0).putInt((int) fileId)
        .putLong(firstBlockNumber).putLong(StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES)
        .putLong(StateArchiveGethFormatV5.MAX_FRAME_BYTES)
        .put(StateArchiveGethFormatV5.formatDigest()).put(indexHeaderDigest)
        .put(previousHeaderDigest).put(previousTerminalFrameDigest)
        .put(startHistoryDigest).put(new byte[272]);
    if (bytes.position() != DIGEST_OFFSET) {
      throw new IllegalStateException("Invalid State Archive V5 segment header layout");
    }
    byte[] digest = StateArchiveFileFormatV3.sha256(
        HEADER_DOMAIN, Arrays.copyOf(bytes.array(), DIGEST_OFFSET));
    bytes.put(digest).putInt(crc32c(bytes.array(), 0, CRC_OFFSET));
    return bytes.array();
  }

  static StateArchiveSegmentHeaderV5 decode(byte[] encoded) {
    if (encoded == null || encoded.length != StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH) {
      throw invalid("segment header length");
    }
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    require(bytes.getInt() == StateArchiveGethFormatV5.SEGMENT_FILE_MAGIC, "header magic");
    require(bytes.getShort() == StateArchiveGethFormatV5.MAJOR_VERSION
        && bytes.getShort() == StateArchiveGethFormatV5.MINOR_VERSION, "header version");
    require(bytes.getInt() == StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH,
        "header size");
    int laneId = Short.toUnsignedInt(bytes.getShort());
    try {
      StateArchiveGethFormatV5.requireLane(laneId);
    } catch (IllegalArgumentException failure) {
      throw invalid("lane", failure);
    }
    require(bytes.getShort() == 0, "header flags");
    long fileId = Integer.toUnsignedLong(bytes.getInt());
    long firstBlockNumber = bytes.getLong();
    require(firstBlockNumber >= 0, "first block");
    require(bytes.getLong() == StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES
        && bytes.getLong() == StateArchiveGethFormatV5.MAX_FRAME_BYTES, "size policy");
    byte[] formatDigest = readHash(bytes);
    require(Arrays.equals(formatDigest, StateArchiveGethFormatV5.formatDigest()),
        "format digest");
    byte[] indexDigest = readHash(bytes);
    byte[] previousHeader = readHash(bytes);
    byte[] previousFrame = readHash(bytes);
    byte[] startHistory = readHash(bytes);
    byte[] reserved = new byte[272];
    bytes.get(reserved);
    for (byte value : reserved) {
      require(value == 0, "reserved bytes");
    }
    byte[] storedDigest = readHash(bytes);
    byte[] actualDigest = StateArchiveFileFormatV3.sha256(
        HEADER_DOMAIN, Arrays.copyOf(encoded, DIGEST_OFFSET));
    require(Arrays.equals(storedDigest, actualDigest), "header digest");
    require(bytes.getInt() == crc32c(encoded, 0, CRC_OFFSET), "header checksum");
    StateArchiveSegmentHeaderV5 decoded = new StateArchiveSegmentHeaderV5(laneId, fileId,
        firstBlockNumber, indexDigest, previousHeader, previousFrame, startHistory,
        actualDigest);
    if (fileId == 0) {
      require(Arrays.equals(previousHeader,
          firstPreviousHeaderDigest(laneId, firstBlockNumber)), "first header baseline");
      require(Arrays.equals(previousFrame,
          firstPreviousFrameDigest(laneId, firstBlockNumber)), "first frame baseline");
    }
    return decoded;
  }

  void requireSuccessorOf(StateArchiveSegmentHeaderV5 previous,
      byte[] terminalFrameDigest, long expectedFirstBlock, byte[] expectedStartHistory) {
    require(laneId == previous.laneId && fileId == previous.fileId + 1,
        "segment predecessor ID");
    require(firstBlockNumber == expectedFirstBlock, "segment first block");
    require(Arrays.equals(indexHeaderDigest, previous.indexHeaderDigest),
        "index header binding");
    require(Arrays.equals(previousHeaderDigest, previous.digest()),
        "previous header binding");
    require(Arrays.equals(previousTerminalFrameDigest, terminalFrameDigest),
        "previous frame binding");
    require(Arrays.equals(startHistoryDigest, expectedStartHistory),
        "start history binding");
  }

  byte[] digest() {
    return headerDigest == null
        ? Arrays.copyOfRange(encode(), DIGEST_OFFSET, DIGEST_OFFSET + 32)
        : Arrays.copyOf(headerDigest, headerDigest.length);
  }

  int getLaneId() { return laneId; }
  long getFileId() { return fileId; }
  long getFirstBlockNumber() { return firstBlockNumber; }
  byte[] getStartHistoryDigest() {
    return Arrays.copyOf(startHistoryDigest, startHistoryDigest.length);
  }
  byte[] getIndexHeaderDigest() {
    return Arrays.copyOf(indexHeaderDigest, indexHeaderDigest.length);
  }

  private static byte[] firstPreviousHeaderDigest(int laneId, long firstBlockNumber) {
    return baseline(FIRST_PREVIOUS_HEADER_DOMAIN, laneId, firstBlockNumber);
  }

  private static byte[] firstPreviousFrameDigest(int laneId, long firstBlockNumber) {
    return baseline(FIRST_PREVIOUS_FRAME_DOMAIN, laneId, firstBlockNumber);
  }

  private static byte[] baseline(byte[] domain, int laneId, long firstBlockNumber) {
    return StateArchiveFileFormatV3.sha256(domain, StateArchiveGethFormatV5.formatDigest(),
        ByteBuffer.allocate(Short.BYTES + Long.BYTES).putShort((short) laneId)
            .putLong(firstBlockNumber).array());
  }

  private static byte[] readHash(ByteBuffer bytes) {
    byte[] value = new byte[32];
    bytes.get(value);
    return value;
  }

  private static byte[] requireHash(byte[] value, String name) {
    if (value == null || value.length != 32) {
      throw new IllegalArgumentException("State Archive V5 " + name + " length mismatch");
    }
    return Arrays.copyOf(value, value.length);
  }

  private static void requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException("State Archive V5 " + name + " is negative");
    }
  }

  private static void requireU32(long value, String name) {
    if (value < 0 || value > 0xffffffffL) {
      throw new IllegalArgumentException("State Archive V5 " + name + " exceeds u32");
    }
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static void require(boolean condition, String name) {
    if (!condition) {
      throw invalid(name);
    }
  }

  private static IllegalArgumentException invalid(String name) {
    return new IllegalArgumentException("State Archive V5 invalid " + name);
  }

  private static IllegalArgumentException invalid(String name, Exception cause) {
    return new IllegalArgumentException("State Archive V5 invalid " + name, cause);
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }
}
