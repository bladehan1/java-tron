package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Exact 672-byte publication-ready tail for one Common-bound V5 archive prefix. */
final class StateArchiveTailV5 {

  private static final byte[] KEY = {0x7f, 0x53, 0x41, 0x05};
  static final int LANE_ACTIVE = 1;
  private static final int HEADER_CRC_OFFSET = StateArchiveGethFormatV5.TAIL_HEADER_LENGTH
      - Integer.BYTES;
  private static final int LANE_CRC_OFFSET = StateArchiveGethFormatV5.TAIL_LANE_LENGTH
      - Integer.BYTES;

  private final long firstBlockNumber;
  private final long commonBlockNumber;
  private final byte[] commonBlockHash;
  private final byte[] commonTargetDigest;
  private final byte[] resultHistoryDigest;
  private final List<LaneTerminal> lanes;

  StateArchiveTailV5(long firstBlockNumber, long commonBlockNumber,
      byte[] commonBlockHash, byte[] commonTargetDigest, byte[] resultHistoryDigest,
      List<LaneTerminal> lanes) {
    this.firstBlockNumber = firstBlockNumber;
    this.commonBlockNumber = commonBlockNumber;
    this.commonBlockHash = requireHash(commonBlockHash, "Common block hash");
    this.commonTargetDigest = requireHash(commonTargetDigest, "Common target digest");
    this.resultHistoryDigest = requireHash(resultHistoryDigest, "result history digest");
    this.lanes = immutableLanes(lanes);
    validate();
  }

  static StateArchiveTailV5 forTarget(long firstBlockNumber, CommonCheckpointTarget target,
      byte[] resultHistoryDigest, List<LaneTerminal> lanes) {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    return new StateArchiveTailV5(firstBlockNumber,
        admitted.getLastBlock().getBlockNumber(), admitted.getLastBlock().getBlockHash(),
        admitted.getPayloadDigest(), resultHistoryDigest, lanes);
  }

  static byte[] key() {
    return Arrays.copyOf(KEY, KEY.length);
  }

  byte[] encode() {
    ByteBuffer bytes = ByteBuffer.allocate(StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH);
    bytes.putInt(StateArchiveGethFormatV5.ARCHIVE_TAIL_MAGIC)
        .putShort(StateArchiveGethFormatV5.MAJOR_VERSION)
        .putShort(StateArchiveGethFormatV5.MINOR_VERSION)
        .putInt(StateArchiveGethFormatV5.TAIL_HEADER_LENGTH)
        .putShort((short) StateArchiveGethFormatV5.TAIL_LANE_LENGTH)
        .putShort((short) StateArchiveGethFormatV5.laneIds().length)
        .putInt(StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH).putInt(0)
        .put(StateArchiveGethFormatV5.formatDigest()).putLong(firstBlockNumber)
        .putLong(commonBlockNumber).put(commonBlockHash).put(commonTargetDigest)
        .put(resultHistoryDigest).put(new byte[20]);
    bytes.putInt(crc32c(bytes.array(), 0, HEADER_CRC_OFFSET));
    for (LaneTerminal lane : lanes) {
      int start = bytes.position();
      bytes.putShort((short) lane.laneId).putShort((short) lane.flags)
          .putInt((int) lane.terminalFileId).putLong(lane.committedIndexBytes)
          .putLong(lane.terminalDataEndOffset).put(lane.terminalFrameDigest)
          .put(lane.terminalSegmentHeaderDigest).putInt(0);
      bytes.putInt(crc32c(bytes.array(), start, LANE_CRC_OFFSET));
    }
    return bytes.array();
  }

  static StateArchiveTailV5 decode(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    require(encoded.length == StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH,
        "tail length");
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    require(bytes.getInt() == StateArchiveGethFormatV5.ARCHIVE_TAIL_MAGIC, "tail magic");
    require(bytes.getShort() == StateArchiveGethFormatV5.MAJOR_VERSION
        && bytes.getShort() == StateArchiveGethFormatV5.MINOR_VERSION, "tail version");
    require(bytes.getInt() == StateArchiveGethFormatV5.TAIL_HEADER_LENGTH,
        "tail header length");
    require(Short.toUnsignedInt(bytes.getShort()) == StateArchiveGethFormatV5.TAIL_LANE_LENGTH,
        "tail lane length");
    int[] laneIds = StateArchiveGethFormatV5.laneIds();
    require(Short.toUnsignedInt(bytes.getShort()) == laneIds.length, "tail lane count");
    require(bytes.getInt() == StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH,
        "tail total length");
    require(bytes.getInt() == 0, "tail flags");
    require(Arrays.equals(read(bytes, 32), StateArchiveGethFormatV5.formatDigest()),
        "tail format digest");
    long firstBlockNumber = bytes.getLong();
    long commonBlockNumber = bytes.getLong();
    byte[] commonBlockHash = read(bytes, 32);
    byte[] commonTargetDigest = read(bytes, 32);
    byte[] resultHistoryDigest = read(bytes, 32);
    requireZero(read(bytes, 20), "tail reserved bytes");
    require(bytes.getInt() == crc32c(encoded, 0, HEADER_CRC_OFFSET),
        "tail header checksum");
    List<LaneTerminal> lanes = new ArrayList<>(laneIds.length);
    for (int expectedLane : laneIds) {
      int start = bytes.position();
      int laneId = Short.toUnsignedInt(bytes.getShort());
      int flags = Short.toUnsignedInt(bytes.getShort());
      long fileId = Integer.toUnsignedLong(bytes.getInt());
      long indexBytes = bytes.getLong();
      long dataEnd = bytes.getLong();
      byte[] frameDigest = read(bytes, 32);
      byte[] segmentDigest = read(bytes, 32);
      require(bytes.getInt() == 0, "tail lane reserved bytes");
      require(bytes.getInt() == crc32c(encoded, start, LANE_CRC_OFFSET),
          "tail lane checksum");
      require(laneId == expectedLane, "tail lane order");
      lanes.add(new LaneTerminal(laneId, flags, fileId, indexBytes, dataEnd,
          frameDigest, segmentDigest));
    }
    return new StateArchiveTailV5(firstBlockNumber, commonBlockNumber, commonBlockHash,
        commonTargetDigest, resultHistoryDigest, lanes);
  }

  void requireTarget(CommonCheckpointTarget target) {
    CommonCheckpointTarget expected = Objects.requireNonNull(target, "target");
    require(commonBlockNumber == expected.getLastBlock().getBlockNumber()
        && Arrays.equals(commonBlockHash, expected.getLastBlock().getBlockHash())
        && Arrays.equals(commonTargetDigest, expected.getPayloadDigest()),
        "tail differs from Common target");
  }

  long getFirstBlockNumber() {
    return firstBlockNumber;
  }

  long getCommonBlockNumber() {
    return commonBlockNumber;
  }

  byte[] getResultHistoryDigest() {
    return Arrays.copyOf(resultHistoryDigest, resultHistoryDigest.length);
  }

  List<LaneTerminal> getLanes() {
    return lanes;
  }

  private void validate() {
    require(firstBlockNumber >= 0 && commonBlockNumber >= firstBlockNumber,
        "tail block range");
    int[] laneIds = StateArchiveGethFormatV5.laneIds();
    require(lanes.size() == laneIds.length, "tail lane count");
    long frameCount;
    try {
      frameCount = StrictMathWrapper.addExact(StrictMathWrapper.subtractExact(commonBlockNumber, firstBlockNumber), 1);
    } catch (ArithmeticException failure) {
      throw invalid("tail block range", failure);
    }
    long expectedIndexBytes = StateArchiveLaneIndexV5.expectedLength(frameCount);
    for (int index = 0; index < laneIds.length; index++) {
      LaneTerminal lane = lanes.get(index);
      require(lane.laneId == laneIds[index], "tail lane order");
      lane.validate(expectedIndexBytes);
    }
  }

  private static List<LaneTerminal> immutableLanes(List<LaneTerminal> supplied) {
    List<LaneTerminal> admitted = new ArrayList<>(Objects.requireNonNull(supplied, "lanes"));
    require(!admitted.contains(null), "tail lane is missing");
    return Collections.unmodifiableList(admitted);
  }

  private static byte[] requireHash(byte[] value, String name) {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    require(admitted.length == 32, name + " length");
    return admitted;
  }

  private static byte[] read(ByteBuffer source, int length) {
    byte[] value = new byte[length];
    source.get(value);
    return value;
  }

  private static void requireZero(byte[] value, String name) {
    for (byte item : value) {
      require(item == 0, name);
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

  static final class LaneTerminal {
    private final int laneId;
    private final int flags;
    private final long terminalFileId;
    private final long committedIndexBytes;
    private final long terminalDataEndOffset;
    private final byte[] terminalFrameDigest;
    private final byte[] terminalSegmentHeaderDigest;

    LaneTerminal(int laneId, int flags, long terminalFileId, long committedIndexBytes,
        long terminalDataEndOffset, byte[] terminalFrameDigest,
        byte[] terminalSegmentHeaderDigest) {
      this.laneId = laneId;
      this.flags = flags;
      this.terminalFileId = terminalFileId;
      this.committedIndexBytes = committedIndexBytes;
      this.terminalDataEndOffset = terminalDataEndOffset;
      this.terminalFrameDigest = requireHash(terminalFrameDigest, "terminal frame digest");
      this.terminalSegmentHeaderDigest = requireHash(terminalSegmentHeaderDigest,
          "terminal segment header digest");
    }

    private void validate(long expectedIndexBytes) {
      require(flags == LANE_ACTIVE, "tail lane flags");
      require(terminalFileId >= 0 && terminalFileId <= 0xffffffffL,
          "tail terminal file ID");
      require(committedIndexBytes == expectedIndexBytes, "tail committed index bytes");
      require(terminalDataEndOffset > StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
          && terminalDataEndOffset <= 0xffffffffL, "tail terminal data offset");
    }

    int getLaneId() {
      return laneId;
    }

    int getFlags() {
      return flags;
    }

    long getTerminalFileId() {
      return terminalFileId;
    }

    long getCommittedIndexBytes() {
      return committedIndexBytes;
    }

    long getTerminalDataEndOffset() {
      return terminalDataEndOffset;
    }

    byte[] getTerminalFrameDigest() {
      return Arrays.copyOf(terminalFrameDigest, terminalFrameDigest.length);
    }

    byte[] getTerminalSegmentHeaderDigest() {
      return Arrays.copyOf(terminalSegmentHeaderDigest,
          terminalSegmentHeaderDigest.length);
    }
  }
}
