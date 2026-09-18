package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Single-slot recovery authority for one Common-committed catalogless Archive tail. */
final class StateArchiveTailV4 {

  static final int MAGIC = 0x53415434; // SAT4
  static final int HEADER_LENGTH = 160;
  static final int LANE_LENGTH = 56;
  static final int TOTAL_LENGTH = HEADER_LENGTH + 5 * LANE_LENGTH;
  static final int TERMINAL_OPEN = 1;
  static final int TERMINAL_SEALED = 2;
  private static final int HEADER_CRC_OFFSET = HEADER_LENGTH - Integer.BYTES;
  private static final int LANE_CRC_OFFSET = LANE_LENGTH - Integer.BYTES;
  private static final int[] LANE_IDS = {0, 4, 5, 13, 22};

  private final long checkpointSequence;
  private final long commonBlockNumber;
  private final byte[] commonBlockHash;
  private final byte[] commonTargetDigest;
  private final List<LaneTerminal> lanes;

  StateArchiveTailV4(long checkpointSequence, long commonBlockNumber, byte[] commonBlockHash,
      byte[] commonTargetDigest, List<LaneTerminal> lanes) {
    this.checkpointSequence = checkpointSequence;
    this.commonBlockNumber = commonBlockNumber;
    this.commonBlockHash = copyDigest(commonBlockHash, "common block hash");
    this.commonTargetDigest = copyDigest(commonTargetDigest, "Common target digest");
    this.lanes = immutableLanes(lanes);
    validate();
  }

  static StateArchiveTailV4 forTarget(long checkpointSequence, CommonCheckpointTarget target,
      List<LaneTerminal> lanes) {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    return new StateArchiveTailV4(checkpointSequence,
        admitted.getLastBlock().getBlockNumber(), admitted.getLastBlock().getBlockHash(),
        admitted.getPayloadDigest(), lanes);
  }

  byte[] encode() {
    ByteBuffer bytes = ByteBuffer.allocate(TOTAL_LENGTH);
    bytes.putInt(MAGIC).putShort(StateArchiveCataloglessFormatV4.MAJOR_VERSION)
        .putShort(StateArchiveCataloglessFormatV4.MINOR_VERSION).putInt(HEADER_LENGTH)
        .putShort((short) LANE_LENGTH).putShort((short) LANE_IDS.length)
        .putInt(TOTAL_LENGTH).putInt(0).put(StateArchiveCataloglessFormatV4.formatDigest())
        .putLong(checkpointSequence).putLong(commonBlockNumber).put(commonBlockHash)
        .put(commonTargetDigest).put(new byte[20]);
    bytes.putInt(crc32c(bytes.array(), 0, HEADER_CRC_OFFSET));
    for (LaneTerminal lane : lanes) {
      int start = bytes.position();
      bytes.putShort((short) lane.laneId).putShort((short) lane.flags)
          .putInt((int) lane.fileId).putLong(lane.firstRecordBlockNumber)
          .putLong(lane.endBlockNumber).putLong(lane.dataEndOffset)
          .putLong(lane.filesMetaCommittedBytes).put(new byte[12]);
      bytes.putInt(crc32c(bytes.array(), start, LANE_CRC_OFFSET));
    }
    return bytes.array();
  }

  static StateArchiveTailV4 decode(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    require(encoded.length == TOTAL_LENGTH, "Archive tail length mismatch");
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    require(bytes.getInt() == MAGIC, "Archive tail magic mismatch");
    require(bytes.getShort() == StateArchiveCataloglessFormatV4.MAJOR_VERSION,
        "Archive tail major version mismatch");
    require(bytes.getShort() == StateArchiveCataloglessFormatV4.MINOR_VERSION,
        "Archive tail minor version mismatch");
    require(bytes.getInt() == HEADER_LENGTH, "Archive tail header length mismatch");
    require(Short.toUnsignedInt(bytes.getShort()) == LANE_LENGTH,
        "Archive tail lane length mismatch");
    require(Short.toUnsignedInt(bytes.getShort()) == LANE_IDS.length,
        "Archive tail lane count mismatch");
    require(bytes.getInt() == TOTAL_LENGTH, "Archive tail total length mismatch");
    require(bytes.getInt() == 0, "Archive tail flags mismatch");
    byte[] formatDigest = new byte[32];
    bytes.get(formatDigest);
    require(Arrays.equals(formatDigest, StateArchiveCataloglessFormatV4.formatDigest()),
        "Archive tail format identity mismatch");
    long sequence = bytes.getLong();
    long blockNumber = bytes.getLong();
    byte[] blockHash = read(bytes, 32);
    byte[] targetDigest = read(bytes, 32);
    requireZero(read(bytes, 20), "Archive tail reserved bytes mismatch");
    require(bytes.getInt() == crc32c(encoded, 0, HEADER_CRC_OFFSET),
        "Archive tail header checksum mismatch");
    List<LaneTerminal> lanes = new ArrayList<>();
    for (int expectedLane : LANE_IDS) {
      int start = bytes.position();
      int laneId = Short.toUnsignedInt(bytes.getShort());
      int flags = Short.toUnsignedInt(bytes.getShort());
      long fileId = Integer.toUnsignedLong(bytes.getInt());
      long first = bytes.getLong();
      long end = bytes.getLong();
      long offset = bytes.getLong();
      long filesMeta = bytes.getLong();
      requireZero(read(bytes, 12), "Archive tail lane reserved bytes mismatch");
      require(bytes.getInt() == crc32c(encoded, start, LANE_CRC_OFFSET),
          "Archive tail lane checksum mismatch");
      require(laneId == expectedLane, "Archive tail lane order mismatch");
      lanes.add(new LaneTerminal(laneId, flags, fileId, first, end, offset, filesMeta));
    }
    return new StateArchiveTailV4(sequence, blockNumber, blockHash, targetDigest, lanes);
  }

  void requireTarget(CommonCheckpointTarget target) {
    CommonCheckpointTarget expected = Objects.requireNonNull(target, "target");
    require(commonBlockNumber == expected.getLastBlock().getBlockNumber()
            && Arrays.equals(commonBlockHash, expected.getLastBlock().getBlockHash())
            && Arrays.equals(commonTargetDigest, expected.getPayloadDigest()),
        "Archive tail differs from Common target");
  }

  long getCheckpointSequence() {
    return checkpointSequence;
  }

  long getCommonBlockNumber() {
    return commonBlockNumber;
  }

  List<LaneTerminal> getLanes() {
    return lanes;
  }

  private void validate() {
    require(checkpointSequence >= 0, "Archive tail checkpoint sequence mismatch");
    require(commonBlockNumber >= 0, "Archive tail Common block mismatch");
    require(lanes.size() == LANE_IDS.length, "Archive tail lane count mismatch");
    for (int index = 0; index < LANE_IDS.length; index++) {
      LaneTerminal lane = lanes.get(index);
      require(lane.laneId == LANE_IDS[index], "Archive tail lane order mismatch");
      lane.validate(commonBlockNumber);
    }
  }

  private static List<LaneTerminal> immutableLanes(List<LaneTerminal> supplied) {
    List<LaneTerminal> admitted = new ArrayList<>(Objects.requireNonNull(supplied, "lanes"));
    require(!admitted.contains(null), "Archive tail lane is missing");
    return Collections.unmodifiableList(admitted);
  }

  private static byte[] copyDigest(byte[] value, String name) {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    require(admitted.length == 32, name + " length mismatch");
    return admitted;
  }

  private static byte[] read(ByteBuffer source, int length) {
    byte[] result = new byte[length];
    source.get(result);
    return result;
  }

  private static void requireZero(byte[] value, String message) {
    for (byte item : value) {
      require(item == 0, message);
    }
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  static final class LaneTerminal {
    private final int laneId;
    private final int flags;
    private final long fileId;
    private final long firstRecordBlockNumber;
    private final long endBlockNumber;
    private final long dataEndOffset;
    private final long filesMetaCommittedBytes;

    LaneTerminal(int laneId, int flags, long fileId, long firstRecordBlockNumber,
        long endBlockNumber, long dataEndOffset, long filesMetaCommittedBytes) {
      this.laneId = laneId;
      this.flags = flags;
      this.fileId = fileId;
      this.firstRecordBlockNumber = firstRecordBlockNumber;
      this.endBlockNumber = endBlockNumber;
      this.dataEndOffset = dataEndOffset;
      this.filesMetaCommittedBytes = filesMetaCommittedBytes;
    }

    private void validate(long commonBlockNumber) {
      require(flags == TERMINAL_OPEN || flags == TERMINAL_SEALED,
          "Archive tail lane flags mismatch");
      require(fileId >= 0 && fileId <= 0xffffffffL,
          "Archive tail lane file id mismatch");
      require(firstRecordBlockNumber >= 0 && firstRecordBlockNumber <= endBlockNumber,
          "Archive tail lane range mismatch");
      require(endBlockNumber == commonBlockNumber,
          "Archive tail lane does not cover Common block");
      require(dataEndOffset >= StateArchiveFileFormatV3.PART_HEADER_LENGTH,
          "Archive tail lane data offset mismatch");
      long sealedCount = flags == TERMINAL_SEALED ? fileId + 1 : fileId;
      long expectedMeta = StateArchiveFilesMetaV4.HEADER_LENGTH
          + sealedCount * StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH;
      require(filesMetaCommittedBytes == expectedMeta,
          "Archive tail files.meta boundary mismatch");
    }

    int getLaneId() {
      return laneId;
    }

    int getFlags() {
      return flags;
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

    long getDataEndOffset() {
      return dataEndOffset;
    }

    long getFilesMetaCommittedBytes() {
      return filesMetaCommittedBytes;
    }
  }
}
