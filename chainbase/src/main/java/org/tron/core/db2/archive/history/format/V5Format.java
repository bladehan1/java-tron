package org.tron.core.db2.archive.history.format;

import java.util.Collection;
import java.util.List;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Read-only V5 wire identity and sizing; it carries no archive write authority. */
public final class V5Format {

  private V5Format() {}

  public static final String FORMAT_ID = StateArchiveGethFormatV5.FORMAT_ID;
  public static final int FORMAT_DESCRIPTOR_MAGIC =
      StateArchiveGethFormatV5.FORMAT_DESCRIPTOR_MAGIC;
  public static final int LANE_INDEX_MAGIC = StateArchiveGethFormatV5.LANE_INDEX_MAGIC;
  public static final int SEGMENT_FILE_MAGIC = StateArchiveGethFormatV5.SEGMENT_FILE_MAGIC;
  public static final int BLOCK_FRAME_MAGIC = StateArchiveGethFormatV5.BLOCK_FRAME_MAGIC;
  public static final int ARCHIVE_TAIL_MAGIC = StateArchiveGethFormatV5.ARCHIVE_TAIL_MAGIC;
  public static final short MAJOR_VERSION = StateArchiveGethFormatV5.MAJOR_VERSION;
  public static final short MINOR_VERSION = StateArchiveGethFormatV5.MINOR_VERSION;
  public static final int FORMAT_DESCRIPTOR_LENGTH =
      StateArchiveGethFormatV5.FORMAT_DESCRIPTOR_LENGTH;
  public static final int LANE_INDEX_HEADER_LENGTH =
      StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH;
  public static final int LANE_INDEX_ENTRY_LENGTH =
      StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH;
  public static final int SEGMENT_HEADER_LENGTH = StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH;
  public static final int FRAME_HEADER_LENGTH = StateArchiveGethFormatV5.FRAME_HEADER_LENGTH;
  public static final int FRAME_TRAILER_LENGTH = StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH;
  public static final int SECTION_HEADER_LENGTH = StateArchiveGethFormatV5.SECTION_HEADER_LENGTH;
  public static final int TAIL_HEADER_LENGTH = StateArchiveGethFormatV5.TAIL_HEADER_LENGTH;
  public static final int TAIL_LANE_LENGTH = StateArchiveGethFormatV5.TAIL_LANE_LENGTH;
  public static final int TAIL_TOTAL_LENGTH = StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH;
  public static final int MAX_FRAME_BYTES = StateArchiveGethFormatV5.MAX_FRAME_BYTES;
  public static final long SEGMENT_TARGET_BYTES = StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES;
  public static final short COMPRESSION_NONE = StateArchiveGethFormatV5.COMPRESSION_NONE;
  public static final short KEY_ORDER_UNSIGNED = StateArchiveGethFormatV5.KEY_ORDER_UNSIGNED;
  public static final short DIGEST_SHA256 = StateArchiveGethFormatV5.DIGEST_SHA256;
  public static final short CHECKSUM_CRC32C = StateArchiveGethFormatV5.CHECKSUM_CRC32C;

  public static int[] laneIds() {
    return StateArchiveGethFormatV5.laneIds();
  }

  public static void requireLane(int laneId) {
    StateArchiveGethFormatV5.requireLane(laneId);
  }

  public static byte[] formatDescriptor() {
    return StateArchiveGethFormatV5.formatDescriptor();
  }

  public static byte[] formatDigest() {
    return StateArchiveGethFormatV5.formatDigest();
  }

  public static byte[] storeMappingDigest() {
    return StateArchiveGethFormatV5.storeMappingDigest();
  }

  public static byte[] blockHistoryDigest(BlockSnapshotMeta meta, List<byte[]> laneItemDigests) {
    return StateArchiveGethFormatV5.blockHistoryDigest(meta, laneItemDigests);
  }

  public static byte[] baselineHistoryDigest(long firstBlockNumber, byte[] parentHash) {
    return StateArchiveGethFormatV5.baselineHistoryDigest(firstBlockNumber, parentHash);
  }

  public static byte[] nextHistoryDigest(byte[] previous, byte[] blockHistoryDigest) {
    return StateArchiveGethFormatV5.nextHistoryDigest(previous, blockHistoryDigest);
  }

  public static int[] fiveLaneIds() {
    return StateArchiveFileFormatV3.fiveLaneIds();
  }

  public static int laneId(int storeId) {
    return StateArchiveFileFormatV3.laneId(storeId);
  }

  public static long laneCoverage(int laneId) {
    return StateArchiveFileFormatV3.laneCoverage(laneId);
  }

  public static short laneKind(int laneId) {
    return StateArchiveFileFormatV3.laneKind(laneId);
  }

  public static short laneBodyCodec(int laneId) {
    return StateArchiveFileFormatV3.laneBodyCodec(laneId);
  }

  public static int fixedKeyWidth(int laneId) {
    return StateArchiveFileFormatV3.fixedKeyWidth(laneId);
  }

  public static int storeId(String dbName) {
    return StateArchiveFileFormatV3.storeId(dbName);
  }

  public static String dbName(int storeId) {
    return StateArchiveFileFormatV3.dbName(storeId);
  }

  public static void requireExactCapture(Collection<String> dbNames) {
    StateArchiveFileFormatV3.requireExactCapture(dbNames);
  }

  public static byte[] sha256(byte[]... values) {
    return StateArchiveFileFormatV3.sha256(values);
  }

  public static byte[] ascii(String value) {
    return StateArchiveFileFormatV3.ascii(value);
  }

  public static long expectedIndexLength(long frameCount) {
    if (frameCount < 0) {
      throw new IllegalArgumentException("State Archive V5 frame count is negative");
    }
    try {
      return StrictMathWrapper.addExact(
          LANE_INDEX_HEADER_LENGTH,
          StrictMathWrapper.multiplyExact(
              StrictMathWrapper.addExact(frameCount, 1), LANE_INDEX_ENTRY_LENGTH));
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 index length overflow", failure);
    }
  }
}
