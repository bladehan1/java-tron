package org.tron.core.db2.archive;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Byte constants and identity for the Geth-style State Archive V5 prototype. */
final class StateArchiveGethFormatV5 {

  static final String FORMAT_ID = "archive-state/geth-lane-index/v5";
  static final int FORMAT_DESCRIPTOR_MAGIC = 0x464d5435; // FMT5
  static final int LANE_INDEX_MAGIC = 0x53414935; // SAI5
  static final int SEGMENT_FILE_MAGIC = 0x53414435; // SAD5
  static final int BLOCK_FRAME_MAGIC = 0x53414635; // SAF5
  static final int ARCHIVE_TAIL_MAGIC = 0x53415435; // SAT5
  static final short MAJOR_VERSION = 5;
  static final short MINOR_VERSION = 0;

  static final int FORMAT_DESCRIPTOR_LENGTH = 128;
  static final int LANE_INDEX_HEADER_LENGTH = 128;
  static final int LANE_INDEX_ENTRY_LENGTH = 8;
  static final int SEGMENT_HEADER_LENGTH = 512;
  static final int FRAME_HEADER_LENGTH = 60;
  static final int FRAME_TRAILER_LENGTH = 36;
  static final int SECTION_HEADER_LENGTH = 80;
  static final int TAIL_HEADER_LENGTH = 192;
  static final int TAIL_LANE_LENGTH = 96;
  static final int TAIL_TOTAL_LENGTH = 672;
  static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
  static final long SEGMENT_TARGET_BYTES = 2L * 1024 * 1024 * 1024;

  static final short COMPRESSION_NONE = 0;
  static final short KEY_ORDER_UNSIGNED = 1;
  static final short DIGEST_SHA256 = 1;
  static final short CHECKSUM_CRC32C = 1;

  private static final int[] LANE_IDS = {0, 4, 5, 13, 22};
  private static final byte[] STORE_MAPPING_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-STORE-MAPPING-V5\0");
  private static final byte[] FORMAT_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-GETH-LANE-INDEX-V5\0");
  private static final byte[] BLOCK_HISTORY_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-BLOCK-HISTORY-V5\0");
  private static final byte[] BASELINE_HISTORY_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-BASELINE-HISTORY-V5\0");
  private static final byte[] ROLLING_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-ROLLING-V5\0");
  private static final byte[] STORE_MAPPING_DIGEST = buildStoreMappingDigest();
  private static final byte[] FORMAT_DESCRIPTOR = buildFormatDescriptor();
  private static final byte[] FORMAT_DIGEST = StateArchiveFileFormatV3.sha256(
      FORMAT_DOMAIN, FORMAT_DESCRIPTOR);

  private StateArchiveGethFormatV5() {
  }

  static int[] laneIds() {
    return Arrays.copyOf(LANE_IDS, LANE_IDS.length);
  }

  static void requireLane(int laneId) {
    for (int candidate : LANE_IDS) {
      if (candidate == laneId) {
        return;
      }
    }
    throw new IllegalArgumentException("Unknown State Archive V5 lane ID: " + laneId);
  }

  static byte[] formatDescriptor() {
    return Arrays.copyOf(FORMAT_DESCRIPTOR, FORMAT_DESCRIPTOR.length);
  }

  static byte[] formatDigest() {
    return Arrays.copyOf(FORMAT_DIGEST, FORMAT_DIGEST.length);
  }

  static byte[] storeMappingDigest() {
    return Arrays.copyOf(STORE_MAPPING_DIGEST, STORE_MAPPING_DIGEST.length);
  }

  static byte[] blockHistoryDigest(BlockSnapshotMeta meta, List<byte[]> laneItemDigests) {
    List<byte[]> admitted = new ArrayList<>(laneItemDigests);
    if (admitted.size() != LANE_IDS.length || admitted.contains(null)) {
      throw new IllegalArgumentException("State Archive V5 lane item digest count differs");
    }
    ByteBuffer identity = ByteBuffer.allocate(Long.BYTES + 2 * 32 + Short.BYTES
        + admitted.size() * 32);
    identity.putLong(meta.getBlockNumber()).put(requireHash(meta.getBlockHash(), "block hash"))
        .put(FORMAT_DIGEST).putShort((short) admitted.size());
    for (byte[] digest : admitted) {
      identity.put(requireHash(digest, "lane item digest"));
    }
    return StateArchiveFileFormatV3.sha256(BLOCK_HISTORY_DOMAIN, identity.array());
  }

  static byte[] baselineHistoryDigest(long firstBlockNumber, byte[] parentHash) {
    if (firstBlockNumber < 0) {
      throw new IllegalArgumentException("State Archive V5 first block is negative");
    }
    return StateArchiveFileFormatV3.sha256(BASELINE_HISTORY_DOMAIN,
        FORMAT_DIGEST, ByteBuffer.allocate(Long.BYTES).putLong(firstBlockNumber).array(),
        requireHash(parentHash, "baseline parent hash"));
  }

  static byte[] nextHistoryDigest(byte[] previous, byte[] blockHistoryDigest) {
    return StateArchiveFileFormatV3.sha256(ROLLING_DOMAIN,
        requireHash(previous, "previous history digest"),
        requireHash(blockHistoryDigest, "block history digest"));
  }

  private static byte[] buildStoreMappingDigest() {
    int encodedLength = STORE_MAPPING_DOMAIN.length + Short.BYTES;
    for (int storeId = 1; storeId <= StateArchiveFileFormatV3.STORE_COUNT; storeId++) {
      encodedLength += 4 * Short.BYTES
          + StateArchiveFileFormatV3.dbName(storeId).getBytes(StandardCharsets.UTF_8).length;
    }
    ByteBuffer bytes = ByteBuffer.allocate(encodedLength);
    bytes.put(STORE_MAPPING_DOMAIN);
    bytes.putShort((short) StateArchiveFileFormatV3.STORE_COUNT);
    for (int storeId = 1; storeId <= StateArchiveFileFormatV3.STORE_COUNT; storeId++) {
      byte[] name = StateArchiveFileFormatV3.dbName(storeId)
          .getBytes(StandardCharsets.UTF_8);
      if (name.length > 0xffff) {
        throw new IllegalStateException("State Archive Store name is too long");
      }
      int laneId = StateArchiveFileFormatV3.laneId(storeId);
      int keyWidth = storeId == 4 ? 21 : storeId == 22 ? 32 : 0;
      bytes.putShort((short) storeId).putShort((short) laneId)
          .putShort((short) keyWidth).putShort((short) name.length).put(name);
    }
    return StateArchiveFileFormatV3.sha256(bytes.array());
  }

  private static byte[] buildFormatDescriptor() {
    ByteBuffer bytes = ByteBuffer.allocate(FORMAT_DESCRIPTOR_LENGTH);
    bytes.putInt(FORMAT_DESCRIPTOR_MAGIC).putShort(MAJOR_VERSION).putShort(MINOR_VERSION)
        .putInt(FORMAT_DESCRIPTOR_LENGTH)
        .putShort((short) LANE_INDEX_HEADER_LENGTH)
        .putShort((short) LANE_INDEX_ENTRY_LENGTH)
        .putShort((short) SEGMENT_HEADER_LENGTH)
        .putShort((short) FRAME_HEADER_LENGTH)
        .putShort((short) FRAME_TRAILER_LENGTH)
        .putShort((short) SECTION_HEADER_LENGTH)
        .putShort((short) TAIL_HEADER_LENGTH)
        .putShort((short) TAIL_LANE_LENGTH)
        .putShort((short) TAIL_TOTAL_LENGTH)
        .putShort((short) LANE_IDS.length)
        .putInt(MAX_FRAME_BYTES).putLong(SEGMENT_TARGET_BYTES)
        .putInt(LANE_INDEX_MAGIC).putInt(SEGMENT_FILE_MAGIC)
        .putInt(BLOCK_FRAME_MAGIC).putInt(ARCHIVE_TAIL_MAGIC)
        .putShort(COMPRESSION_NONE).putShort(KEY_ORDER_UNSIGNED)
        .putShort(DIGEST_SHA256).putShort(CHECKSUM_CRC32C)
        .put(STORE_MAPPING_DIGEST).put(new byte[28]);
    if (bytes.hasRemaining()) {
      throw new IllegalStateException("Invalid State Archive V5 format descriptor length");
    }
    return bytes.array();
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  private static byte[] requireHash(byte[] value, String name) {
    if (value == null || value.length != StateArchiveFileFormatV3.HASH_LENGTH) {
      throw new IllegalArgumentException("State Archive V5 " + name + " length mismatch");
    }
    return Arrays.copyOf(value, value.length);
  }
}
