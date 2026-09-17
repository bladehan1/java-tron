package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.FrameRange;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveFiveLaneWriterV5Test {

  private static final String FIRST_HEADER_HASH =
      "42022a51466ad9659a06ace2590fe1a0d33cfde7520eae531791fdb217a187af";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void createsFiveLanesAppendsAndRotatesAfterOneFrameOvershoot() throws Exception {
    Path root = temporaryFolder.newFolder("history-v5").toPath();
    byte[] baseline = hash(99);
    byte[] afterTwo;
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, baseline, 650)) {
      writer.append(diff(100));
      writer.append(diff(101));
      afterTwo = writer.getResultHistoryDigest();

      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        Path first = StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 0);
        assertEquals(704, Files.size(first));
        assertFalse(Files.exists(StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 1)));
        FrameRange second = writer.locate(laneId, 101);
        assertRange(second, 0, 608, 704);
      }

      writer.append(diff(102));
      assertEquals(102, writer.getAppendHead().getBlockNumber());
      assertFalse(java.util.Arrays.equals(afterTwo, writer.getResultHistoryDigest()));
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        Path laneRoot = StateArchiveFiveLaneWriterV5.laneRoot(root, laneId);
        Path first = StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 0);
        Path second = StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 1);
        assertTrue(Files.exists(laneRoot.resolve("blocks.idx")));
        assertEquals(704, Files.size(first));
        assertEquals(608, Files.size(second));
        assertEquals(160, Files.size(laneRoot.resolve("blocks.idx")));
        assertRange(writer.locate(laneId, 102), 1, 512, 608);

        StateArchiveSegmentHeaderV5 firstHeader = readHeader(first);
        StateArchiveSegmentHeaderV5 secondHeader = readHeader(second);
        byte[] terminalFrame = slice(first, 608, 96);
        byte[] terminalDigest = new StateArchiveBlockFrameCodecV5()
            .decode(terminalFrame).getFrameDigest();
        secondHeader.requireSuccessorOf(firstHeader, terminalDigest, 102, afterTwo);
      }
    }
  }

  @Test
  public void freezesFirstHeaderAndRejectsCorruptionAndNonConsecutiveBlocks() throws Exception {
    Path root = temporaryFolder.newFolder("identity").toPath();
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 7, hash(6), 650)) {
      Path first = StateArchiveFiveLaneWriterV5.dataPath(root, 0, 0);
      byte[] header = slice(first, 0, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
      assertEquals(FIRST_HEADER_HASH, hex(StateArchiveFileFormatV3.sha256(header)));
      StateArchiveSegmentHeaderV5 decoded = StateArchiveSegmentHeaderV5.decode(header);
      assertEquals(0, decoded.getLaneId());
      assertEquals(0, decoded.getFileId());
      assertEquals(7, decoded.getFirstBlockNumber());
      assertArrayEquals(hash(6), decoded.getStartHistoryDigest());

      byte[] corrupted = java.util.Arrays.copyOf(header, header.length);
      corrupted[200] ^= 1;
      assertThrows(IllegalArgumentException.class,
          () -> StateArchiveSegmentHeaderV5.decode(corrupted));

      assertThrows(IllegalArgumentException.class, () -> writer.append(diff(8)));
      writer.append(diff(7));
      assertThrows(IllegalArgumentException.class, () -> writer.append(diff(9)));
      assertThrows(IllegalArgumentException.class,
          () -> writer.append(diff(8, hash(1))));
      writer.append(diff(8));
    }
  }

  @Test
  public void forcesAllDataThenAllIndexesBeforeReturningTailReady() throws Exception {
    Path root = temporaryFolder.newFolder("tail-ready").toPath();
    List<String> events = new ArrayList<>();
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      writer.append(diff(100));
      writer.append(diff(101));
      CommonCheckpointTarget target = target(101);

      StateArchiveTailV5 tail = writer.forceTailReady(target,
          (stage, laneId) -> events.add(stage + ":" + laneId));

      assertEquals(Arrays.asList("DATA:0", "DATA:4", "DATA:5", "DATA:13", "DATA:22",
          "LANE_INDEX:0", "LANE_INDEX:4", "LANE_INDEX:5", "LANE_INDEX:13",
          "LANE_INDEX:22", "TAIL_READY:-1"), events);
      tail.requireTarget(target);
      assertArrayEquals(writer.getResultHistoryDigest(), tail.getResultHistoryDigest());
      assertEquals(5, tail.getLanes().size());
      for (StateArchiveTailV5.LaneTerminal terminal : tail.getLanes()) {
        assertEquals(152, terminal.getCommittedIndexBytes());
        assertEquals(0, terminal.getTerminalFileId());
        assertEquals(704, terminal.getTerminalDataEndOffset());
      }
      assertArrayEquals(tail.encode(), StateArchiveTailV5.decode(tail.encode()).encode());
    }
  }

  @Test
  public void rejectsTailBeforeAppendAndWhenCommonTargetDiffers() throws Exception {
    Path root = temporaryFolder.newFolder("tail-reject").toPath();
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      assertThrows(IllegalArgumentException.class,
          () -> writer.forceTailReady(target(100)));
      writer.append(diff(100));
      assertThrows(IllegalArgumentException.class,
          () -> writer.forceTailReady(target(101)));
    }
  }

  private static StateArchiveSegmentHeaderV5 readHeader(Path path) throws Exception {
    return StateArchiveSegmentHeaderV5.decode(
        slice(path, 0, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
  }

  private static byte[] slice(Path path, int offset, int length) throws Exception {
    byte[] all = Files.readAllBytes(path);
    return java.util.Arrays.copyOfRange(all, offset, offset + length);
  }

  private static void assertRange(FrameRange range, long fileId, long start, long end) {
    assertEquals(fileId, range.getFileId());
    assertEquals(start, range.getStartOffset());
    assertEquals(end, range.getEndOffset());
  }

  private static BlockReverseDiff diff(long blockNumber) {
    return diff(blockNumber, hash((int) blockNumber - 1));
  }

  private static BlockReverseDiff diff(long blockNumber, byte[] parentHash) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber,
        hash((int) blockNumber), parentHash, blockNumber * 3_000), Collections.emptyList());
  }

  private static CommonCheckpointTarget target(int blockNumber) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, hash((int) blockNumber),
        hash((int) blockNumber - 1), blockNumber * 3_000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + blockNumber), meta, meta,
        hash(90), hash(91));
  }

  private static byte[] hash(int suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte element : value) {
      encoded.append(String.format("%02x", element));
    }
    return encoded.toString();
  }
}
