package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.FrameRange;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveCommittedViewV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void reopensExactCommittedPrefixAcrossSegments() throws Exception {
    Fixture fixture = createFixture("exact", 102, 650);

    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      assertEquals(100, view.getFirstBlockNumber());
      assertEquals(102, view.getCommittedBlockNumber());
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        assertRange(view.locate(laneId, 100), 0, 512, 608);
        assertRange(view.locate(laneId, 102), 1, 512, 608);
      }
      assertThrows(IllegalArgumentException.class, () -> view.locate(0, 99));
      assertThrows(IllegalArgumentException.class, () -> view.locate(0, 103));
    }
  }

  @Test
  public void pinsVisibilityButRejectsSuffixOnLaterReopen() throws Exception {
    Path root = temporaryFolder.newFolder("suffix").toPath();
    StateArchiveTailV5 tail;
    CommonCheckpointTarget target = target(101);
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      writer.append(diff(100));
      writer.append(diff(101));
      tail = writer.forceTailReady(target);
      try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
          root, tail, target)) {
        writer.append(diff(102));
        assertRange(view.locate(0, 101), 0, 608, 704);
        assertThrows(IllegalArgumentException.class, () -> view.locate(0, 102));
      }
    }

    StateArchiveTailV5 committedTail = tail;
    assertThrows(IOException.class,
        () -> StateArchiveCommittedViewV5.open(root, committedTail, target));
  }

  @Test
  public void rejectsMissingAndShortIndexOrData() throws Exception {
    Fixture missingIndex = createFixture("missing-index", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    Files.delete(indexPath(missingIndex.root, 0));
    assertReopenFails(missingIndex);

    Fixture shortIndex = createFixture("short-index", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    truncate(indexPath(shortIndex.root, 4), 144);
    assertReopenFails(shortIndex);

    Fixture missingData = createFixture("missing-data", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    Files.delete(StateArchiveFiveLaneWriterV5.dataPath(missingData.root, 5, 0));
    assertReopenFails(missingData);

    Fixture shortData = createFixture("short-data", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    truncate(StateArchiveFiveLaneWriterV5.dataPath(shortData.root, 13, 0), 703);
    assertReopenFails(shortData);
  }

  @Test
  public void rejectsExtraSegmentHeaderChainTerminalAndCommonDrift() throws Exception {
    Fixture extra = createFixture("extra-segment", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    Path extraPath = StateArchiveFiveLaneWriterV5.dataPath(extra.root, 0, 1);
    Files.createDirectories(extraPath.getParent());
    Files.copy(StateArchiveFiveLaneWriterV5.dataPath(extra.root, 0, 0), extraPath);
    assertReopenFails(extra);

    Fixture chain = createFixture("header-chain", 102, 650);
    Path firstPath = StateArchiveFiveLaneWriterV5.dataPath(chain.root, 4, 0);
    Path secondPath = StateArchiveFiveLaneWriterV5.dataPath(chain.root, 4, 1);
    StateArchiveSegmentHeaderV5 first = readHeader(firstPath);
    StateArchiveSegmentHeaderV5 second = readHeader(secondPath);
    StateArchiveSegmentHeaderV5 wrongSuccessor = StateArchiveSegmentHeaderV5.next(
        first, hash(77), 102, second.getStartHistoryDigest());
    writeAt(secondPath, 0, wrongSuccessor.encode());
    assertReopenFails(chain);

    Fixture terminal = createFixture("terminal", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    StateArchiveTailV5 wrongTail = StateArchiveTailV5.forTarget(
        terminal.tail.getFirstBlockNumber(), terminal.target,
        terminal.tail.getResultHistoryDigest(), terminalsWithWrongFrame(terminal.tail));
    assertThrows(IOException.class, () -> StateArchiveCommittedViewV5.open(
        terminal.root, wrongTail, terminal.target));

    Fixture common = createFixture("common", 101,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    assertThrows(IllegalArgumentException.class, () -> StateArchiveCommittedViewV5.open(
        common.root, common.tail, target(102)));
  }

  private Fixture createFixture(String name, long lastBlock, long rotationBytes)
      throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    CommonCheckpointTarget target = target((int) lastBlock);
    StateArchiveTailV5 tail;
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99), rotationBytes)) {
      for (long block = 100; block <= lastBlock; block++) {
        writer.append(diff(block));
      }
      tail = writer.forceTailReady(target);
    }
    return new Fixture(root, tail, target);
  }

  private static List<LaneTerminal> terminalsWithWrongFrame(StateArchiveTailV5 tail) {
    List<LaneTerminal> result = new ArrayList<>();
    for (LaneTerminal terminal : tail.getLanes()) {
      byte[] frameDigest = terminal.getLaneId() == 0
          ? hash(77) : terminal.getTerminalFrameDigest();
      result.add(new LaneTerminal(terminal.getLaneId(), terminal.getFlags(),
          terminal.getTerminalFileId(), terminal.getCommittedIndexBytes(),
          terminal.getTerminalDataEndOffset(), frameDigest,
          terminal.getTerminalSegmentHeaderDigest()));
    }
    return result;
  }

  private static void assertReopenFails(Fixture fixture) {
    assertThrows(IOException.class, () -> StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target));
  }

  private static Path indexPath(Path root, int laneId) {
    return StateArchiveFiveLaneWriterV5.laneRoot(root, laneId).resolve("blocks.idx");
  }

  private static void truncate(Path path, long length) throws Exception {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      channel.truncate(length);
    }
  }

  private static void writeAt(Path path, long position, byte[] value) throws Exception {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      ByteBuffer bytes = ByteBuffer.wrap(value);
      while (bytes.hasRemaining()) {
        channel.write(bytes, position + bytes.position());
      }
    }
  }

  private static StateArchiveSegmentHeaderV5 readHeader(Path path) throws Exception {
    byte[] data = Files.readAllBytes(path);
    return StateArchiveSegmentHeaderV5.decode(java.util.Arrays.copyOf(
        data, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
  }

  private static void assertRange(FrameRange range, long fileId, long start, long end) {
    assertEquals(fileId, range.getFileId());
    assertEquals(start, range.getStartOffset());
    assertEquals(end, range.getEndOffset());
  }

  private static BlockReverseDiff diff(long blockNumber) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber,
        hash((int) blockNumber), hash((int) blockNumber - 1), blockNumber * 3_000),
        Collections.emptyList());
  }

  private static CommonCheckpointTarget target(int blockNumber) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + blockNumber), meta, meta,
        hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }

  private static final class Fixture {
    private final Path root;
    private final StateArchiveTailV5 tail;
    private final CommonCheckpointTarget target;

    private Fixture(Path root, StateArchiveTailV5 tail, CommonCheckpointTarget target) {
      this.root = root;
      this.tail = tail;
      this.target = target;
    }
  }
}
