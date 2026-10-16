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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.PointLocation;
import org.tron.core.db2.archive.StateArchiveServingIndexBuildCoordinatorV3.Mode;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveServingSourceV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 15000)
  public void reconstructsRequestedRangeAndFeedsExistingServingOwner() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Fixture fixture = fixture("valid", true);
    StateArchiveServingSourceV5 source = fixture.source(fixture.metas::get);

    assertThrows(StateArchiveServingSource.ReadBudgetException.class,
        () -> source.readCommittedDiffs(99, 100, 1));
    List<BlockReverseDiff> replayed = source.readCommittedDiffs(99, 102, Long.MAX_VALUE);
    assertEquals(3, replayed.size());
    assertEquals(fixture.metas.get(100L), replayed.get(0).getMeta());
    assertEquals("code", replayed.get(0).getGroups().get(0).getDbName());
    assertThrows(IllegalArgumentException.class,
        () -> source.readCommittedDiffs(98, 100, Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> source.readCommittedDiffs(99, 103, Long.MAX_VALUE));

    try (StateArchiveServingWorkerV3 worker = new StateArchiveServingWorkerV3(
        () -> new StateArchiveServingIndexBuildCoordinatorV3(
            fixture.root, Engine.LEVELDB, 1_000), source, () -> { }, 0)) {
      worker.offer(fixture.target);
      worker.completeInitialSync(fixture.target);
      assertEquals(Mode.LIVE_BACKGROUND, worker.status().getMode());
      assertEquals(99, worker.status().getIndexedFrom());
      assertEquals(102, worker.status().getIndexedThrough());
    }
  }

  @Test
  public void rejectsCanonicalMismatchAndMissingCoverage() throws Exception {
    Fixture fixture = fixture("identity", false);
    Map<Long, BlockSnapshotMeta> wrong = new HashMap<>(fixture.metas);
    wrong.put(101L, BlockSnapshotMeta.forBlock(101, hash(77), hash(100), 303_000L));
    assertThrows(IOException.class, () -> fixture.source(wrong::get)
        .readCommittedDiffs(99, 102, Long.MAX_VALUE));

    Map<Long, BlockSnapshotMeta> missing = new HashMap<>(fixture.metas);
    missing.remove(101L);
    assertThrows(IOException.class, () -> fixture.source(missing::get)
        .readCommittedDiffs(99, 102, Long.MAX_VALUE));
  }

  @Test
  public void publishedAdvanceReadsOnlyRequestedSuffix() throws Exception {
    Path root = temporaryFolder.newFolder("advance").toPath();
    byte[] baseline = hash(90);
    Map<Long, BlockSnapshotMeta> metas = new HashMap<>();
    for (int block = 100; block <= 102; block++) {
      metas.put((long) block, BlockSnapshotMeta.forBlock(block, hash(block),
          hash(block - 1), block * 3_000L));
    }
    CommonCheckpointTarget through101 = CommonCheckpointTarget.restore(hash(70), hash(71),
        metas.get(100L), metas.get(101L), hash(72), hash(73));
    CommonCheckpointTarget through102 = CommonCheckpointTarget.restore(hash(74), hash(75),
        metas.get(102L), metas.get(102L), hash(76), hash(77));
    List<Long> canonicalReads = new ArrayList<>();
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, baseline, 700)) {
      writer.append(new BlockReverseDiff(metas.get(100L), Collections.emptyList()));
      writer.append(new BlockReverseDiff(metas.get(101L), Collections.emptyList()));
      StateArchiveTailV5 tail101 = writer.forceTailReady(through101);
      StateArchiveServingSourceV5 source = new StateArchiveServingSourceV5(
          root, tail101, through101, baseline, block -> {
        canonicalReads.add(block);
        return metas.get(block);
      });

      writer.append(new BlockReverseDiff(metas.get(102L), Collections.emptyList()));
      StateArchiveTailV5 tail102 = writer.forceTailReady(through102);
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        Files.delete(StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 0));
      }
      source.updatePublished(tail102, through102);
      List<BlockReverseDiff> replayed = source.readCommittedDiffs(
          101, 102, Long.MAX_VALUE);

      assertEquals(1, replayed.size());
      assertEquals(Collections.singletonList(102L), canonicalReads);
    }
  }

  @Test
  public void rejectsNonTerminalFrameCorruption() throws Exception {
    Fixture fixture = fixture("corrupt", false);
    PointLocation location = location(fixture, 0, 100);
    flip(location.getDataPath(), location.getStartOffset() + 8);

    assertThrows(IOException.class, () -> fixture.source(fixture.metas::get)
        .readCommittedDiffs(99, 102, Long.MAX_VALUE));
  }

  @Test
  public void rejectsCrossLaneFrameIdentity() throws Exception {
    Fixture fixture = fixture("lane", false);
    PointLocation shared = location(fixture, 0, 100);
    PointLocation account = location(fixture, 4, 100);
    byte[] accountFrame = read(account);
    assertEquals(shared.getEndOffset() - shared.getStartOffset(), accountFrame.length);
    write(shared, accountFrame);

    assertThrows(IOException.class, () -> fixture.source(fixture.metas::get)
        .readCommittedDiffs(99, 102, Long.MAX_VALUE));
  }

  private Fixture fixture(String name, boolean withCode) throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    byte[] baseline = hash(90);
    Map<Long, BlockSnapshotMeta> metas = new HashMap<>();
    List<BlockReverseDiff> diffs = new ArrayList<>();
    for (int block = 100; block <= 102; block++) {
      BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block),
          hash(block - 1), block * 3_000L);
      metas.put((long) block, meta);
      List<DbGroup> groups = withCode ? Collections.singletonList(new DbGroup("code",
          Collections.singletonList(new Entry(new byte[]{1},
              OldValue.present(new byte[]{(byte) block}))))) : Collections.emptyList();
      diffs.add(new BlockReverseDiff(meta, groups));
    }
    CommonCheckpointTarget target = CommonCheckpointTarget.restore(hash(70), hash(71),
        metas.get(100L), metas.get(102L), hash(72), hash(73));
    StateArchiveTailV5 tail;
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, baseline, 700)) {
      for (BlockReverseDiff diff : diffs) {
        writer.append(diff);
      }
      tail = writer.forceTailReady(target);
    }
    return new Fixture(root, baseline, target, tail, metas);
  }

  private static PointLocation location(Fixture fixture, int laneId, long block)
      throws IOException {
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      return view.capture(laneId, block);
    }
  }

  private static byte[] read(PointLocation location) throws IOException {
    int length = (int) (location.getEndOffset() - location.getStartOffset());
    ByteBuffer bytes = ByteBuffer.allocate(length);
    try (FileChannel channel = FileChannel.open(location.getDataPath(),
        StandardOpenOption.READ)) {
      while (bytes.hasRemaining()) {
        channel.read(bytes, location.getStartOffset() + bytes.position());
      }
    }
    return bytes.array();
  }

  private static void write(PointLocation location, byte[] bytes) throws IOException {
    ByteBuffer source = ByteBuffer.wrap(bytes);
    try (FileChannel channel = FileChannel.open(location.getDataPath(),
        StandardOpenOption.WRITE)) {
      while (source.hasRemaining()) {
        channel.write(source, location.getStartOffset() + source.position());
      }
      channel.force(false);
    }
  }

  private static void flip(Path path, long position) throws IOException {
    ByteBuffer value = ByteBuffer.allocate(1);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      channel.read(value, position);
      value.array()[0] ^= 1;
      channel.write(ByteBuffer.wrap(value.array()), position);
      channel.force(false);
    }
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }

  private static final class Fixture {
    private final Path root;
    private final byte[] baseline;
    private final CommonCheckpointTarget target;
    private final StateArchiveTailV5 tail;
    private final Map<Long, BlockSnapshotMeta> metas;

    private Fixture(Path root, byte[] baseline, CommonCheckpointTarget target,
        StateArchiveTailV5 tail, Map<Long, BlockSnapshotMeta> metas) {
      this.root = root;
      this.baseline = Arrays.copyOf(baseline, baseline.length);
      this.target = target;
      this.tail = tail;
      this.metas = metas;
    }

    private StateArchiveServingSourceV5 source(
        StateArchiveCanonicalBlockMetaSource canonical) {
      return new StateArchiveServingSourceV5(root, tail, target, baseline, canonical);
    }
  }
}
