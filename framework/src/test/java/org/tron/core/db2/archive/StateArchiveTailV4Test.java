package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.MutableIndex;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.RuntimeBuilder;
import org.tron.core.db2.archive.StateArchiveTailV4.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveTailV4Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void fixedRecordBindsFiveLanesAndCommonIdentity() {
    CommonCheckpointTarget target = target(101);
    StateArchiveTailV4 tail = StateArchiveTailV4.forTarget(7, target, terminals(100, 101));

    byte[] encoded = tail.encode();
    assertEquals(440, encoded.length);
    StateArchiveTailV4 decoded = StateArchiveTailV4.decode(encoded);
    decoded.requireTarget(target);
    assertEquals(7, decoded.getCheckpointSequence());
    assertEquals(101, decoded.getCommonBlockNumber());
    assertEquals(5, decoded.getLanes().size());
    assertEquals(22, decoded.getLanes().get(4).getLaneId());
    assertEquals(128, decoded.getLanes().get(0).getFilesMetaCommittedBytes());

    assertThrows(IllegalArgumentException.class, () -> decoded.requireTarget(target(102)));
    byte[] corrupt = encoded.clone();
    corrupt[StateArchiveTailV4.HEADER_LENGTH + 20] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> StateArchiveTailV4.decode(corrupt));
  }

  @Test
  public void rejectsLaneOrderCoverageAndMetaBoundaryDrift() {
    List<LaneTerminal> wrongOrder = terminals(100, 101);
    Collections.swap(wrongOrder, 0, 1);
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV4.forTarget(0, target(101), wrongOrder));

    List<LaneTerminal> shortLane = terminals(100, 101);
    shortLane.set(2, terminal(5, 100, 100, 128));
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV4.forTarget(0, target(101), shortLane));

    List<LaneTerminal> wrongMeta = terminals(100, 101);
    wrongMeta.set(3, terminal(13, 100, 101, 192));
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV4.forTarget(0, target(101), wrongMeta));
  }

  @Test
  public void reservedTailSurvivesIndexAppendReopenAndShadowCheckpoint() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("archive-tail-index").toPath();
    Path indexPath = root.resolve("index");
    CommonCheckpointTarget target = target(101);
    StateArchiveTailV4 tail = StateArchiveTailV4.forTarget(9, target, terminals(100, 101));

    try (MutableIndex empty = new MutableIndex(indexPath, Engine.LEVELDB)) {
      assertThrows(ArchivePersistenceException.class, () -> empty.archiveTail(target));
      empty.publishArchiveTail(tail);
    }
    try (MutableIndex index = new MutableIndex(indexPath, Engine.LEVELDB)) {
      assertEquals(9, index.archiveTail(target).getCheckpointSequence());
      index.publishArchiveTail(tail);
      assertThrows(ArchivePersistenceException.class, () -> index.publishArchiveTail(
          StateArchiveTailV4.forTarget(8, target, terminals(100, 101))));
      index.append("first", plan(101), hash(1), () -> { }, () -> { });
      assertEquals(9, index.archiveTail(target).getCheckpointSequence());
      try (PersistentServingKeyIndexGeneration source = index.pin();
          RuntimeBuilder builder = source.openRuntimeBuilder(root.resolve("builder"));
          PersistentServingKeyIndexGeneration shadow = builder.extendExact(
              root.resolve("shadow"), "second", plan(102), hash(2))) {
        assertEquals(9, source.archiveTail(target).getCheckpointSequence());
        assertEquals(9, shadow.archiveTail(target).getCheckpointSequence());
        assertEquals(102, shadow.getIndexedThrough());
      }
    }
    try (MutableIndex reopened = new MutableIndex(indexPath, Engine.LEVELDB)) {
      assertEquals(9, reopened.archiveTail(target).getCheckpointSequence());
      assertThrows(IllegalArgumentException.class, () -> reopened.archiveTail(target(102)));
    }
  }

  private static List<LaneTerminal> terminals(long first, long end) {
    List<LaneTerminal> result = new ArrayList<>();
    for (int lane : new int[]{0, 4, 5, 13, 22}) {
      result.add(terminal(lane, first, end, 128));
    }
    return result;
  }

  private static LaneTerminal terminal(int lane, long first, long end, long metaBytes) {
    return new LaneTerminal(lane, StateArchiveTailV4.TERMINAL_OPEN, 0, first, end,
        1024 + lane, metaBytes);
  }

  private static ServingIndexIncrementalPlan plan(int block) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L);
    BlockReverseDiff diff = new BlockReverseDiff(meta,
        Collections.singletonList(new BlockReverseDiff.DbGroup("code",
            Collections.singletonList(new BlockReverseDiff.Entry(new byte[]{7},
                OldValue.present(hash(block)))))));
    return ServingIndexIncrementalPlan.planCommittedDiffs(block - 1, hash(block - 1),
        Collections.singletonList(diff));
  }

  private static CommonCheckpointTarget target(int block) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + block), meta, meta,
        hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
