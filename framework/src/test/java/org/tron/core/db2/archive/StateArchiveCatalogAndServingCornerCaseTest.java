package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.MutableIndex;
import org.tron.core.db2.archive.StateArchiveServingIndexBuildCoordinatorV3.LiveServingIndexer;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveCatalogAndServingCornerCaseTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void bulkBatchCutsProduceTheSameExactIndexIdentity() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    List<BlockReverseDiff> all = diffs(1, 6, 0);
    Path oneBatch = temporaryFolder.newFolder("one-batch").toPath();
    Path splitBatch = temporaryFolder.newFolder("split-batch").toPath();
    try (StateArchiveServingIndexBuildCoordinatorV3 one =
        new StateArchiveServingIndexBuildCoordinatorV3(oneBatch, Engine.LEVELDB, 6);
        StateArchiveServingIndexBuildCoordinatorV3 split =
            new StateArchiveServingIndexBuildCoordinatorV3(splitBatch, Engine.LEVELDB, 3)) {
      one.offerCommittedRange(all, target(all, 1));
      split.offerCommittedRange(all.subList(0, 2), target(all.subList(0, 2), 2));
      split.offerCommittedRange(all.subList(2, 3), target(all.subList(2, 3), 3));
      split.offerCommittedRange(all.subList(3, 6), target(all.subList(3, 6), 4));
      assertEquals(6, one.status().getIndexedThrough());
      assertEquals(6, split.status().getIndexedThrough());
    }
    try (MutableIndex one = new MutableIndex(
        oneBatch.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
                .resolve("single-v1"),
        Engine.LEVELDB);
        MutableIndex split = new MutableIndex(
            splitBatch.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
                .resolve("single-v1"),
            Engine.LEVELDB);
        PersistentServingKeyIndexGeneration oneGeneration = one.pin();
        PersistentServingKeyIndexGeneration splitGeneration = split.pin()) {
      assertArrayEquals(oneGeneration.getAuthoritativePrefixDigest(),
          splitGeneration.getAuthoritativePrefixDigest());
      assertEquals(oneGeneration.getKeyChangeCount(), splitGeneration.getKeyChangeCount());
      OptionalLong oneChange = oneGeneration.firstChangeAfter("code", new byte[]{3}, 0, 6);
      OptionalLong splitChange = splitGeneration.firstChangeAfter("code", new byte[]{3}, 0, 6);
      assertEquals(oneChange, splitChange);
    }
  }

  @Test
  public void liveRangePublishesOnceWithTheBulkLogicalIdentity() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    List<BlockReverseDiff> all = diffs(1, 17, 0);
    Path bulkRoot = temporaryFolder.newFolder("bulk-reference").toPath();
    Path liveRoot = temporaryFolder.newFolder("live-range").toPath();
    try (StateArchiveServingIndexBuildCoordinatorV3 bulk =
        new StateArchiveServingIndexBuildCoordinatorV3(bulkRoot, Engine.LEVELDB, 17);
        StateArchiveServingIndexBuildCoordinatorV3 liveOwner =
            new StateArchiveServingIndexBuildCoordinatorV3(liveRoot, Engine.LEVELDB, 1)) {
      bulk.offerCommittedRange(all, target(all, 1));
      List<BlockReverseDiff> first = all.subList(0, 1);
      liveOwner.offerCommittedRange(first, target(first, 2));
      LiveServingIndexer live = liveOwner.completeInitialSync(target(first, 2));
      List<BlockReverseDiff> suffix = all.subList(1, all.size());
      live.indexNow(suffix, target(suffix, 3));
      assertEquals(17, liveOwner.status().getIndexedThrough());
      // One initial build plus one atomic publication for the full 16-block Common range.
      assertEquals(2, liveOwner.status().getBuildSequence());
      assertEquals(0, liveOwner.status().getPendingBlocks());
    }
    try (MutableIndex bulk = new MutableIndex(
        bulkRoot.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
                .resolve("single-v1"),
        Engine.LEVELDB);
        MutableIndex live = new MutableIndex(
            liveRoot.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
                .resolve("single-v1"),
            Engine.LEVELDB);
        PersistentServingKeyIndexGeneration expected = bulk.pin();
        PersistentServingKeyIndexGeneration actual = live.pin()) {
      assertArrayEquals(expected.getAuthoritativePrefixDigest(),
          actual.getAuthoritativePrefixDigest());
      assertEquals(expected.getKeyChangeCount(), actual.getKeyChangeCount());
      for (int key = 0; key < 8; key++) {
        assertEquals(expected.firstChangeAfter("code", new byte[]{(byte) key}, 0, 17),
            actual.firstChangeAfter("code", new byte[]{(byte) key}, 0, 17));
      }
    }
  }

  @Test
  public void gapInvalidatesLiveHandleWithoutAdvancingI() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("live-gap").toPath();
    List<BlockReverseDiff> first = diffs(1, 1, 0);
    try (StateArchiveServingIndexBuildCoordinatorV3 coordinator =
        new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.LEVELDB, 1)) {
      CommonCheckpointTarget firstTarget = target(first, 1);
      coordinator.offerCommittedRange(first, firstTarget);
      LiveServingIndexer live = coordinator.completeInitialSync(firstTarget);
      List<BlockReverseDiff> gap = diffs(3, 1, 2);
      assertThrows(IllegalArgumentException.class,
          () -> live.indexNow(gap, target(gap, 3)));
      assertEquals(1, coordinator.status().getIndexedThrough());
      assertEquals(StateArchiveServingIndexBuildCoordinatorV3.Mode.CATCH_UP_REQUIRED,
          coordinator.status().getMode());
      List<BlockReverseDiff> successor = diffs(2, 1, 1);
      assertThrows(IllegalStateException.class,
          () -> live.indexNow(successor, target(successor, 2)));
    }
  }

  private static List<BlockReverseDiff> diffs(int first, int count, int parent) {
    List<BlockReverseDiff> result = new ArrayList<>();
    for (int block = first; block < first + count; block++) {
      result.add(diff(block, block == first ? parent : block - 1, 8));
    }
    return result;
  }

  private static BlockReverseDiff diff(int block, int parent, int valueLength) {
    byte[] value = new byte[valueLength];
    Arrays.fill(value, (byte) block);
    List<DbGroup> groups = valueLength == 0 ? Collections.emptyList()
        : Collections.singletonList(new DbGroup("code", Collections.singletonList(
            new Entry(new byte[]{(byte) (block & 7)}, OldValue.present(value)))));
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block),
        hash(parent), block * 3_000L), groups);
  }

  private static CommonCheckpointTarget target(List<BlockReverseDiff> diffs, int salt) {
    return CommonCheckpointTarget.restore(hash(70), hash(80 + salt),
        diffs.get(0).getMeta(), diffs.get(diffs.size() - 1).getMeta(), hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    byte[] result = new byte[32];
    result[27] = (byte) value;
    result[31] = (byte) value;
    return result;
  }
}
