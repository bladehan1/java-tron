package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveServingIndexBuildCoordinatorV3.Mode;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveRuntimeV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 30000)
  public void publishesBuildsServesSuccessorAndReopensAtExactCommon() throws Exception {
    Path root = temporaryFolder.newFolder("v5-runtime").toPath();
    byte[] format = hash(70);
    byte[] baseline = hash(90);
    Map<Long, BlockSnapshotMeta> canonical = new HashMap<>();
    for (int block = 100; block <= 102; block++) {
      canonical.put((long) block, meta(block));
    }

    CommonCheckpointTarget successor;
    try (StateArchiveAppendCheckpointMaterializerV5 archive = runtime(
        root, format, baseline, canonical)) {
      CommonCheckpointTarget initial = publish(archive, format,
          Arrays.asList(diff(100), diff(101)));
      archive.afterCommit(initial);
      archive.completeServingInitialSync(initial);
      assertEquals(Mode.LIVE_BACKGROUND, archive.servingIndexStatus().getMode());
      assertEquals(99, archive.servingIndexStatus().getIndexedFrom());
      assertEquals(101, archive.servingIndexStatus().getIndexedThrough());
      assertHistory(archive, initial, 99, 100);

      successor = publish(archive, format, Collections.singletonList(diff(102)));
      archive.afterCommit(successor);
      awaitIndexed(archive, 102);
      assertEquals(102, archive.servingIndexStatus().getIndexedThrough());
      assertHistory(archive, successor, 101, 102);
    }

    try (StateArchiveAppendCheckpointMaterializerV5 reopened = runtime(
        root, format, baseline, canonical)) {
      assertEquals(successor, reopened.loadPublishedTargetIfPresent().get());
      reopened.afterCommit(successor);
      reopened.completeServingInitialSync(successor);
      assertEquals(Mode.LIVE_BACKGROUND, reopened.servingIndexStatus().getMode());
      assertEquals(99, reopened.servingIndexStatus().getIndexedFrom());
      assertEquals(102, reopened.servingIndexStatus().getIndexedThrough());
      assertEquals(0, reopened.servingIndexStatus().getBuildSequence());
      assertHistory(reopened, successor, 101, 102);
    }
  }

  private static StateArchiveAppendCheckpointMaterializerV5 runtime(Path root,
      byte[] format, byte[] baseline, Map<Long, BlockSnapshotMeta> canonical)
      throws Exception {
    return new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
        100, baseline, 700, (required, reserve) -> { }, canonical::get, null,
        (path, position, length) -> { }, (stage, laneId) -> { }, () -> { });
  }

  private static void awaitIndexed(StateArchiveAppendCheckpointMaterializerV5 archive,
      long block) throws InterruptedException {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (archive.servingIndexStatus().getIndexedThrough() != block
        && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(block, archive.servingIndexStatus().getIndexedThrough());
  }

  private static CommonCheckpointTarget publish(
      StateArchiveAppendCheckpointMaterializerV5 archive, byte[] format,
      List<BlockReverseDiff> diffs) throws Exception {
    StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
    CommonCheckpointPayload payload = payload(format, diffs, descriptor);
    CommonCheckpointCapture capture = CommonCheckpointCapture.create(
        payload, diffs, descriptor);
    CommonCheckpointTarget target = archive.prepare(capture);
    archive.materialize(payload, target);
    archive.publish(target);
    return target;
  }

  private static void assertHistory(StateArchiveAppendCheckpointMaterializerV5 archive,
      CommonCheckpointTarget target, long after, int expected) throws Exception {
    try (CheckpointPointHistory history = archive.pinHistory(target)) {
      assertEquals(99, history.getIndexedFrom());
      assertEquals(target.getLastBlock().getBlockNumber(), history.getIndexedThrough());
      OldValue value = history.findOldValueAfter("code", new byte[]{1}, after).get();
      assertTrue(value.isPresent());
      assertArrayEquals(new byte[]{(byte) expected}, value.getValue());
    }
  }

  private static CommonCheckpointPayload payload(byte[] format,
      List<BlockReverseDiff> diffs, StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      PathStateFlushTarget.BlockBinding binding = mock(
          PathStateFlushTarget.BlockBinding.class);
      when(binding.getMeta()).thenReturn(meta);
      when(binding.getParentStateRoot()).thenReturn(hash(30 + (int) meta.getBlockNumber()));
      when(binding.getStateRoot()).thenReturn(hash(31 + (int) meta.getBlockNumber()));
      when(binding.getTransitionPayloadDigest()).thenReturn(hash(91));
      bindings.add(binding);
    }
    PathStateFlushTarget path = mock(PathStateFlushTarget.class);
    byte[] parentStateRoot = bindings.get(0).getParentStateRoot();
    byte[] stateRoot = bindings.get(bindings.size() - 1).getStateRoot();
    when(path.getBlocks()).thenReturn(bindings);
    when(path.getParentStateRoot()).thenReturn(parentStateRoot);
    when(path.getStateRoot()).thenReturn(stateRoot);
    when(path.getStores()).thenReturn(Collections.emptyList());
    when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.createV2(format, path, descriptor,
        Collections.emptyList());
  }

  private static BlockReverseDiff diff(int block) {
    return new BlockReverseDiff(meta(block), Collections.singletonList(new DbGroup("code",
        Collections.singletonList(new Entry(new byte[]{1},
            OldValue.present(new byte[]{(byte) block}))))));
  }

  private static BlockSnapshotMeta meta(int block) {
    return BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block * 3_000L);
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
