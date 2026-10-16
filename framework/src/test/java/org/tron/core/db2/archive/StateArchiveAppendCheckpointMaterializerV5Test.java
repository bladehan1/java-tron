package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.MutableIndex;
import org.tron.core.db2.archive.StateArchiveAppendCheckpointMaterializerV5.PublicationStage;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveAppendCheckpointMaterializerV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test
  public void forcesDataThenIndexSyncsTailThenPublishesCommonMarker() throws Exception {
    Path root = temporaryFolder.newFolder("v5-materializer").toPath();
    byte[] format = hash(70);
    List<BlockReverseDiff> diffs = Arrays.asList(diff(100), diff(101));
    List<String> events = new ArrayList<>();

    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            (required, reserve) -> { }, null, (path, position, length) -> { },
            (stage, laneId) -> events.add(stage + ":" + laneId), () -> { })) {
      StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
      CommonCheckpointPayload payload = payload(format, diffs, descriptor);
      CommonCheckpointCapture capture = CommonCheckpointCapture.create(
          payload, diffs, descriptor);
      CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);

      assertEquals(Status.NEEDS_MATERIALIZATION, archive.inspect(target));
      assertEquals(target, archive.prepare(capture));
      assertEquals(Status.MATERIALIZED, archive.inspect(target));
      assertFalse(StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(root)
          .isPresent());
      assertForceOrder(events);

      archive.materialize(payload, target);
      archive.publish(target);
      assertEquals(Status.PUBLISHED, archive.inspect(target));
      assertEquals(target, StateArchiveCheckpointMaterializer
          .loadReadableTargetIfPresent(root).get());
      assertEquals(PublicationStage.COMMON_PUBLISHED + ":-1",
          events.get(events.size() - 1));

      events.clear();
      List<BlockReverseDiff> successor = Collections.singletonList(diff(102));
      StateArchiveHotBatchDescriptor nextDescriptor = archive.planCheckpoint(successor);
      CommonCheckpointPayload nextPayload = payload(format, successor, nextDescriptor);
      CommonCheckpointTarget nextTarget = archive.prepare(CommonCheckpointCapture.create(
          nextPayload, successor, nextDescriptor));
      archive.publish(nextTarget);
      assertEquals(Status.PUBLISHED, archive.inspect(nextTarget));
      assertForceOrder(events.subList(0, 11));
      assertEquals(PublicationStage.COMMON_PUBLISHED + ":-1", events.get(11));
    }
  }

  @Test
  public void restartUsesCommonTargetRatherThanNewestPreparedTail() throws Exception {
    Path root = temporaryFolder.newFolder("v5-common-tail-restart").toPath();
    byte[] format = hash(70);
    CommonCheckpointTarget committed;
    CommonCheckpointTarget pending;
    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            number -> null)) {
      List<BlockReverseDiff> first = Collections.singletonList(diff(100));
      StateArchiveHotBatchDescriptor binding = archive.planCheckpoint(first);
      CommonCheckpointPayload payload = payload(format, first, binding);
      committed = archive.prepare(CommonCheckpointCapture.create(payload, first, binding));
      archive.publish(committed);
      List<BlockReverseDiff> second = Collections.singletonList(diff(101));
      binding = archive.planCheckpoint(second);
      payload = payload(format, second, binding);
      pending = archive.prepare(CommonCheckpointCapture.create(payload, second, binding));
    }
    // The second prepared Tail is durable, but the caller's Common commit is still block 100.
    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        StateArchiveAppendCheckpointMaterializerV5.openForCommon(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            number -> null, committed)) {
      assertEquals(Status.PUBLISHED, archive.inspect(committed));
      assertEquals(Status.NEEDS_MATERIALIZATION, archive.inspect(pending));
      List<BlockReverseDiff> second = Collections.singletonList(diff(101));
      StateArchiveHotBatchDescriptor binding = archive.planCheckpoint(second);
      CommonCheckpointPayload payload = payload(format, second, binding);
      CommonCheckpointTarget retry = archive.prepare(
          CommonCheckpointCapture.create(payload, second, binding));
      assertEquals(pending, retry);
      archive.publish(retry);
    }
    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        StateArchiveAppendCheckpointMaterializerV5.openForCommon(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            number -> null, pending)) {
      assertEquals(Status.PUBLISHED, archive.inspect(pending));
    }
  }

  @Test
  public void committedRedoPublishesPreparedHistoryAfterRestart() throws Exception {
    Path root = temporaryFolder.newFolder("v5-prepared-common-restart").toPath();
    byte[] format = hash(70);
    CommonCheckpointTarget pending;
    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            number -> null)) {
      List<BlockReverseDiff> first = Collections.singletonList(diff(100));
      StateArchiveHotBatchDescriptor binding = archive.planCheckpoint(first);
      CommonCheckpointPayload payload = payload(format, first, binding);
      pending = archive.prepare(CommonCheckpointCapture.create(payload, first, binding));
    }
    try (StateArchiveAppendCheckpointMaterializerV5 archive =
        StateArchiveAppendCheckpointMaterializerV5.openForCommon(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            number -> null, pending)) {
      assertEquals(Status.MATERIALIZED, archive.inspect(pending));
      archive.publish(pending);
      assertEquals(Status.PUBLISHED, archive.inspect(pending));
    }
  }

  @Test
  public void interruptedLegacyTailMigrationIsRetryableAndRetiresServingCopy() throws Exception {
    for (boolean publishedTail : new boolean[]{false, true}) {
      Path root = temporaryFolder.newFolder().toPath();
      byte[] format = hash(70);
      List<BlockReverseDiff> first = Collections.singletonList(diff(100));
      StateArchiveHotBatchDescriptor binding = StateArchiveHotStore.planCheckpointDescriptor(
          Engine.LEVELDB, 99, hash(99), new byte[32], first);
      CommonCheckpointTarget target = CommonCheckpointTarget.from(payload(format, first, binding));
      try (StateArchiveFiveLaneWriterV5 writer =
          new StateArchiveFiveLaneWriterV5(root, 100, hash(99));
          StateArchiveServingIndexBuildCoordinatorV3 coordinator =
              new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.LEVELDB, 1000)) {
        writer.append(diff(100));
        StateArchiveTailV5 tail = writer.forceTailReady(target);
        coordinator.publishArchiveTailV5(tail);
        StateArchiveCheckpointMaterializer.publishReadableTarget(root, target);
        // Both interruption points keep the exact legacy Common-bound source until retirement.
        try (HistoryTailStore control = new HistoryTailStore(root, format, 100, hash(99))) {
          if (publishedTail) {
            control.publish(tail, null, () -> { });
          }
        }
      }
      try (StateArchiveAppendCheckpointMaterializerV5 archive =
          StateArchiveAppendCheckpointMaterializerV5.openForCommon(root, format, Engine.LEVELDB,
              100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
              number -> null, target)) {
        assertEquals(Status.PUBLISHED, archive.inspect(target));
      }
      Path index = root.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
          .resolve("single-v1");
      try (MutableIndex reopened = new MutableIndex(index, Engine.LEVELDB)) {
        assertThrows(ArchivePersistenceException.class, () -> reopened.archiveTailV5(target));
      }
    }
  }

  @Test
  public void tailSyncFailureNeverPublishesCommon() throws Exception {
    Path root = temporaryFolder.newFolder("v5-tail-sync-failure").toPath();
    byte[] format = hash(71);
    List<BlockReverseDiff> diffs = Collections.singletonList(diff(100));
    List<String> events = new ArrayList<>();

    StateArchiveAppendCheckpointMaterializerV5 archive =
        new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            (required, reserve) -> { }, null, (path, position, length) -> { },
            (stage, laneId) -> events.add(stage + ":" + laneId), () -> {
          throw new IOException("injected tail sync failure");
        });
    StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
    CommonCheckpointPayload payload = payload(format, diffs, descriptor);
    CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
    assertThrows(IOException.class, () -> archive.prepare(
        CommonCheckpointCapture.create(payload, diffs, descriptor)));
    assertFalse(StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(root)
        .isPresent());
    assertEquals(10, events.size());
    archive.close();

    Path index = root.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
        .resolve("single-v1");
    try (MutableIndex reopened = new MutableIndex(index, Engine.LEVELDB)) {
      assertThrows(ArchivePersistenceException.class,
          () -> reopened.archiveTailV5(target));
    }
  }

  @Test
  public void rejectsNonFreshRootBeforeCreatingV5State() throws Exception {
    Path root = temporaryFolder.newFolder("v5-old-root").toPath();
    Path oldFormat = root.resolve("files.meta");
    Files.write(oldFormat, new byte[]{4});

    IOException failure = assertThrows(IOException.class,
        () -> new StateArchiveAppendCheckpointMaterializerV5(root, hash(72), Engine.LEVELDB,
            100, hash(99), StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
            blockNumber -> null));

    assertEquals("Archive V5 non-fresh root lacks a committed Common publication",
        failure.getMessage());
    try (java.util.stream.Stream<Path> entries = Files.list(root)) {
      assertEquals(1L, entries.count());
    }
    assertEquals(1L, Files.size(oldFormat));
  }

  private static void assertForceOrder(List<String> events) {
    int[] lanes = StateArchiveGethFormatV5.laneIds();
    assertEquals(11, events.size());
    for (int index = 0; index < lanes.length; index++) {
      assertEquals(PublicationStage.DATA_FORCED + ":" + lanes[index], events.get(index));
      assertEquals(PublicationStage.LANE_INDEX_FORCED + ":" + lanes[index],
          events.get(index + lanes.length));
    }
    assertEquals(PublicationStage.TAIL_SYNCED + ":-1", events.get(10));
  }

  private static CommonCheckpointPayload payload(byte[] format,
      List<BlockReverseDiff> diffs, StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      PathStateFlushTarget.BlockBinding binding =
          org.mockito.Mockito.mock(PathStateFlushTarget.BlockBinding.class);
      org.mockito.Mockito.when(binding.getMeta()).thenReturn(meta);
      org.mockito.Mockito.when(binding.getParentStateRoot())
          .thenReturn(hash(30 + (int) meta.getBlockNumber()));
      org.mockito.Mockito.when(binding.getStateRoot())
          .thenReturn(hash(31 + (int) meta.getBlockNumber()));
      org.mockito.Mockito.when(binding.getTransitionPayloadDigest()).thenReturn(hash(90));
      bindings.add(binding);
    }
    PathStateFlushTarget path = org.mockito.Mockito.mock(PathStateFlushTarget.class);
    byte[] parentStateRoot = bindings.get(0).getParentStateRoot();
    byte[] stateRoot = bindings.get(bindings.size() - 1).getStateRoot();
    org.mockito.Mockito.when(path.getBlocks()).thenReturn(bindings);
    org.mockito.Mockito.when(path.getParentStateRoot())
        .thenReturn(parentStateRoot);
    org.mockito.Mockito.when(path.getStateRoot())
        .thenReturn(stateRoot);
    org.mockito.Mockito.when(path.getStores()).thenReturn(Collections.emptyList());
    org.mockito.Mockito.when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.createV2(format, path, descriptor, Collections.emptyList());
  }

  private static BlockReverseDiff diff(int blockNumber) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L);
    return new BlockReverseDiff(meta, Collections.singletonList(new DbGroup("code",
        Collections.singletonList(new Entry(new byte[]{1},
            OldValue.present(new byte[]{(byte) blockNumber}))))));
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }

}
