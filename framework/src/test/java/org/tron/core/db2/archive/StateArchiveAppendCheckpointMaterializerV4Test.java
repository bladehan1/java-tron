package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveAppendCheckpointMaterializerV4Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test(timeout = 15000)
  public void preparesPublishesServesAndReopensCataloglessCheckpoint() throws Exception {
    Path root = temporaryFolder.newFolder("append-v4").toPath()
        .resolve("history").resolve("v4");
    byte[] format = hash(70);
    byte[] baseline = hash(80);
    List<BlockReverseDiff> diffs = Arrays.asList(diff(1, 8), diff(2, 0));
    CommonCheckpointTarget target;

    try (StateArchiveAppendCheckpointMaterializerV4 archive = materializer(
        root, format, baseline)) {
      StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
      CommonCheckpointPayload payload = payload(format, diffs, descriptor);
      target = archive.prepare(CommonCheckpointCapture.create(payload, diffs, descriptor));
      assertEquals(Status.MATERIALIZED, archive.inspect(target));
      archive.publish(target);
      assertEquals(Status.PUBLISHED, archive.inspect(target));
      assertEquals(target, archive.loadPublishedTargetIfPresent().get());

      archive.afterCommit(target);
      archive.completeServingInitialSync(target);
      try (CheckpointPointHistory history = archive.pinHistory(target)) {
        OldValue value = history.findOldValueAfter("code", new byte[]{1}, 0).get();
        assertTrue(value.isPresent());
        assertArrayEquals(new byte[8], value.getValue());
      }
    }

    try (StateArchiveAppendCheckpointMaterializerV4 reopened = materializer(
        root, format, baseline)) {
      assertEquals(target, reopened.loadPublishedTargetIfPresent().get());
      assertEquals(Status.PUBLISHED, reopened.inspect(target));
    }
  }

  @Test(timeout = 30000)
  public void pointReadsOnlyTheStoreLaneAndFailsClosedOnTargetCorruption() throws Exception {
    Path root = temporaryFolder.newFolder("append-v4-point-lanes").toPath()
        .resolve("history").resolve("v4");
    byte[] format = hash(71);
    byte[] baseline = hash(81);
    BlockReverseDiff diff = allLaneDiff(1);
    CommonCheckpointTarget target;

    try (StateArchiveAppendCheckpointMaterializerV4 archive = materializer(
        root, format, baseline)) {
      StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(
          Collections.singletonList(diff));
      CommonCheckpointPayload payload = payload(format, Collections.singletonList(diff),
          descriptor);
      target = archive.prepare(CommonCheckpointCapture.create(payload,
          Collections.singletonList(diff), descriptor));
      archive.publish(target);
      archive.afterCommit(target);
      archive.completeServingInitialSync(target);

      int[] laneIds = {0, 4, 5, 13, 22};
      String[] stores = {"code", "account", "account-asset", "delegation", "storage-row"};
      byte[][] keys = {{1}, fixedKey(21, 4), {5}, {13}, fixedKey(32, 22)};
      OldValue[] values = {OldValue.absent(), OldValue.present(new byte[0]),
          OldValue.present(new byte[]{5}), OldValue.present(new byte[]{13}),
          OldValue.present(new byte[]{22})};
      for (int index = 0; index < laneIds.length; index++) {
        List<Path[]> hidden = hideOtherLanes(root, laneIds[index]);
        try (CheckpointPointHistory history = archive.pinHistory(target)) {
          assertEquals(values[index], history.findOldValueAfter(stores[index], keys[index], 0)
              .get());
        } finally {
          restore(hidden);
        }
      }
      try (CheckpointPointHistory history = archive.pinHistory(target)) {
        assertFalse(history.findOldValueAfter("code", new byte[]{9}, 0).isPresent());
      }

      Path targetIndex = StateArchiveSegmentNamesV4.current(root.resolve("segments"),
          0, 0, 1).getIndex();
      try (FileChannel channel = FileChannel.open(targetIndex, StandardOpenOption.WRITE)) {
        ByteBuffer wrongBlock = ByteBuffer.allocate(Long.BYTES).putLong(2);
        wrongBlock.flip();
        channel.write(wrongBlock, StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH);
      }
      try (CheckpointPointHistory history = archive.pinHistory(target)) {
        assertThrows(java.io.IOException.class,
            () -> history.findOldValueAfter("code", new byte[]{1}, 0));
      }
    }
  }

  private static StateArchiveAppendCheckpointMaterializerV4 materializer(Path root,
      byte[] format, byte[] baseline) throws Exception {
    return new StateArchiveAppendCheckpointMaterializerV4(root, format, Engine.LEVELDB,
        baseline, StateArchiveFileFormatV3.COMPRESSION_NONE, 1_500);
  }

  private static CommonCheckpointPayload payload(byte[] format,
      List<BlockReverseDiff> diffs, StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
      when(binding.getMeta()).thenReturn(meta);
      when(binding.getParentStateRoot()).thenReturn(hash(30 + (int) meta.getBlockNumber()));
      when(binding.getStateRoot()).thenReturn(hash(31 + (int) meta.getBlockNumber()));
      when(binding.getTransitionPayloadDigest()).thenReturn(hash(90));
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
    return CommonCheckpointPayload.createV2(format, path, descriptor, Collections.emptyList());
  }

  private static BlockReverseDiff diff(int block, int valueLength) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3_000L);
    if (valueLength == 0) {
      return new BlockReverseDiff(meta, Collections.emptyList());
    }
    return new BlockReverseDiff(meta, Collections.singletonList(new DbGroup("code",
        Collections.singletonList(new Entry(new byte[]{1},
            OldValue.present(new byte[valueLength]))))));
  }

  private static BlockReverseDiff allLaneDiff(int block) {
    return new BlockReverseDiff(
        BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block * 3_000L),
        Arrays.asList(
            new DbGroup("code", Collections.singletonList(
                new Entry(new byte[]{1}, OldValue.absent()))),
            new DbGroup("account", Collections.singletonList(
                new Entry(fixedKey(21, 4), OldValue.present(new byte[0])))),
            new DbGroup("account-asset", Collections.singletonList(
                new Entry(new byte[]{5}, OldValue.present(new byte[]{5})))),
            new DbGroup("delegation", Collections.singletonList(
                new Entry(new byte[]{13}, OldValue.present(new byte[]{13})))),
            new DbGroup("storage-row", Collections.singletonList(
                new Entry(fixedKey(32, 22), OldValue.present(new byte[]{22}))))));
  }

  private static List<Path[]> hideOtherLanes(Path root, int retainedLane) throws Exception {
    List<Path[]> hidden = new ArrayList<>();
    for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
      if (laneId == retainedLane) {
        continue;
      }
      StateArchiveSegmentNamesV4.SegmentPair pair = StateArchiveSegmentNamesV4.current(
          root.resolve("segments"), 0, laneId, 1);
      hide(pair.getData(), hidden);
      hide(pair.getIndex(), hidden);
    }
    return hidden;
  }

  private static void hide(Path source, List<Path[]> hidden) throws Exception {
    Path target = source.resolveSibling(source.getFileName() + ".hidden");
    Files.move(source, target);
    hidden.add(new Path[]{source, target});
  }

  private static void restore(List<Path[]> hidden) throws Exception {
    for (int index = hidden.size() - 1; index >= 0; index--) {
      Path[] pair = hidden.get(index);
      Files.move(pair[1], pair[0]);
    }
  }

  private static byte[] fixedKey(int length, int tail) {
    byte[] key = new byte[length];
    key[length - 1] = (byte) tail;
    return key;
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
