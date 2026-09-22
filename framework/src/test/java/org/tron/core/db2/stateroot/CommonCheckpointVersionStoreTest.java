package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Covers the common-checkpoint version store: single-sync writes, replay, and pruning. */
public class CommonCheckpointVersionStoreTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void publishRecordsShardsIndexAndLatestWithExactlyOneSync() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    PathStateNativeNodeStore nativeStore = PathStateNativeNodeStore.open(
        temporaryFolder.newFolder("versions").toPath(), Engine.LEVELDB);
    CommonCheckpointVersionStore versions = CommonCheckpointVersionStore.wrap(nativeStore,
        CommonCheckpointVersionStore.DEFAULT_RETAINED_BLOCKS);
    try {
      CommonCheckpointPayload payload = payload(10, hash(9), hash(10), hash(19), hash(20),
          new byte[]{7}, new byte[]{8});
      CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
      versions.publish(payload);

      assertEquals(10, versions.latestHead());
      assertArrayEquals(target.getPayloadDigest(), versions.latestDigest());
      assertEquals(10, versions.firstHead());
      assertEquals(Arrays.asList(10L), versions.versions());
      // The shards and index are unsynced; latest is the only synced write of a checkpoint.
      assertEquals(1, nativeStore.getSyncedWriteBatchCalls());
      assertTrue(nativeStore.getUnsyncedWriteBatchCalls() >= 1);

      List<CommonCheckpointPayload.Mutation> chainbase =
          CommonCheckpointVersionStore.decodeMutations(
              versions.chainbaseShard(10, "code"));
      assertEquals(1, chainbase.size());
      assertArrayEquals(new byte[]{1}, chainbase.get(0).getKey());
      assertArrayEquals(new byte[]{7}, chainbase.get(0).getValue());

      CommonCheckpointVersionStore.PathShard account =
          CommonCheckpointVersionStore.decodePathShard(versions.pathStoreShard(10, 4));
      assertEquals(1, account.getFlatMutations().size());
      assertArrayEquals(new byte[]{1}, account.getFlatMutations().get(0).getKey());
      assertArrayEquals(new byte[]{7}, account.getFlatMutations().get(0).getValue());
      assertEquals(1, account.getNodeMutations().size());
      assertArrayEquals(new byte[]{8}, account.getNodeMutations().get(0).getValue());
      CommonCheckpointVersionStore.PathShard superShard =
          CommonCheckpointVersionStore.decodePathShard(versions.pathStoreShard(10, 0));
      assertEquals(1, superShard.getNodeMutations().size());
      assertArrayEquals(new byte[]{5}, superShard.getNodeMutations().get(0).getKey());

      byte[] marker = versions.versionMarker(10);
      assertEquals(64, marker.length);
      assertArrayEquals(target.getPayloadDigest(), Arrays.copyOf(marker, 32));
      assertArrayEquals(target.getStateRoot(), Arrays.copyOfRange(marker, 32, 64));

      // Every anchored store appears in the h: progress journal of that version.
      assertEquals(10L, (long) versions.progressHead(10, "c:code"));
      assertEquals(10L, (long) versions.progressHead(10, "p:4"));
      assertEquals(10L, (long) versions.progressHead(10, "p:0"));
      assertNull(versions.progressHead(10, "c:storage-row"));

      // Re-publish is an idempotent same-key retry.
      versions.publish(payload);
      assertEquals(10, versions.latestHead());
      assertEquals(Arrays.asList(10L), versions.versions());
    } finally {
      versions.close();
    }
  }

  @Test
  public void pruneRetainsBoundaryAnchorAndRecentWindow() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    PathStateNativeNodeStore nativeStore = PathStateNativeNodeStore.open(
        temporaryFolder.newFolder("prune").toPath(), Engine.LEVELDB);
    CommonCheckpointVersionStore versions = CommonCheckpointVersionStore.wrap(nativeStore, 15);
    try {
      for (long head = 10; head <= 50; head += 10) {
        versions.publish(payload(head, hash((int) head - 1), hash((int) head),
            hash(10), hash(11), new byte[]{1}, new byte[]{(byte) head}));
      }
      assertEquals(50, versions.latestHead());
      assertTrue(versions.needsPrune());

      long boundary = versions.pruneIfNeeded();
      assertEquals(30, boundary);
      assertEquals(30, versions.firstHead());
      assertEquals(Arrays.asList(30L, 40L, 50L), versions.versions());
      assertNull(versions.chainbaseShard(10, "code"));
      assertNull(versions.chainbaseShard(20, "code"));
      assertTrue(versions.chainbaseShard(30, "code") != null);
      // The h: progress journal is pruned together with its version.
      assertNull(versions.progressHead(10, "c:code"));
      assertNull(versions.progressHead(20, "c:code"));
      assertEquals(30L, (long) versions.progressHead(30, "c:code"));
      assertEquals(-1, versions.pruneIfNeeded());
      // The retained boundary anchor can span slightly more than retainedBlocks.
      assertTrue(versions.latestHead() - versions.firstHead() >= 0);

      // A store sitting exactly on the boundary replays every newer version.
      assertEquals(Arrays.asList(40L, 50L), versions.versionsBetween(30, 50));
      // A store older than the boundary fails closed: its needed versions were pruned.
      assertThrows(IOException.class, () -> versions.versionsBetween(20, 50));
      assertThrows(IOException.class, () -> versions.versionsBetween(0, 50));
    } finally {
      versions.close();
    }
  }

  @Test
  public void pathStateReplayRestoresLostUnsyncedTail() throws Exception {
    Path root = temporaryFolder.newFolder("path-replay").toPath();
    PathStateParticipantScope scope = new PathStateCanonicalizer().participantScope();
    byte[] formatIdentity = hash(7);
    CommonCheckpointPayload firstPayload = payload(1, hash(0), hash(1), hash(10), hash(11),
        new byte[]{2}, new byte[]{4});
    CommonCheckpointPayload secondPayload = payload(2, hash(1), hash(2), hash(11), hash(12),
        new byte[]{7}, new byte[]{8});
    CommonCheckpointVersionStore versions = CommonCheckpointVersionStore.open(
        root.resolve("versions"), Engine.ROCKSDB);
    PathStatePhysicalStoreSet stores = PathStatePhysicalStoreSet.open(root.resolve("path-state"),
        scope, Engine.ROCKSDB);
    try {
      PathStateCheckpointMaterializer materializer = new PathStateCheckpointMaterializer(stores,
          scope, formatIdentity);
      versions.publish(firstPayload);
      materializer.materialize(firstPayload, CommonCheckpointTarget.from(firstPayload));
      versions.publish(secondPayload);
      materializer.materialize(secondPayload, CommonCheckpointTarget.from(secondPayload));
      assertArrayEquals(new byte[]{7}, stores.participant("account").getFlat(new byte[]{1}));
      assertEquals(2L, (long) stores.participant("account").checkpointHead());
      assertEquals(2L, (long) stores.superStore().checkpointHead());
    } finally {
      stores.close();
    }

    // Simulate a power loss: the second version's unsynced tail is gone from the store.
    PathStatePhysicalStoreSet damaged = PathStatePhysicalStoreSet.openExisting(
        root.resolve("path-state"), scope, Engine.ROCKSDB);
    try {
      PathStatePhysicalStoreSet.PhysicalStore account = damaged.participant("account");
      // v1's writes survive; v2's unsynced tail is lost entirely.
      account.putFlat(new byte[]{1}, new byte[]{2});
      account.putMetadata(PathStatePhysicalStoreSet.CHECKPOINT_HEAD_METADATA,
          ByteBuffer.allocate(Long.BYTES).putLong(1).array());
      assertArrayEquals(new byte[]{2}, account.getFlat(new byte[]{1}));
      assertEquals(1L, (long) account.checkpointHead());

      new PathStateCheckpointMaterializer(damaged, scope, formatIdentity)
          .replayFromVersionStore(versions, versions.latestHead());
      assertArrayEquals(new byte[]{7}, account.getFlat(new byte[]{1}));
      assertEquals(2L, (long) account.checkpointHead());
      assertEquals(2L, (long) damaged.superStore().checkpointHead());
      assertArrayEquals(versions.versionMarker(2), account.checkpointTargetMarker());
    } finally {
      damaged.close();
      versions.close();
    }
  }

  @Test
  public void replayFromEmptyVersionStoreIsANoOpAndLatestStaysAbsent() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    CommonCheckpointVersionStore versions = CommonCheckpointVersionStore.open(
        temporaryFolder.newFolder("empty").toPath(), Engine.LEVELDB);
    try {
      assertEquals(-1, versions.latestHead());
      assertEquals(-1, versions.firstHead());
      assertFalse(versions.needsPrune());
      assertEquals(-1, versions.pruneIfNeeded());
    } finally {
      versions.close();
    }
  }

  static CommonCheckpointPayload payload(long blockNumber, byte[] parentHash, byte[] blockHash,
      byte[] parentRoot, byte[] stateRoot, byte[] flatValue, byte[] nodeValue) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, blockHash, parentHash,
        blockNumber * 3_000L);
    PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
    when(binding.getMeta()).thenReturn(meta);
    when(binding.getParentStateRoot()).thenReturn(parentRoot);
    when(binding.getStateRoot()).thenReturn(stateRoot);
    when(binding.getTransitionPayloadDigest()).thenReturn(hash((int) blockNumber + 30));

    PathStateFlushTarget.StoreTarget store = mock(PathStateFlushTarget.StoreTarget.class);
    when(store.getStoreId()).thenReturn(4);
    when(store.getDbName()).thenReturn("account");
    when(store.getStoreRoot()).thenReturn(hash((int) blockNumber + 40));
    when(store.getFlatMutations()).thenReturn(Collections.singletonList(
        new PathStateSnapshotDelta.Mutation(new byte[]{1}, flatValue)));
    when(store.getNodeMutations()).thenReturn(Collections.singletonList(
        new PathStateSnapshotDelta.Mutation(new byte[]{3}, nodeValue)));

    PathStateFlushTarget target = mock(PathStateFlushTarget.class);
    when(target.getBlocks()).thenReturn(Collections.singletonList(binding));
    when(target.getParentStateRoot()).thenReturn(parentRoot);
    when(target.getStateRoot()).thenReturn(stateRoot);
    when(target.getStores()).thenReturn(Collections.singletonList(store));
    when(target.getSuperNodeMutations()).thenReturn(Collections.singletonList(
        new PathStateSnapshotDelta.Mutation(new byte[]{5}, new byte[]{6})));
    List<CommonCheckpointPayload.StoreMutations> chainbase = Collections.singletonList(
        new CommonCheckpointPayload.StoreMutations("code", Collections.singletonList(
            new CommonCheckpointPayload.Mutation(new byte[]{1}, flatValue))));
    return CommonCheckpointPayload.create(hash(7), target,
        Collections.singletonList(new BlockReverseDiff(meta, Collections.emptyList())),
        chainbase);
  }

  static byte[] hash(int seed) {
    byte[] hash = new byte[32];
    for (int index = 0; index < hash.length; index++) {
      hash[index] = (byte) (seed + index);
    }
    return hash;
  }
}
