package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.config.args.Args;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.StateArchiveCheckpointMaterializer;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.stateroot.CommonCheckpointVersionStore;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/**
 * Covers the synchronous common-checkpoint write path with the version store: callers observe a
 * fully applied checkpoint on return, and a store that lost its unsynced tail is repaired from
 * the version store at startup.
 */
public class CommonCheckpointVersionReplayTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void configure() {
    Args.setParam(new String[]{}, TestConstants.TEST_CONF);
  }

  @AfterClass
  public static void clearConfiguration() {
    Args.clearParam();
  }

  @Test
  public void checkpointAppliesFullyAndDurablyBeforeReturning() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("sync-complete").toPath();
    Fixture fixture = new Fixture(root);
    CommonCheckpointRuntime runtime = fixture.runtime();
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          runtime.recoverBeforeServing());
      fixture.appendBlock(1, hash(0), hash(1), new byte[]{1});

      // No drain API exists anymore: the caller thread completes everything synchronously.
      CommonCheckpointTarget target = runtime.checkpointAndRebase(1);
      assertEquals(1, target.getLastBlock().getBlockNumber());
      assertEquals(CommonCheckpointRuntimeOwner.State.READY, runtime.getState());
      assertFalse(Files.exists(root.resolve("wal").resolve(CommonCheckpointFile.FILE_NAME)));
      assertSame(fixture.database.getHead().getRoot(), fixture.database.getHead());
      assertArrayEquals(new byte[]{1}, fixture.code.get(new byte[]{1}));
      assertEquals(0, fixture.code.syncedFlushes);
      assertEquals(1, fixture.code.unsyncedFlushes);
      assertEquals(1, fixture.versions.latestHead());
      assertArrayEquals(target.getPayloadDigest(), fixture.versions.latestDigest());

      // A second checkpoint does not need any catch-up: the rebase already ran.
      fixture.appendBlock(2, hash(1), hash(2), new byte[]{2});
      CommonCheckpointTarget second = runtime.checkpointAndRebase(1);
      assertEquals(2, second.getLastBlock().getBlockNumber());
      assertArrayEquals(new byte[]{2}, fixture.code.get(new byte[]{1}));
      assertEquals(2, fixture.versions.latestHead());
    } finally {
      runtime.close();
    }
  }

  @Test
  public void replayRepairsStoreWhoseUnsyncedTailWasLost() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("replay-repair").toPath();
    Fixture fixture = new Fixture(root);
    CommonCheckpointRuntime runtime = fixture.runtime();
    runtime.recoverBeforeServing();
    fixture.appendBlock(1, hash(0), hash(1), new byte[]{2});
    CommonCheckpointTarget first = runtime.checkpointAndRebase(1);
    fixture.appendBlock(2, hash(1), hash(2), new byte[]{3});
    runtime.checkpointAndRebase(1);
    runtime.close();

    // Simulate a power loss: version 2's unsynced write is gone, version 1's content survived.
    fixture.code.put(new byte[]{1}, new byte[]{2});
    assertArrayEquals(new byte[]{2}, fixture.code.get(new byte[]{1}));
    fixture.newStoreGeneration();

    CommonCheckpointRuntime recovered = fixture.runtime();
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          recovered.recoverBeforeServing());
      assertArrayEquals(new byte[]{3}, fixture.code.get(new byte[]{1}));
      assertEquals(Status.PUBLISHED, fixture.chainbase.inspect(
          CommonCheckpointTarget.from(fixture.payload(2, hash(1), hash(2), new byte[]{3}))));
      assertEquals(1, first.getLastBlock().getBlockNumber());
    } finally {
      recovered.close();
    }
    // A second restart with an intact Store verifies clean and replays nothing.
    CommonCheckpointRuntime intact = fixture.runtime();
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          intact.recoverBeforeServing());
      assertArrayEquals(new byte[]{3}, fixture.code.get(new byte[]{1}));
    } finally {
      intact.close();
    }
  }

  @Test
  public void legacyInStoreHeadKeyIsIgnoredAndLeftUntouched() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("legacy-key").toPath();
    Fixture fixture = new Fixture(root);
    // Pre-fix versions (a0889a0e55 and earlier) wrote this binary progress key into every
    // business Store. New code never reads it; operators may delete it at their own pace
    // (ARM-001 was cleaned by hand). The checkpoint and replay machinery must ignore it.
    byte[] legacyKey = ("\0" + "common-checkpoint-head-v1").getBytes(
        java.nio.charset.StandardCharsets.US_ASCII);
    fixture.code.put(legacyKey, new byte[8]);

    CommonCheckpointRuntime runtime = fixture.runtime();
    runtime.recoverBeforeServing();
    fixture.appendBlock(1, hash(0), hash(1), new byte[]{2});
    runtime.checkpointAndRebase(1);
    runtime.close();

    CommonCheckpointRuntime recovered = fixture.runtime();
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          recovered.recoverBeforeServing());
      assertArrayEquals(new byte[]{2}, fixture.code.get(new byte[]{1}));
      // The legacy key is untouched and never consulted.
      assertArrayEquals(new byte[8], fixture.code.get(legacyKey));
    } finally {
      recovered.close();
    }
  }

  @Test
  public void rollbackGapRebaselinesVersionStoreInsteadOfFailingClosed() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("rollback-gap").toPath();
    Fixture fixture = new Fixture(root);
    CommonCheckpointRuntime runtime = fixture.runtime();
    runtime.recoverBeforeServing();
    fixture.appendBlock(1, hash(0), hash(1), new byte[]{1});
    runtime.checkpointAndRebase(1);
    fixture.appendBlock(2, hash(1), hash(2), new byte[]{2});
    runtime.checkpointAndRebase(1);
    assertEquals(2, fixture.versions.latestHead());
    runtime.close();

    // Old code (no version store) runs for a while: checkpoints 3..5 complete with synced
    // business-store writes and advance every authority's published marker, but record nothing
    // in the version store.
    StateArchiveCheckpointMaterializer archive =
        new StateArchiveCheckpointMaterializer(root.resolve("archive"), fixture.format, null,
            Engine.LEVELDB);
    for (int number = 3; number <= 5; number++) {
      CommonCheckpointPayload payload = fixture.payload(number, hash(number - 1), hash(number),
          new byte[]{(byte) number});
      CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
      fixture.chainbase.materialize(payload, target);
      fixture.chainbase.publish(target);
      archive.materialize(payload, target);
      archive.publish(target);
      fixture.pathState.publish(target);
    }
    archive.close();

    // New code starts again: the version store lags the published target, so it re-baselines
    // instead of failing closed — no replay is needed (old-code writes were synced).
    CommonCheckpointRuntime recovered = fixture.runtime();
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          recovered.recoverBeforeServing());
      assertEquals(CommonCheckpointRuntimeOwner.State.READY, recovered.getState());
      assertEquals(5, fixture.versions.latestHead());
      assertEquals(5, fixture.versions.firstHead());
      assertTrue(fixture.versions.versions().isEmpty());

      // Checkpoints, replay and pruning keep working on top of the new baseline.
      fixture.appendBlock(6, hash(5), hash(6), new byte[]{6});
      CommonCheckpointTarget sixth = recovered.checkpointAndRebase(1);
      assertEquals(6, sixth.getLastBlock().getBlockNumber());
      assertArrayEquals(new byte[]{6}, fixture.code.get(new byte[]{1}));
      assertEquals(6, fixture.versions.latestHead());
      assertEquals(java.util.Arrays.asList(6L), fixture.versions.versions());
      assertEquals(5, fixture.versions.firstHead());
      assertEquals(6L, (long) fixture.versions.progressHead(6, "c:code"));
      assertFalse(fixture.versions.needsPrune());
    } finally {
      recovered.close();
    }
  }

  @Test
  public void replayFailsClosedWhenVersionStoreDisagreesWithPublishedTarget()
      throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path root = temporaryFolder.newFolder("replay-fail-closed").toPath();
    Fixture fixture = new Fixture(root, 2);
    CommonCheckpointRuntime runtime = fixture.runtime();
    runtime.recoverBeforeServing();
    for (int number = 1; number <= 2; number++) {
      fixture.appendBlock(number, hash(number - 1), hash(number), new byte[]{(byte) number});
      runtime.checkpointAndRebase(1);
    }
    runtime.close();

    // A foreign version lands in the version store: its latest no longer matches the published
    // authority target, so startup must fail closed instead of replaying an inconsistent window.
    fixture.tamperVersionStore(fixture.payload(9, hash(8), hash(9), new byte[]{9}));

    CommonCheckpointRuntime recovered = fixture.runtime();
    try {
      assertThrows(IOException.class, recovered::recoverBeforeServing);
      assertEquals(CommonCheckpointRuntimeOwner.State.FAILED, recovered.getState());
    } finally {
      recovered.close();
    }
  }

  private static byte[] hash(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static final class Fixture {

    private final Path root;
    private final long retainedBlocks;
    private final byte[] format = hash(80);
    private final MemoryDb code = new MemoryDb("code");
    private Chainbase database;
    private ChainbaseCheckpointMaterializer chainbase;
    private final CommonCheckpointMaterializer pathState = fake(Authority.PATH_STATE);
    private CommonCheckpointVersionStore versions;

    private Fixture(Path root) throws IOException {
      this(root, CommonCheckpointVersionStore.DEFAULT_RETAINED_BLOCKS);
    }

    private Fixture(Path root, long retainedBlocks) throws IOException {
      this.root = root;
      this.retainedBlocks = retainedBlocks;
      newStoreGeneration();
    }

    /**
     * Simulates a process restart: a fresh SnapshotRoot over the same durable data has an empty
     * read cache, so post-crash verification reads the database rather than stale cache entries.
     */
    private void newStoreGeneration() {
      database = new Chainbase(new SnapshotRoot(code));
      chainbase = new ChainbaseCheckpointMaterializer(root.resolve("chainbase"), format,
          Collections.singletonList(database));
    }

    /** Publishes one foreign version into the closed runtime's version store, then closes it. */
    private void tamperVersionStore(CommonCheckpointPayload foreign) throws IOException {
      CommonCheckpointVersionStore handle = CommonCheckpointVersionStore.open(
          root.resolve("versions"), Engine.LEVELDB, retainedBlocks);
      try {
        handle.publish(foreign);
      } finally {
        handle.close();
      }
    }

    private SnapshotImpl appendBlock(long number, byte[] parentHash, byte[] blockHash,
        byte[] value) {
      BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(number, blockHash, parentHash,
          number * 3_000L);
      PathStateSnapshotDelta path = mock(PathStateSnapshotDelta.class);
      when(path.getMeta()).thenReturn(meta);
      when(path.getParentStateRoot()).thenReturn(hash(10 + (int) number - 1));
      when(path.getStateRoot()).thenReturn(hash(10 + (int) number));
      when(path.getTransitionPayloadDigest()).thenReturn(hash(50 + (int) number));
      when(path.getStores()).thenReturn(Collections.emptyList());
      when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
      BlockReverseDiff archive = new BlockReverseDiff(meta, Collections.emptyList());
      SnapshotImpl layer = (SnapshotImpl) database.getHead().advance();
      layer.attachBlockArtifacts(meta, archive, path);
      database.setHead(layer);
      layer.put(new byte[]{1}, value);
      return layer;
    }

    private CommonCheckpointPayload payload(long number, byte[] parentHash, byte[] blockHash,
        byte[] value) {
      BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(number, blockHash, parentHash,
          number * 3_000L);
      PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
      when(binding.getMeta()).thenReturn(meta);
      when(binding.getParentStateRoot()).thenReturn(hash(10 + (int) number - 1));
      when(binding.getStateRoot()).thenReturn(hash(10 + (int) number));
      when(binding.getTransitionPayloadDigest()).thenReturn(hash(50 + (int) number));
      PathStateFlushTarget pathState = mock(PathStateFlushTarget.class);
      when(pathState.getBlocks()).thenReturn(Collections.singletonList(binding));
      when(pathState.getParentStateRoot()).thenReturn(hash(10 + (int) number - 1));
      when(pathState.getStateRoot()).thenReturn(hash(10 + (int) number));
      when(pathState.getStores()).thenReturn(Collections.emptyList());
      when(pathState.getSuperNodeMutations()).thenReturn(Collections.emptyList());
      return CommonCheckpointPayload.create(format, pathState,
          Collections.singletonList(new BlockReverseDiff(meta, Collections.emptyList())),
          Collections.singletonList(new CommonCheckpointPayload.StoreMutations("code",
              Collections.singletonList(
                  new CommonCheckpointPayload.Mutation(new byte[]{1}, value)))));
    }

    private CommonCheckpointRuntime runtime() throws IOException {
      // The coordinator owns and closes the store, so every runtime gets a fresh handle.
      versions = CommonCheckpointVersionStore.open(root.resolve("versions"), Engine.LEVELDB,
          retainedBlocks);
      CommonCheckpointRedoCoordinator coordinator = new CommonCheckpointRedoCoordinator(
          new CommonCheckpointFile(root.resolve("wal")), chainbase, pathState,
          new StateArchiveCheckpointMaterializer(root.resolve("archive"), format, null,
              Engine.LEVELDB), versions);
      return new CommonCheckpointRuntime(new CommonCheckpointRuntimeOwner(coordinator),
          Collections.singletonList(database), root.resolve("archive"), format, Engine.LEVELDB,
          (blockNumber, blockHash) -> {
            throw new IOException("latest state is intentionally unavailable");
          }, target -> () -> { });
    }

    private static CommonCheckpointMaterializer fake(Authority authority) {
      return new CommonCheckpointMaterializer() {
        private CommonCheckpointTarget materialized;
        private CommonCheckpointTarget published;

        @Override
        public Authority authority() {
          return authority;
        }

        @Override
        public Status inspect(CommonCheckpointTarget target) {
          if (target.equals(published)) {
            return Status.PUBLISHED;
          }
          return target.equals(materialized) ? Status.MATERIALIZED
              : Status.NEEDS_MATERIALIZATION;
        }

        @Override
        public void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target) {
          materialized = target;
        }

        @Override
        public void publish(CommonCheckpointTarget target) {
          published = target;
        }
      };
    }
  }

  private static final class MemoryDb implements DB<byte[], byte[]>, Flusher {

    private final String name;
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();
    private int syncedFlushes;
    private int unsyncedFlushes;

    private MemoryDb(String name) {
      this.name = name;
    }

    @Override
    public byte[] get(byte[] key) {
      return values.get(WrappedByteArray.of(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
      values.put(WrappedByteArray.of(key), value);
    }

    @Override
    public long size() {
      return values.size();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public void remove(byte[] key) {
      values.remove(WrappedByteArray.of(key));
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      Map<byte[], byte[]> copy = new LinkedHashMap<>();
      values.forEach((key, value) -> copy.put(key.getBytes(), value));
      return copy.entrySet().iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public String getDbName() {
      return name;
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], byte[]> newInstance() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
      unsyncedFlushes++;
      apply(batch);
    }

    @Override
    public void flushSynced(Map<WrappedByteArray, WrappedByteArray> batch) {
      syncedFlushes++;
      apply(batch);
    }

    @Override
    public void reset() {
      values.clear();
    }

    private void apply(Map<WrappedByteArray, WrappedByteArray> batch) {
      batch.forEach((key, value) -> {
        if (value.getBytes() == null) {
          values.remove(key);
        } else {
          values.put(key, value.getBytes());
        }
      });
    }
  }
}
