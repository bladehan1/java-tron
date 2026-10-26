package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.tron.core.db2.NativeCheckpointTestSupport.database;
import static org.tron.core.db2.NativeCheckpointTestSupport.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.config.args.Args;
import org.tron.core.db2.archive.checkpoint.storage.SharedCheckpointDatabase;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.checkpoint.common.CommonCheckpointFile;
import org.tron.core.db2.checkpoint.common.CommonCheckpointMaterializer;
import org.tron.core.db2.checkpoint.common.CommonCheckpointPayload;
import org.tron.core.db2.checkpoint.common.CommonCheckpointPayloadCodec;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRedoCoordinator;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRedoCoordinator.RecoveryAction;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.ChainbaseCheckpointMaterializer;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.db2.stateroot.PathStateFlushTarget;

public class SharedCheckpointDatabaseTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Args.setParam(new String[]{"--output-directory", "output_shared_checkpoint_test"},
        "config-test.conf");
  }

  @Test
  public void retainedScanRejectsCorruptBodyAndGenerationIdentity() throws Exception {
    for (boolean corruptBody : new boolean[]{true, false}) {
      Path root = temporary.newFolder().toPath();
      try (SharedCheckpointDatabase database = new SharedCheckpointDatabase(root, engine())) {
        database.publish(payload(1));
        database.applied();
      }
      byte[] prefix = "generation/v1/".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
      byte[] key = java.nio.ByteBuffer.allocate(prefix.length + Long.BYTES)
          .put(prefix).putLong(1).array();
      byte[] encoded = new CommonCheckpointPayloadCodec().encode(payload(corruptBody ? 1 : 2));
      if (corruptBody) {
        encoded[encoded.length - 1] ^= 1;
      }
      try (ArchiveIndexStorage.Writer writer =
          ArchiveIndexStorage.openWriter(root, engine())) {
        writer.write(Collections.singletonList(ArchiveIndexStorage.put(key, encoded)), true);
      }
      // The optimization must retain both checksum validation and key-to-block binding.
      assertThrows(IOException.class, () -> new SharedCheckpointDatabase(root, engine()));
    }
  }

  @Test
  public void retainsGenerationsAndRestoresLossEvenAfterPublication() throws Exception {
    Path common = temporary.newFolder().toPath();
    Chainbase properties = new Chainbase(new SnapshotRoot(
        database(temporary.newFolder().toPath(), "properties")));
    CommonCheckpointFile file = CommonCheckpointFile.openDatabase(common, engine());
    Fake path = new Fake(CommonCheckpointMaterializer.Authority.PATH_STATE);
    Fake history = new Fake(CommonCheckpointMaterializer.Authority.STATE_ARCHIVE);
    ChainbaseCheckpointMaterializer chainbase = new ChainbaseCheckpointMaterializer(common,
        hash(70), Collections.singletonList(properties));
    try (CommonCheckpointRedoCoordinator coordinator =
        new CommonCheckpointRedoCoordinator(file, chainbase, path, history)) {
      coordinator.apply(payload(1));
      coordinator.apply(payload(2));
      assertFalse(file.isPresent());
      assertEquals(2, file.retained().size());
      // Simulate losing unsynced target rows while publication files remain durable.
      properties.getHead().getRoot().remove(new byte[]{1});
      properties.getHead().getRoot().remove(new byte[]{2});
      coordinator.recover();
      assertArrayEquals(new byte[]{11}, properties.getUnchecked(new byte[]{1}));
      assertArrayEquals(new byte[]{12}, properties.getUnchecked(new byte[]{2}));
      path.failSync = true;
      assertThrows(IOException.class, coordinator::checkpointDurableBase);
      assertEquals("failed target force cannot retire required redo", 2, file.retained().size());
      path.failSync = false;
      coordinator.checkpointDurableBase();
      assertEquals(1, file.retained().size());
      assertEquals(2, CommonCheckpointTarget.from(file.retained().get(0))
          .getLastBlock().getBlockNumber());
    } finally {
      properties.close();
    }
    try (SharedCheckpointDatabase reopened =
        new SharedCheckpointDatabase(common.resolve("redo"), engine())) {
      assertEquals(1, reopened.retained().size());
      assertFalse(reopened.isPending());
    }
  }

  @Test
  public void configuredCadenceBoundsRetainedGenerations() throws Exception {
    Path common = temporary.newFolder().toPath();
    Fake chain = new Fake(CommonCheckpointMaterializer.Authority.CHAINBASE);
    Fake path = new Fake(CommonCheckpointMaterializer.Authority.PATH_STATE);
    Fake history = new Fake(CommonCheckpointMaterializer.Authority.STATE_ARCHIVE);
    try (CommonCheckpointFile file = CommonCheckpointFile.openDatabase(common, engine());
        CommonCheckpointRedoCoordinator coordinator =
            new CommonCheckpointRedoCoordinator(file, chain, path, history)) {
      coordinator.configureDurableBaseCadence(2);
      coordinator.apply(payload(1));
      coordinator.apply(payload(2));
      // The cadence trigger submits an asynchronous durable-base advance; wait for it before
      // asserting the pruned generation count.
      coordinator.awaitDurableBaseIdle();
      assertEquals(1, file.retained().size());
      coordinator.apply(payload(3));
      assertEquals(2, file.retained().size());
      coordinator.apply(payload(4));
      coordinator.awaitDurableBaseIdle();
      assertEquals(1, file.retained().size());
      assertEquals(4, CommonCheckpointTarget.from(file.latestCommitted())
          .getLastBlock().getBlockNumber());
    }
  }

  @Test
  public void recoverAwaitsInFlightAsynchronousDurableBase() throws Exception {
    Path common = temporary.newFolder().toPath();
    Fake chain = new Fake(CommonCheckpointMaterializer.Authority.CHAINBASE);
    Fake path = new Fake(CommonCheckpointMaterializer.Authority.PATH_STATE);
    Fake history = new Fake(CommonCheckpointMaterializer.Authority.STATE_ARCHIVE);
    path.syncReplayStarted = new CountDownLatch(1);
    path.syncReplayRelease = new CountDownLatch(1);
    try (CommonCheckpointFile file = CommonCheckpointFile.openDatabase(common, engine());
        CommonCheckpointRedoCoordinator coordinator =
            new CommonCheckpointRedoCoordinator(file, chain, path, history)) {
      coordinator.configureDurableBaseCadence(1);
      coordinator.apply(payload(1));
      // The asynchronous durable-base advance is now blocked inside the sync retained replay.
      assertTrue(path.syncReplayStarted.await(5, TimeUnit.SECONDS));
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<RecoveryAction> recovery = executor.submit(coordinator::recover);
      try {
        // recover() must wait for the in-flight advance instead of overlapping it.
        assertThrows(TimeoutException.class,
            () -> recovery.get(200, TimeUnit.MILLISECONDS));
      } finally {
        path.syncReplayRelease.countDown();
      }
      try {
        assertEquals(RecoveryAction.COMPLETED_REDO, recovery.get(5, TimeUnit.SECONDS));
      } finally {
        executor.shutdownNow();
      }
      coordinator.awaitDurableBaseIdle();
      assertEquals(1, file.retained().size());
      assertEquals(1, CommonCheckpointTarget.from(file.latestCommitted())
          .getLastBlock().getBlockNumber());
    }
  }

  @Test
  public void rejectsConflictsGapsAndPruneBeforeMaterialization() throws Exception {
    Path root = temporary.newFolder().toPath();
    try (SharedCheckpointDatabase database = new SharedCheckpointDatabase(root, engine())) {
      database.publish(payload(1));
      assertThrows(IOException.class, database::advanceDurableBase);
      database.applied();
      assertThrows(IOException.class, () -> database.publish(payload(3)));
      database.publish(payload(2));
      assertThrows(IOException.class, () -> database.publish(payload(1)));
      assertEquals(2, database.retained().size());
    }
  }

  @Test
  public void rejectsMissingGenerationBeforeOpeningWrites() throws Exception {
    Path root = temporary.newFolder().toPath();
    try (SharedCheckpointDatabase database = new SharedCheckpointDatabase(root, engine())) {
      database.publish(payload(1));
      database.applied();
      database.publish(payload(2));
      database.applied();
    }
    byte[] prefix = "generation/v1/".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    byte[] key = java.nio.ByteBuffer.allocate(prefix.length + Long.BYTES)
        .put(prefix).putLong(1).array();
    try (ArchiveIndexStorage.Writer writer =
        ArchiveIndexStorage.openWriter(root, engine())) {
      writer.write(Collections.singletonList(ArchiveIndexStorage.delete(key)), true);
    }
    // The first generation anchor must survive: deletion cannot silently reset the base.
    assertThrows(IOException.class, () -> new SharedCheckpointDatabase(root, engine()));
  }

  @Test
  public void reopensPendingCommitAndKeepsBothRedoBodies() throws Exception {
    Path common = temporary.newFolder().toPath();
    try (CommonCheckpointFile file = CommonCheckpointFile.openDatabase(common, engine())) {
      file.publish(payload(1));
      assertNotNull(file.loadIfPresent());
    }
    try (CommonCheckpointFile file = CommonCheckpointFile.openDatabase(common, engine())) {
      assertEquals(1, file.retained().size());
      assertEquals("properties", file.loadRequired().getChainbaseStores().get(0).getDbName());
      assertEquals(1, file.loadRequired().getPathStores().size());
      assertThrows(IOException.class, file::advanceDurableBase);
    }
  }

  static CommonCheckpointPayload payload(int block) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block);
    PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
    when(binding.getMeta()).thenReturn(meta);
    when(binding.getParentStateRoot()).thenReturn(hash(block - 1));
    when(binding.getStateRoot()).thenReturn(hash(block));
    when(binding.getTransitionPayloadDigest()).thenReturn(hash(block + 10));
    PathStateFlushTarget.StoreTarget store = mock(PathStateFlushTarget.StoreTarget.class);
    when(store.getStoreId()).thenReturn(4);
    when(store.getDbName()).thenReturn("account");
    when(store.getStoreRoot()).thenReturn(hash(block));
    when(store.getFlatMutations()).thenReturn(Collections.emptyList());
    when(store.getNodeMutations()).thenReturn(Collections.emptyList());
    PathStateFlushTarget target = mock(PathStateFlushTarget.class);
    when(target.getBlocks()).thenReturn(Collections.singletonList(binding));
    when(target.getParentStateRoot()).thenReturn(hash(block - 1));
    when(target.getStateRoot()).thenReturn(hash(block));
    when(target.getStores()).thenReturn(Collections.singletonList(store));
    when(target.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.create(hash(70), target,
        Collections.singletonList(new BlockReverseDiff(meta, Collections.emptyList())),
        Collections.singletonList(new CommonCheckpointPayload.StoreMutations("properties",
            Collections.singletonList(new CommonCheckpointPayload.Mutation(
                new byte[]{(byte) block}, new byte[]{(byte) (block + 10)})))));
  }

  static byte[] hash(int value) {
    byte[] result = new byte[32];
    result[31] = (byte) value;
    return result;
  }

  private static final class Fake implements CommonCheckpointMaterializer {
    private final Authority authority;
    private CommonCheckpointTarget materialized;
    private CommonCheckpointTarget published;
    private boolean failSync;
    private volatile CountDownLatch syncReplayStarted;
    private volatile CountDownLatch syncReplayRelease;

    private Fake(Authority authority) {
      this.authority = authority;
    }

    @Override
    public Authority authority() {
      return authority;
    }

    @Override
    public Status inspect(CommonCheckpointTarget target) {
      return target.equals(published) ? Status.PUBLISHED
          : target.equals(materialized) ? Status.MATERIALIZED : Status.NEEDS_MATERIALIZATION;
    }

    @Override
    public void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target) {
      materialized = target;
    }

    @Override
    public void publish(CommonCheckpointTarget target) {
      published = target;
    }

    @Override
    public void replayRetained(List<CommonCheckpointPayload> payloads, boolean sync)
        throws IOException {
      if (sync && failSync) {
        throw new IOException("injected target force failure");
      }
      if (sync && syncReplayStarted != null) {
        syncReplayStarted.countDown();
        try {
          if (!syncReplayRelease.await(5, TimeUnit.SECONDS)) {
            throw new IOException("injected sync replay release timeout");
          }
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new IOException("injected sync replay interrupted", failure);
        }
      }
      if (!payloads.isEmpty()) {
        materialized = CommonCheckpointTarget.from(payloads.get(payloads.size() - 1));
      }
    }
  }
}
