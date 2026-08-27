package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.args.Storage;
import org.tron.core.db.Manager;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.DynamicPropertiesStore;

public class PathStateManagerStartupIntegrationTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void disabledAndMissingStartupDoNotCreatePathStateDirectory() throws Exception {
    Path output = temporaryFolder.newFolder("startup-gates").toPath();
    Manager disabled = new Manager();
    withConfig(output, false, () -> invoke(disabled, "initPathStateRoot"));
    assertNull(disabled.getPathStateSnapshotHead());
    assertFalse(Files.exists(output.resolve("path-state-root")));

    Manager missing = new Manager();
    assertThrows(IllegalStateException.class,
        () -> withConfig(output, true, () -> invoke(missing, "initPathStateRoot")));
    assertNull(missing.getPathStateSnapshotHead());
    assertFalse(Files.exists(output.resolve("path-state-root")));
  }

  @Test
  public void readyCurrentAttachesExactCanonicalHeadAndCloses() throws Exception {
    Path output = temporaryFolder.newFolder("startup-ready").toPath();
    Path root = output.resolve("path-state-root");
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(root, Engine.ROCKSDB);
    PathStateRootMetadata base;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot state = stores.createRoot();
      base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), state.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
    }

    DynamicPropertiesStore dynamic = mock(DynamicPropertiesStore.class);
    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(100L);
    when(dynamic.getLatestBlockHeaderHash()).thenReturn(Sha256Hash.wrap(base.getBlockHash()));
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(dynamic);
    when(chainBase.getAccountAssetStore()).thenReturn(mock(AccountAssetStore.class));
    Manager manager = new Manager();
    setChainBaseManager(manager, chainBase);
    setField(manager, "revokingStore", new SnapshotManager(""));

    withConfig(output, true, () -> invoke(manager, "initPathStateRoot"));
    assertNotNull(manager.getPathStateSnapshotHead());
    assertNotNull(manager.getPathStateRuntime());
    assertArrayEquals(base.encode(), manager.getPathStateSnapshotHead().getHead().encode());
    assertEquals(PathStateRuntimeAttachment.State.READY,
        manager.getPathStateRuntime().status().getState());
    assertEquals(100L, manager.getPathStateRuntime().status().getReadyBlockNumber());
    invoke(manager, "closePathStateRoot");
    assertNull(manager.getPathStateSnapshotHead());
    assertNull(manager.getPathStateRuntime());

    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(101L);
    assertThrows(IllegalStateException.class,
        () -> withConfig(output, true, () -> invoke(manager, "initPathStateRoot")));
    assertNull(manager.getPathStateSnapshotHead());
  }

  @Test
  public void shortReorgRewindsToChainbaseHeadAndRetiresOldSuffix() throws Exception {
    Path output = temporaryFolder.newFolder("short-reorg").toPath();
    Path root = output.resolve("path-state-root");
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(root, Engine.ROCKSDB);
    PathStateRootMetadata base;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot state = stores.createRoot();
      base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), state.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
    }
    PathStateSnapshotHead builder = PathStateSnapshotHead.open(
        manifest, PathStateLayerLimits.defaults());
    PathStateRootMetadata first = builder.advance(transition(101, 11, base.getBlockHash()));
    PathStateRootMetadata oldSecond = builder.advance(transition(102, 12,
        first.getBlockHash()));

    DynamicPropertiesStore dynamic = mock(DynamicPropertiesStore.class);
    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(102L);
    when(dynamic.getLatestBlockHeaderHash()).thenReturn(
        Sha256Hash.wrap(oldSecond.getBlockHash()));
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(dynamic);
    when(chainBase.getAccountAssetStore()).thenReturn(mock(AccountAssetStore.class));
    Manager manager = new Manager();
    setChainBaseManager(manager, chainBase);
    setField(manager, "revokingStore", new SnapshotManager(""));

    withConfig(output, true, () -> invoke(manager, "initPathStateRoot"));
    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(101L);
    when(dynamic.getLatestBlockHeaderHash()).thenReturn(Sha256Hash.wrap(first.getBlockHash()));
    invoke(manager, "rewindPathStateRootAfterPop");

    assertArrayEquals(first.encode(), manager.getPathStateSnapshotHead().getHead().encode());
    assertArrayEquals(first.encode(), new PathStateCurrentStore(manifest).current().encode());
    assertFalse(Files.exists(manifest.getLayerDirectory(
        oldSecond.getBlockNumber(), oldSecond.getBlockHash())));
    assertFalse(manager.getPathStateRuntime().isFailed());
    assertEquals(PathStateRuntimeAttachment.State.READY,
        manager.getPathStateRuntime().status().getState());
    assertEquals(101L, manager.getPathStateRuntime().status().getReadyBlockNumber());

    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(100L);
    when(dynamic.getLatestBlockHeaderHash()).thenReturn(Sha256Hash.wrap(bytes(99)));
    invoke(manager, "rewindPathStateRootAfterPop");
    assertNotNull(manager.getPathStateRuntime().getFailure());
    assertEquals(PathStateRuntimeAttachment.FailureStage.REORG,
        manager.getPathStateRuntime().status().getFailureStage());
    assertEquals(100L, manager.getPathStateRuntime().status().getObservedBlockNumber());
    assertEquals(1L, manager.getPathStateRuntime().status().getRootLag());
    assertArrayEquals(first.encode(), new PathStateCurrentStore(manifest).current().encode());
    invoke(manager, "closePathStateRoot");
  }

  @Test
  public void missingCurrentRebuildsExactNativeSnapshotAndAttaches() throws Exception {
    Path output = temporaryFolder.newFolder("startup-rebuild").toPath();
    long blockNumber = 100L;
    long timestamp = 300L;
    BlockId blockId = new BlockId(Sha256Hash.wrap(bytes(1)), blockNumber);
    Sha256Hash parentHash = Sha256Hash.wrap(bytes(2));
    DynamicPropertiesStore dynamic = mock(DynamicPropertiesStore.class);
    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(blockNumber);
    when(dynamic.getLatestBlockHeaderHash()).thenReturn(blockId);
    when(dynamic.getLatestBlockHeaderTimestamp()).thenReturn(timestamp);
    when(dynamic.getAllowSameTokenName()).thenReturn(1L);
    BlockCapsule block = mock(BlockCapsule.class);
    when(block.getNum()).thenReturn(blockNumber);
    when(block.getBlockId()).thenReturn(blockId);
    when(block.getParentHash()).thenReturn(parentHash);
    when(block.getTimeStamp()).thenReturn(timestamp);
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(dynamic);
    when(chainBase.getBlockByNum(blockNumber)).thenReturn(block);
    when(chainBase.getAccountAssetStore()).thenReturn(mock(AccountAssetStore.class));

    AtomicInteger closed = new AtomicInteger();
    Manager manager = new Manager();
    setChainBaseManager(manager, chainBase);

    withConfig(output, true, () -> {
      SnapshotManager snapshots = new SnapshotManager("");
      for (PathStateParticipantDescriptor.StoreIdentity participant
          : PathStateParticipantDescriptor.current().getStores()) {
        snapshots.getDbs().add(emptyNativeStore(participant.getDbName(), blockNumber,
            blockId.getBytes(), closed));
      }
      setField(manager, "revokingStore", snapshots);
      invoke(manager, "initPathStateRoot");
    });
    assertNotNull(manager.getPathStateSnapshotHead());
    PathStateRootMetadata head = manager.getPathStateSnapshotHead().getHead();
    assertArrayEquals(blockId.getBytes(), head.getBlockHash());
    assertArrayEquals(parentHash.getBytes(), head.getParentHash());
    assertNotNull(PathStateStoreManifest.validateExisting(
        output.resolve("path-state-root"), Engine.ROCKSDB));
    assertEquals(PathStateParticipantDescriptor.current().getStores().size(), closed.get());
  }

  @SuppressWarnings("unchecked")
  private static Chainbase emptyNativeStore(String dbName, long blockNumber, byte[] blockHash,
      AtomicInteger closed) throws Exception {
    DB<byte[], byte[]> database = mock(DB.class,
        withSettings().extraInterfaces(SnapshotCapableStore.class));
    SnapshotCapableStore capable = (SnapshotCapableStore) database;
    when(database.getDbName()).thenReturn(dbName);
    when(capable.getDbName()).thenReturn(dbName);
    when(capable.getSourceIdentity()).thenReturn("source-" + dbName);
    StoreSnapshot snapshot = mock(StoreSnapshot.class);
    when(snapshot.getDbName()).thenReturn(dbName);
    when(snapshot.getSourceIdentity()).thenReturn("source-" + dbName);
    when(snapshot.getBlockNumber()).thenReturn(blockNumber);
    when(snapshot.getBlockHash()).thenReturn(blockHash);
    when(snapshot.range(org.mockito.ArgumentMatchers.any(byte[].class),
        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(Collections.emptyList());
    org.mockito.Mockito.doAnswer(invocation -> {
      closed.incrementAndGet();
      return null;
    }).when(snapshot).close();
    when(capable.pin(org.mockito.ArgumentMatchers.eq(blockNumber),
        org.mockito.ArgumentMatchers.any(byte[].class))).thenReturn(snapshot);
    return new Chainbase(new SnapshotRoot(database));
  }

  private static void withConfig(Path output, boolean enabled, ThrowingRunnable action)
      throws Exception {
    CommonParameter args = CommonParameter.getInstance();
    Storage oldStorage = args.getStorage();
    Storage storage = new Storage();
    String oldOutput = args.outputDirectory;
    try {
      args.outputDirectory = output.toString();
      args.storage = storage;
      storage.setDbEngine("ROCKSDB");
      storage.setPathStateRootEnabled(enabled);
      storage.setPathStateRootDirectory("path-state-root");
      storage.setPathStateRootReversibleLayerLimit(8);
      storage.setPathStateRootReversibleLayerBytes(1L << 20);
      action.run();
    } finally {
      args.outputDirectory = oldOutput;
      args.storage = oldStorage;
    }
  }

  private static void setChainBaseManager(Manager manager, ChainBaseManager chainBase)
      throws Exception {
    setField(manager, "chainBaseManager", chainBase);
  }

  private static void setField(Manager manager, String name, Object value) throws Exception {
    java.lang.reflect.Field field = Manager.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(manager, value);
  }

  private static void invoke(Manager manager, String methodName) throws Exception {
    Method method = Manager.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    try {
      method.invoke(manager);
    } catch (InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw failure;
    }
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static PathStateBlockTransition transition(long blockNumber, int seed,
      byte[] parentHash) {
    return new PathStateBlockTransition(blockNumber, bytes(seed), parentHash,
        blockNumber * 3, P66Phase.P66_ON, Collections.emptyList());
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
