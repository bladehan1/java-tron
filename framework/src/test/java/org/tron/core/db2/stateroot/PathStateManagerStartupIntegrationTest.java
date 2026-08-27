package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.Storage;
import org.tron.core.db.Manager;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
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
    Manager manager = new Manager();
    setChainBaseManager(manager, chainBase);

    withConfig(output, true, () -> invoke(manager, "initPathStateRoot"));
    assertNotNull(manager.getPathStateSnapshotHead());
    assertArrayEquals(base.encode(), manager.getPathStateSnapshotHead().getHead().encode());
    invoke(manager, "closePathStateRoot");
    assertNull(manager.getPathStateSnapshotHead());

    when(dynamic.getLatestBlockHeaderNumber()).thenReturn(101L);
    assertThrows(IllegalStateException.class,
        () -> withConfig(output, true, () -> invoke(manager, "initPathStateRoot")));
    assertNull(manager.getPathStateSnapshotHead());
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
    java.lang.reflect.Field field = Manager.class.getDeclaredField("chainBaseManager");
    field.setAccessible(true);
    field.set(manager, chainBase);
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

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
