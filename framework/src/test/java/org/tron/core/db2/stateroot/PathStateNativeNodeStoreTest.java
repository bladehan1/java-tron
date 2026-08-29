package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateNativeNodeStoreTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void nativeStoresOwnBytesAndPreserveSyncedMutationsAcrossReopen() throws Exception {
    for (Engine engine : availableEngines()) {
      Path directory = new File(temporaryFolder.getRoot(), "native-" + engine).toPath();
      byte[] path = new byte[]{1, 2, 3};
      byte[] node = new byte[]{4, 5, 6};
      try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(directory, engine)) {
        store.put(path, node);
        path[0] = 15;
        node[0] = 15;
        byte[] returned = store.get(new byte[]{1, 2, 3});
        returned[0] = 15;
        assertArrayEquals(new byte[]{4, 5, 6}, store.get(new byte[]{1, 2, 3}));
      }
      try (PathStateNativeNodeStore reopened = PathStateNativeNodeStore.open(directory, engine)) {
        assertArrayEquals(new byte[]{4, 5, 6}, reopened.get(new byte[]{1, 2, 3}));
        reopened.delete(new byte[]{1, 2, 3});
      }
      try (PathStateNativeNodeStore reopened = PathStateNativeNodeStore.open(directory, engine)) {
        assertNull(reopened.get(new byte[]{1, 2, 3}));
      }
    }
  }

  @Test
  public void nativeStoreRejectsInvalidEntriesAndUseAfterClose() throws Exception {
    Path directory = temporaryFolder.newFolder("native-invalid").toPath();
    PathStateNativeNodeStore store = PathStateNativeNodeStore.open(directory, Engine.ROCKSDB);
    assertThrows(IllegalArgumentException.class, () -> store.put(new byte[0], new byte[]{1}));
    assertThrows(IllegalArgumentException.class, () -> store.put(new byte[]{1}, new byte[0]));
    store.close();
    store.close();
    assertThrows(IllegalStateException.class, () -> store.get(new byte[0]));
  }

  @Test
  public void streamsNativeScansWithoutCollectingTheResultSet() throws Exception {
    for (Engine engine : availableEngines()) {
      Path directory = new File(temporaryFolder.getRoot(), "stream-" + engine).toPath();
      List<PathStateNativeNodeStore.BatchMutation> mutations = new ArrayList<>();
      for (int index = 0; index < 64; index++) {
        mutations.add(PathStateNativeNodeStore.BatchMutation.put(
            new byte[]{1, (byte) index}, new byte[]{(byte) (index + 1)}));
      }
      try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(directory, engine)) {
        store.writeBatch(mutations);
        AtomicInteger prefixCount = new AtomicInteger();
        AtomicInteger allCount = new AtomicInteger();
        store.scanPrefix(new byte[]{1}, entry -> prefixCount.incrementAndGet());
        store.scanAll(entry -> allCount.incrementAndGet());
        assertEquals(64, prefixCount.get());
        assertEquals(64, allCount.get());
      }
    }
  }

  @Test
  public void baseStoreSetCreatesExact27PlusSuperAndPersistsRootNodes() throws Exception {
    PathStateStoreManifest manifest = manifest("base-set", Engine.ROCKSDB);
    byte[] rootHash;
    PathStateRootMetadata progress;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2})));
      rootHash = root.rootHash();
      progress = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), rootHash, bytes(3));
      stores.commit(progress);
      assertArrayEquals(progress.encode(), stores.getProgress().encode());
      root.verifyNodeStores();
      assertThrows(IllegalStateException.class, stores::createRoot);
    }

    assertEquals(1, childDirectoryCount(manifest.getBaseDirectory()));
    try (PathStateNativeNodeStore nodes = PathStateNativeNodeStore.open(
        manifest.getBaseDirectory().resolve("nodes"), Engine.ROCKSDB)) {
      assertNotNull(nodes.get(namespaceRootKey(21)));
      assertNotNull(nodes.get(namespaceRootKey(0)));
      assertEquals(32, rootHash.length);
    }
    try (PathStateNodeStoreSet reopened = PathStateNodeStoreSet.openBase(manifest)) {
      assertArrayEquals(progress.encode(), reopened.getProgress().encode());
      PathStateRoot restored = reopened.createRoot();
      assertArrayEquals(rootHash, restored.rootHash());
      restored.verifyNodeStores();
    }
  }

  @Test
  public void missingDurableLeafFailsClosedDuringRootRestore() throws Exception {
    PathStateStoreManifest manifest = manifest("missing-leaf", Engine.ROCKSDB);
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2})));
      byte[] stateRoot = root.rootHash();
      stores.commit(PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), stateRoot, bytes(3)));
    }
    try (PathStateNativeNodeStore nodes = PathStateNativeNodeStore.open(
        manifest.getBaseDirectory().resolve("nodes"), Engine.ROCKSDB)) {
      nodes.delete(durableLeafKey(21,
          PathStateCommitmentCodec.storeLeafKey(21, new byte[]{1})));
    }
    try (PathStateNodeStoreSet reopened = PathStateNodeStoreSet.openBase(manifest)) {
      assertThrows(IllegalStateException.class, reopened::createRoot);
    }
  }

  @Test
  public void restoredBaseCommitsLeafUpdatesAndDeletesAcrossSecondReopen() throws Exception {
    for (Engine engine : availableEngines()) {
      PathStateStoreManifest manifest = manifest("leaf-delta-" + engine, engine);
      byte[] firstRoot;
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
        PathStateRoot root = stores.createRoot();
        root.apply(Arrays.asList(
            PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
            PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
        firstRoot = root.rootHash();
        stores.commit(PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
            P66Phase.P66_ON, manifest.getIdentityDigest(), firstRoot, bytes(3)));
      }

      byte[] secondRoot;
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
        PathStateRoot root = stores.createRoot();
        assertArrayEquals(firstRoot, root.rootHash());
        root.apply(Arrays.asList(
            PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5}),
            PathStateMutation.delete("account", new byte[]{3})));
        secondRoot = root.rootHash();
        stores.commit(PathStateRootMetadata.base(101, bytes(4), bytes(1), 303,
            P66Phase.P66_ON, manifest.getIdentityDigest(), secondRoot, bytes(6)));
      }

      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
        PathStateRoot root = stores.createRoot();
        assertArrayEquals(secondRoot, root.rootHash());
        root.verifyNodeStores();
      }
    }
  }

  @Test
  public void rejectedProgressDoesNotFlushPendingNodes() throws Exception {
    PathStateStoreManifest manifest = manifest("rejected-progress", Engine.ROCKSDB);
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2})));
      root.rootHash();
      PathStateRootMetadata mismatch = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), bytes(9), bytes(3));
      assertThrows(IllegalArgumentException.class, () -> stores.commit(mismatch));
      assertNull(stores.getProgress());
    }

    try (PathStateNativeNodeStore nodes = PathStateNativeNodeStore.open(
        manifest.getBaseDirectory().resolve("nodes"), Engine.ROCKSDB)) {
      assertNull(nodes.get(namespaceRootKey(21)));
      assertNull(nodes.get(namespaceRootKey(0)));
    }
  }

  @Test
  public void levelAndRocksStoreSetsProduceTheSameCurrentRoot() throws Exception {
    org.junit.Assume.assumeFalse(Arch.isArm64());
    byte[] level = rootFor(manifest("set-level", Engine.LEVELDB));
    byte[] rocks = rootFor(manifest("set-rocks", Engine.ROCKSDB));
    assertArrayEquals(level, rocks);
  }

  @Test
  public void layerStoreSetIsBoundToMetadataAndCanonicalDirectory() throws Exception {
    PathStateStoreManifest manifest = manifest("layer-set", Engine.ROCKSDB);
    PathStateRootMetadata layer = PathStateRootMetadata.layer(101, bytes(1), bytes(2), 300,
        P66Phase.P66_ON, manifest.getIdentityDigest(), bytes(3), bytes(4), bytes(5));
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openLayer(manifest, layer)) {
      assertEquals(manifest.getLayerDirectory(101, bytes(1)).resolve("nodes"),
          stores.getDirectory());
    }

    PathStateRootMetadata foreign = PathStateRootMetadata.layer(101, bytes(1), bytes(2), 300,
        P66Phase.P66_ON, bytes(9), bytes(3), bytes(4), bytes(5));
    assertThrows(java.io.IOException.class,
        () -> PathStateNodeStoreSet.openLayer(manifest, foreign));
  }

  @Test
  public void immutableLayerMetadataSealsTheWritableNodeSet() throws Exception {
    PathStateStoreManifest manifest = manifest("sealed-layer", Engine.ROCKSDB);
    PathStateRootMetadata layer = PathStateRootMetadata.layer(101, bytes(1), bytes(2), 300,
        P66Phase.P66_ON, manifest.getIdentityDigest(), bytes(3), bytes(4), bytes(5));
    Path layerDirectory = manifest.getLayerDirectory(101, bytes(1));
    try (PathStateNodeStoreSet ignored = PathStateNodeStoreSet.openLayer(manifest, layer)) {
      assertNotNull(ignored);
    }
    PathStateMetadataFile.publishImmutable(
        layerDirectory.resolve(PathStateCurrentStore.METADATA_FILE), layer);

    assertThrows(java.io.IOException.class,
        () -> PathStateNodeStoreSet.openLayer(manifest, layer));
  }

  private byte[] rootFor(PathStateStoreManifest manifest) throws Exception {
    byte[] stateRoot;
    PathStateRootMetadata progress;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      stateRoot = root.rootHash();
      progress = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), stateRoot, bytes(3));
      stores.commit(progress);
    }
    try (PathStateNodeStoreSet reopened = PathStateNodeStoreSet.openBase(manifest)) {
      assertArrayEquals(progress.encode(), reopened.getProgress().encode());
      assertArrayEquals(stateRoot, reopened.createRoot().rootHash());
    }
    return stateRoot;
  }

  private PathStateStoreManifest manifest(String name, Engine engine) throws Exception {
    return PathStateStoreManifest.createOrOpen(temporaryFolder.newFolder(name).toPath(), engine);
  }

  private static List<Engine> availableEngines() {
    return Arch.isArm64() ? Collections.singletonList(Engine.ROCKSDB)
        : Arrays.asList(Engine.LEVELDB, Engine.ROCKSDB);
  }

  private static long childDirectoryCount(Path directory) throws Exception {
    try (Stream<Path> paths = Files.list(directory)) {
      return paths.filter(Files::isDirectory).count();
    }
  }

  private static byte[] namespaceRootKey(int storeId) {
    return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(storeId).array();
  }

  private static byte[] durableLeafKey(int storeId, byte[] secureKey) {
    return java.nio.ByteBuffer.allocate(Integer.BYTES * 2 + secureKey.length)
        .putInt(-2)
        .putInt(storeId)
        .put(secureKey)
        .array();
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }
}
