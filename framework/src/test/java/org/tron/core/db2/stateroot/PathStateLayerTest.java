package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateLayerTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void layersInheritPublishedParentAndRestoreCurrentAcrossReopen() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = publishedBase("inherit-" + engine, engine);
      PathStateRootMetadata first;
      byte[] firstRoot;
      try (PathStateLayer layer = PathStateLayer.begin(fixture.manifest, fixture.base, 101,
          bytes(11), fixture.base.getBlockHash(), 303, P66Phase.P66_ON, bytes(12))) {
        layer.apply(Arrays.asList(
            PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5}),
            PathStateMutation.delete("account", new byte[]{3})));
        firstRoot = layer.rootHash();
        first = layer.commit();
        assertArrayEquals(first.encode(), layer.commit().encode());
        assertThrows(IllegalStateException.class, () -> layer.apply(Collections.singletonList(
            PathStateMutation.delete("proposal", new byte[]{1}))));
      }

      assertCurrentRoot(fixture.manifest, first, firstRoot);
      assertTrue(nodeEntryCount(fixture.manifest.getLayerDirectory(101, first.getBlockHash()),
          engine) < nodeEntryCount(fixture.manifest.getBaseDirectory(), engine));
      assertTrue(nodeTombstoneCount(
          fixture.manifest.getLayerDirectory(101, first.getBlockHash()), engine) > 0);

      PathStateRootMetadata second;
      byte[] secondRoot;
      try (PathStateLayer layer = PathStateLayer.begin(fixture.manifest, first, 102,
          bytes(13), first.getBlockHash(), 306, P66Phase.P66_ON, bytes(14))) {
        layer.apply(Collections.singletonList(
            PathStateMutation.put("account", new byte[]{7}, new byte[]{8})));
        secondRoot = layer.rootHash();
        second = layer.commit();
      }

      assertCurrentRoot(fixture.manifest, second, secondRoot);
      try (Stream<Path> layers = Files.list(fixture.manifest.getLayersDirectory())) {
        assertEquals(2, layers.count());
      }
    }
  }

  @Test
  public void staleParentFailsBeforeCreatingLayerDirectory() throws Exception {
    Fixture fixture = publishedBase("stale-parent", Engine.ROCKSDB);
    PathStateRootMetadata first;
    try (PathStateLayer layer = PathStateLayer.begin(fixture.manifest, fixture.base, 101,
        bytes(11), fixture.base.getBlockHash(), 303, P66Phase.P66_ON, bytes(12))) {
      layer.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5})));
      first = layer.commit();
    }

    Path rejected = fixture.manifest.getLayerDirectory(102, bytes(13));
    assertThrows(IOException.class, () -> PathStateLayer.begin(fixture.manifest, fixture.base, 102,
        bytes(13), first.getBlockHash(), 306, P66Phase.P66_ON, bytes(14)));
    assertFalse(Files.exists(rejected));
  }

  @Test
  public void currentLayerRestoreFailsClosedWhenDurableLeafIsMissing() throws Exception {
    Fixture fixture = publishedBase("corrupt-layer", Engine.ROCKSDB);
    PathStateRootMetadata layer;
    try (PathStateLayer child = PathStateLayer.begin(fixture.manifest, fixture.base, 101,
        bytes(11), fixture.base.getBlockHash(), 303, P66Phase.P66_ON, bytes(12))) {
      child.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5})));
      layer = child.commit();
    }

    Path nodes = fixture.manifest.getLayerDirectory(101, layer.getBlockHash())
        .resolve(PathStateNodeStoreSet.NODES_DIRECTORY);
    try (PathStateNativeNodeStore nativeStore =
        PathStateNativeNodeStore.open(nodes, Engine.ROCKSDB)) {
      nativeStore.delete(durableLeafKey(21,
          PathStateCommitmentCodec.storeLeafKey(21, new byte[]{1})));
    }
    try (PathStateNodeStoreSet current = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
      assertThrows(IllegalStateException.class, current::createRoot);
    }
  }

  private Fixture publishedBase(String name, Engine engine) throws Exception {
    Path rootDirectory = new File(temporaryFolder.getRoot(), name).toPath();
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(rootDirectory, engine);
    PathStateRootMetadata base;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300, P66Phase.P66_ON,
          manifest.getIdentityDigest(), root.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
    }
    return new Fixture(manifest, base);
  }

  private static void assertCurrentRoot(PathStateStoreManifest manifest,
      PathStateRootMetadata expected, byte[] expectedRoot) throws Exception {
    assertArrayEquals(expected.encode(), new PathStateCurrentStore(manifest).current().encode());
    try (PathStateNodeStoreSet current = PathStateNodeStoreSet.openCurrent(manifest)) {
      PathStateRoot restored = current.createRoot();
      assertArrayEquals(expectedRoot, restored.rootHash());
      restored.verifyNodeStores();
    }
  }

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB}
        : new Engine[]{Engine.LEVELDB, Engine.ROCKSDB};
  }

  private static byte[] durableLeafKey(int storeId, byte[] secureKey) {
    return java.nio.ByteBuffer.allocate(Integer.BYTES * 2 + secureKey.length)
        .putInt(-2)
        .putInt(storeId)
        .put(secureKey)
        .array();
  }

  private static long nodeEntryCount(Path owner, Engine engine) throws Exception {
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(
        owner.resolve(PathStateNodeStoreSet.NODES_DIRECTORY), engine)) {
      return store.scanAll().stream()
          .filter(entry -> java.nio.ByteBuffer.wrap(entry.getKey()).getInt() >= 0)
          .count();
    }
  }

  private static long nodeTombstoneCount(Path owner, Engine engine) throws Exception {
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(
        owner.resolve(PathStateNodeStoreSet.NODES_DIRECTORY), engine)) {
      return store.scanAll().stream()
          .filter(entry -> java.nio.ByteBuffer.wrap(entry.getKey()).getInt() == -3)
          .count();
    }
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static final class Fixture {

    private final PathStateStoreManifest manifest;
    private final PathStateRootMetadata base;

    private Fixture(PathStateStoreManifest manifest, PathStateRootMetadata base) {
      this.manifest = manifest;
      this.base = base;
    }
  }
}
