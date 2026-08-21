package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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

public class PathStateCanonicalSwitchTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void switchesToAncestorThenBuildsAndRestoresCanonicalSiblingFork() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("fork-" + engine, engine);
      PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
      PathStateRootMetadata firstA = append(fixture.manifest, fixture.base, 101, 11, limits);
      PathStateRootMetadata secondA = append(fixture.manifest, firstA, 102, 13, limits);
      PathStateCurrentStore currentStore = new PathStateCurrentStore(fixture.manifest);

      assertArrayEquals(fixture.base.encode(),
          currentStore.switchToAncestor(fixture.base, limits).encode());
      PathStateRootMetadata firstB = append(fixture.manifest, fixture.base, 101, 21, limits);
      PathStateRootMetadata secondB = append(fixture.manifest, firstB, 102, 23, limits);

      assertArrayEquals(secondB.encode(), currentStore.current().encode());
      assertThrows(IOException.class, () -> currentStore.switchToAncestor(secondA, limits));
      assertArrayEquals(secondB.encode(), currentStore.current().encode());
      assertTrue(Files.exists(fixture.manifest.getLayerDirectory(102, secondA.getBlockHash())
          .resolve(PathStateCurrentStore.METADATA_FILE)));
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
        PathStateRoot restored = stores.createRoot();
        assertArrayEquals(secondB.getStateRoot(), restored.rootHash());
        restored.verifyNodeStores();
      }
      assertArrayEquals(secondB.encode(), new PathStateCurrentStore(
          PathStateStoreManifest.validateExisting(fixture.manifest.getDirectory(), engine))
          .current().encode());
    }
  }

  @Test
  public void deepReorgBeyondConfiguredWindowFailsClosed() throws Exception {
    Fixture fixture = fixture("deep-reorg", Engine.ROCKSDB);
    PathStateLayerLimits roomy = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata first = append(fixture.manifest, fixture.base, 101, 11, roomy);
    PathStateRootMetadata second = append(fixture.manifest, first, 102, 13, roomy);
    PathStateCurrentStore currentStore = new PathStateCurrentStore(fixture.manifest);

    assertThrows(IOException.class, () -> currentStore.switchToAncestor(fixture.base,
        new PathStateLayerLimits(1, Long.MAX_VALUE)));
    assertArrayEquals(second.encode(), currentStore.current().encode());
  }

  @Test
  public void corruptAncestorNodesRejectSwitchWithoutMovingCurrent() throws Exception {
    Fixture fixture = fixture("corrupt-target", Engine.ROCKSDB);
    PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata current = append(fixture.manifest, fixture.base, 101, 11, limits);
    Path nodes = fixture.manifest.getBaseDirectory().resolve(PathStateNodeStoreSet.NODES_DIRECTORY);
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(nodes, Engine.ROCKSDB)) {
      store.delete(durableLeafKey(21,
          PathStateCommitmentCodec.storeLeafKey(21, new byte[]{1})));
    }

    PathStateCurrentStore currentStore = new PathStateCurrentStore(fixture.manifest);
    assertThrows(IOException.class, () -> currentStore.switchToAncestor(fixture.base, limits));
    assertArrayEquals(current.encode(), currentStore.current().encode());
  }

  @Test
  public void failedCurrentReplacementPreservesOldCanonicalHead() throws Exception {
    Fixture fixture = fixture("switch-fault", Engine.ROCKSDB);
    PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata current = append(fixture.manifest, fixture.base, 101, 11, limits);
    PathStateCurrentStore currentStore = new PathStateCurrentStore(fixture.manifest);

    assertThrows(IOException.class, () -> currentStore.switchToAncestor(fixture.base, limits,
        temporary -> {
          assertTrue(Files.exists(temporary));
          throw new IOException("injected after temporary force");
        }));
    assertArrayEquals(current.encode(), currentStore.current().encode());
    try (Stream<Path> paths = Files.list(fixture.manifest.getDirectory())) {
      assertTrue(paths.noneMatch(path -> path.getFileName().toString().startsWith(".CURRENT-")));
    }
    assertArrayEquals(fixture.base.encode(),
        currentStore.switchToAncestor(fixture.base, limits).encode());
  }

  private Fixture fixture(String name, Engine engine) throws Exception {
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(
        new File(temporaryFolder.getRoot(), name).toPath(), engine);
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      PathStateRootMetadata base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), root.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
      return new Fixture(manifest, base);
    }
  }

  private static PathStateRootMetadata append(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, int seed, PathStateLayerLimits limits)
      throws Exception {
    try (PathStateLayer layer = PathStateLayer.begin(manifest, parent, blockNumber, bytes(seed),
        parent.getBlockHash(), blockNumber * 3, P66Phase.P66_ON, bytes(seed + 1), limits)) {
      layer.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{(byte) seed})));
      return layer.commit();
    }
  }

  private static byte[] durableLeafKey(int storeId, byte[] secureKey) {
    return java.nio.ByteBuffer.allocate(Integer.BYTES * 2 + secureKey.length)
        .putInt(-2)
        .putInt(storeId)
        .put(secureKey)
        .array();
  }

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB}
        : new Engine[]{Engine.LEVELDB, Engine.ROCKSDB};
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
