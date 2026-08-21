package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateLayerLimitsTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void countLimitRejectsSecondLayerBeforeCreatingItsDirectory() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("count-" + engine, engine);
      PathStateLayerLimits limits = new PathStateLayerLimits(1, Long.MAX_VALUE);
      PathStateRootMetadata first = append(fixture.manifest, fixture.base, 101, 11, limits);
      Path rejected = fixture.manifest.getLayerDirectory(102, bytes(13));

      assertThrows(IOException.class, () -> PathStateLayer.begin(fixture.manifest, first, 102,
          bytes(13), first.getBlockHash(), 306, P66Phase.P66_ON, bytes(14), limits));
      assertFalse(Files.exists(rejected));
      assertArrayEquals(first.encode(),
          new PathStateCurrentStore(fixture.manifest).current().encode());
      Long logicalBytes = PathStateNodeStoreSet.loadLogicalBytes(
          fixture.manifest.getLayerDirectory(101, first.getBlockHash()), fixture.manifest);
      assertNotNull(logicalBytes);
      assertTrue(logicalBytes > 0);
    }
  }

  @Test
  public void byteLimitRejectsCommitBeforeIntentOrNativeProgress() throws Exception {
    Fixture fixture = fixture("bytes", Engine.ROCKSDB);
    PathStateLayerLimits limits = new PathStateLayerLimits(10, 1);
    Path directory = fixture.manifest.getLayerDirectory(101, bytes(11));
    try (PathStateLayer layer = PathStateLayer.begin(fixture.manifest, fixture.base, 101,
        bytes(11), fixture.base.getBlockHash(), 303, P66Phase.P66_ON, bytes(12), limits)) {
      layer.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5})));
      assertThrows(IOException.class, layer::commit);
    }

    assertFalse(Files.exists(directory.resolve(PathStateLayerPublication.INTENT_FILE)));
    assertFalse(Files.exists(directory.resolve(PathStateCurrentStore.METADATA_FILE)));
    assertNull(PathStateNodeStoreSet.loadProgress(directory, fixture.manifest));
    assertNull(PathStateNodeStoreSet.loadLogicalBytes(directory, fixture.manifest));
    assertArrayEquals(fixture.base.encode(),
        new PathStateCurrentStore(fixture.manifest).current().encode());
  }

  @Test
  public void restartRejectsStoredUsageAboveConfiguredLimits() throws Exception {
    Fixture fixture = fixture("restart-limit", Engine.ROCKSDB);
    PathStateLayerLimits roomy = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata first = append(fixture.manifest, fixture.base, 101, 11, roomy);
    append(fixture.manifest, first, 102, 13, roomy);

    assertThrows(IOException.class, () -> new PathStateLayerPublication(fixture.manifest,
        new PathStateLayerLimits(1, Long.MAX_VALUE)).recover());
    assertThrows(IOException.class, () -> new PathStateLayerPublication(fixture.manifest,
        new PathStateLayerLimits(10, 1)).recover());
  }

  @Test
  public void corruptLogicalBytesMarkerFailsClosed() throws Exception {
    Fixture fixture = fixture("corrupt-marker", Engine.ROCKSDB);
    PathStateRootMetadata layer = append(fixture.manifest, fixture.base, 101, 11,
        PathStateLayerLimits.defaults());
    Path nodes = fixture.manifest.getLayerDirectory(101, layer.getBlockHash())
        .resolve(PathStateNodeStoreSet.NODES_DIRECTORY);
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(nodes, Engine.ROCKSDB)) {
      store.put(logicalBytesKey(), ByteBuffer.allocate(Long.BYTES).putLong(1).array());
    }

    assertThrows(IOException.class,
        new PathStateLayerPublication(fixture.manifest)::recover);
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

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB}
        : new Engine[]{Engine.LEVELDB, Engine.ROCKSDB};
  }

  private static byte[] logicalBytesKey() {
    byte[] suffix = "logical-bytes".getBytes(StandardCharsets.US_ASCII);
    return ByteBuffer.allocate(Integer.BYTES + suffix.length)
        .putInt(-1).put(suffix).array();
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
