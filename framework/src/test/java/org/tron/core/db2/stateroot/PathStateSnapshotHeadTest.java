package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateSnapshotHeadTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void publishesOnlyCommittedContinuousSnapshotsAcrossReopen() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("advance-" + engine, engine);
      PathStateSnapshotHead owner = PathStateSnapshotHead.open(
          fixture.manifest, PathStateLayerLimits.defaults());
      PreparedPathStateTransition prepared = owner.prepare(transition(101, 11,
          fixture.base.getBlockHash(), Collections.singletonList(
              PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5}))));
      PreparedPathStateTransition stale = owner.prepare(transition(101, 21,
          fixture.base.getBlockHash(), Collections.singletonList(
              PathStateMutation.put("proposal", new byte[]{1}, new byte[]{6}))));
      assertTrue(prepared.getNodeMutationCount() > 0);
      try (Stream<java.nio.file.Path> layers = Files.list(
          fixture.manifest.getLayersDirectory())) {
        assertFalse(layers.findAny().isPresent());
      }
      assertArrayEquals(fixture.base.encode(),
          new PathStateCurrentStore(fixture.manifest).current().encode());
      PathStateRootMetadata first = owner.advancePrepared(prepared);
      assertArrayEquals(first.getStateRoot(), owner.getSnapshot().getStateRoot());
      assertThrows(IOException.class, () -> owner.advancePrepared(stale));
      assertArrayEquals(first.encode(), owner.getHead().encode());

      assertThrows(IOException.class, () -> owner.advance(transition(103, 13,
          first.getBlockHash(), Collections.emptyList())));
      assertArrayEquals(first.encode(), owner.getHead().encode());
      assertFalse(owner.isFailed());

      PathStateRootMetadata second = owner.advance(transition(102, 12,
          first.getBlockHash(), Collections.emptyList()));
      assertArrayEquals(first.getStateRoot(), second.getStateRoot());
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
        assertArrayEquals(second.getStateRoot(), stores.createRoot().rootHash());
      }
    }
  }

  @Test
  public void commitAdmissionFailureKeepsOwnedSnapshotAndCurrent() throws Exception {
    Fixture fixture = fixture("admission", Engine.ROCKSDB);
    PathStateSnapshotHead owner = PathStateSnapshotHead.open(
        fixture.manifest, new PathStateLayerLimits(10, 1));

    assertThrows(IOException.class, () -> owner.advance(transition(101, 11,
        fixture.base.getBlockHash(), Collections.singletonList(
            PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5})))));

    assertArrayEquals(fixture.base.encode(), owner.getHead().encode());
    assertArrayEquals(fixture.base.encode(),
        new PathStateCurrentStore(fixture.manifest).current().encode());
    assertFalse(owner.isFailed());
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

  private static PathStateBlockTransition transition(long blockNumber, int hashSeed,
      byte[] parentHash, java.util.List<PathStateMutation> mutations) {
    return new PathStateBlockTransition(blockNumber, bytes(hashSeed), parentHash,
        blockNumber * 3, P66Phase.P66_ON, mutations);
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
