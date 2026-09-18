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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateLayerRetirement.RecoveryAction;
import org.tron.core.db2.stateroot.PathStateLayerRetirement.Stage;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateLayerRetirementTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void retiredForkReleasesLayerBudgetForCanonicalSibling() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("bounded-fork-" + engine, engine);
      PathStateLayerLimits limits = new PathStateLayerLimits(2, Long.MAX_VALUE);
      PathStateRootMetadata firstA = append(fixture.manifest, fixture.base, 101, 11, limits);
      PathStateRootMetadata secondA = append(fixture.manifest, firstA, 102, 13, limits);

      PathStateLayerRetirement retirement = new PathStateLayerRetirement(
          fixture.manifest, limits);
      assertArrayEquals(fixture.base.encode(),
          retirement.switchToAncestor(fixture.base).encode());
      assertFalse(Files.exists(layerDirectory(fixture.manifest, firstA)));
      assertFalse(Files.exists(layerDirectory(fixture.manifest, secondA)));

      PathStateRootMetadata firstB = append(fixture.manifest, fixture.base, 101, 21, limits);
      PathStateRootMetadata secondB = append(fixture.manifest, firstB, 102, 23, limits);
      assertArrayEquals(secondB.encode(), new PathStateCurrentStore(fixture.manifest)
          .current().encode());
      assertEquals(RecoveryAction.NONE, retirement.recover());
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
        PathStateRoot restored = stores.createRoot();
        assertArrayEquals(secondB.getStateRoot(), restored.rootHash());
        restored.verifyNodeStores();
      }
    }
  }

  @Test
  public void everyRetirementFaultRecoversIdempotently() throws Exception {
    for (Engine engine : availableEngines()) {
      for (Stage failure : Stage.values()) {
        Fixture fixture = fixture("recover-" + engine + "-" + failure, engine);
        PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
        PathStateRootMetadata first = append(fixture.manifest, fixture.base, 101, 11, limits);
        PathStateRootMetadata second = append(fixture.manifest, first, 102, 13, limits);
        PathStateLayerRetirement retirement = new PathStateLayerRetirement(
            fixture.manifest, limits, stage -> {
          if (stage == failure) {
            throw new IOException("injected after " + stage);
          }
        });

        assertThrows(IOException.class, () -> retirement.switchToAncestor(fixture.base));
        PathStateLayerRetirement recovery = new PathStateLayerRetirement(
            fixture.manifest, limits);
        RecoveryAction expected = failure == Stage.AFTER_RETIRE
            ? RecoveryAction.NONE : RecoveryAction.COMPLETED_RETIREMENT;
        assertEquals(expected, recovery.recover());
        assertEquals(RecoveryAction.NONE, recovery.recover());
        assertArrayEquals(fixture.base.encode(), new PathStateCurrentStore(fixture.manifest)
            .current().encode());
        assertFalse(Files.exists(layerDirectory(fixture.manifest, first)));
        assertFalse(Files.exists(layerDirectory(fixture.manifest, second)));
        assertFalse(Files.exists(fixture.manifest.getDirectory()
            .resolve(PathStateLayerRetirement.INTENT_FILE)));
      }
    }
  }

  @Test
  public void corruptRetirementIntentFailsClosedBeforeCurrentMoves() throws Exception {
    Fixture fixture = fixture("corrupt-intent", Engine.ROCKSDB);
    PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata first = append(fixture.manifest, fixture.base, 101, 11, limits);
    PathStateRootMetadata second = append(fixture.manifest, first, 102, 13, limits);
    PathStateLayerRetirement retirement = new PathStateLayerRetirement(fixture.manifest, limits,
        stage -> {
          if (stage == Stage.AFTER_INTENT) {
            throw new IOException("injected after intent");
          }
        });
    assertThrows(IOException.class, () -> retirement.switchToAncestor(fixture.base));
    Path intent = fixture.manifest.getDirectory().resolve(PathStateLayerRetirement.INTENT_FILE);
    byte[] corrupt = Files.readAllBytes(intent);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(intent, corrupt);

    assertThrows(IOException.class,
        new PathStateLayerRetirement(fixture.manifest, limits)::recover);
    assertArrayEquals(second.encode(), new PathStateCurrentStore(fixture.manifest)
        .current().encode());
    assertTrue(Files.exists(layerDirectory(fixture.manifest, first)));
    assertTrue(Files.exists(layerDirectory(fixture.manifest, second)));
  }

  @Test
  public void retirementCannotDeleteAfterAnotherCanonicalBranchAdvances() throws Exception {
    Fixture fixture = fixture("stale-retirement", Engine.ROCKSDB);
    PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata firstA = append(fixture.manifest, fixture.base, 101, 11, limits);
    PathStateRootMetadata secondA = append(fixture.manifest, firstA, 102, 13, limits);
    PathStateLayerRetirement retirement = new PathStateLayerRetirement(fixture.manifest, limits,
        stage -> {
          if (stage == Stage.AFTER_INTENT) {
            throw new IOException("injected after intent");
          }
        });
    assertThrows(IOException.class, () -> retirement.switchToAncestor(fixture.base));

    new PathStateCurrentStore(fixture.manifest).switchToAncestor(fixture.base, limits);
    PathStateRootMetadata firstB = append(fixture.manifest, fixture.base, 101, 21, limits);
    assertThrows(IOException.class,
        new PathStateLayerRetirement(fixture.manifest, limits)::recover);
    assertArrayEquals(firstB.encode(), new PathStateCurrentStore(fixture.manifest)
        .current().encode());
    assertTrue(Files.exists(layerDirectory(fixture.manifest, firstA)));
    assertTrue(Files.exists(layerDirectory(fixture.manifest, secondA)));
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

  private static Path layerDirectory(PathStateStoreManifest manifest,
      PathStateRootMetadata metadata) {
    return manifest.getLayerDirectory(metadata.getBlockNumber(), metadata.getBlockHash());
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
