package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateBaseCompaction.RecoveryAction;
import org.tron.core.db2.stateroot.PathStateBaseCompaction.Stage;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateBaseCompactionTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void materializesPrefixOneLayerAtATimeAcrossEngines() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("compact-" + engine, engine);
      PathStateBaseCompaction compaction = new PathStateBaseCompaction(
          fixture.manifest, fixture.limits);

      PathStateRootMetadata compacted = compaction.compactThrough(
          fixture.second.getBlockNumber(), fixture.second.getBlockHash());

      assertEquals(Kind.BASE, compacted.getKind());
      assertEquals(102, compacted.getBlockNumber());
      assertArrayEquals(fixture.second.getStateRoot(), compacted.getStateRoot());
      assertFalse(Files.exists(fixture.manifest.getLayerDirectory(
          fixture.first.getBlockNumber(), fixture.first.getBlockHash())));
      assertFalse(Files.exists(fixture.manifest.getLayerDirectory(
          fixture.second.getBlockNumber(), fixture.second.getBlockHash())));
      assertArrayEquals(fixture.head.encode(),
          new PathStateCurrentStore(fixture.manifest).current().encode());
      try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
        PathStateRoot restored = stores.createRoot();
        assertArrayEquals(fixture.head.getStateRoot(), restored.rootHash());
        restored.verifyNodeStores();
      }
      assertEquals(RecoveryAction.NONE, compaction.recover());
    }
  }

  @Test
  public void everyDurableStageRecoversToTheSameBase() throws Exception {
    for (Engine engine : availableEngines()) {
      for (Stage failure : Stage.values()) {
        Fixture fixture = fixture("recover-" + engine + "-" + failure, engine);
        PathStateBaseCompaction interrupted = new PathStateBaseCompaction(
            fixture.manifest, fixture.limits, failAfter(failure));
        assertThrows(IOException.class, () -> interrupted.compactThrough(
            fixture.first.getBlockNumber(), fixture.first.getBlockHash()));

        PathStateBaseCompaction recovery = new PathStateBaseCompaction(
            fixture.manifest, fixture.limits);
        RecoveryAction expected = failure == Stage.AFTER_RETIRE
            ? RecoveryAction.NONE : RecoveryAction.COMPLETED_COMPACTION;
        assertEquals(expected, recovery.recover());
        assertEquals(RecoveryAction.NONE, recovery.recover());
        PathStateRootMetadata base = PathStateMetadataFile.load(fixture.manifest
            .getBaseDirectory().resolve(PathStateCurrentStore.METADATA_FILE));
        assertEquals(Kind.BASE, base.getKind());
        assertArrayEquals(fixture.first.getStateRoot(), base.getStateRoot());
        assertArrayEquals(fixture.head.encode(),
            new PathStateCurrentStore(fixture.manifest).current().encode());
        assertFalse(Files.exists(fixture.manifest.getDirectory()
            .resolve(PathStateBaseCompaction.INTENT_FILE)));
        assertFalse(Files.exists(fixture.manifest.getDirectory()
            .resolve(PathStateBaseCompaction.NEXT_DIRECTORY)));
        assertFalse(Files.exists(fixture.manifest.getDirectory()
            .resolve(PathStateBaseCompaction.PREVIOUS_DIRECTORY)));
      }
    }
  }

  @Test
  public void refusesToCompactTheReversibleHead() throws Exception {
    Fixture fixture = fixture("retain-head", Engine.ROCKSDB);
    PathStateBaseCompaction compaction = new PathStateBaseCompaction(
        fixture.manifest, fixture.limits);

    assertThrows(IOException.class, () -> compaction.compactThrough(
        fixture.head.getBlockNumber(), fixture.head.getBlockHash()));
    assertArrayEquals(fixture.base.encode(), PathStateMetadataFile.load(fixture.manifest
        .getBaseDirectory().resolve(PathStateCurrentStore.METADATA_FILE)).encode());
  }

  @Test
  public void snapshotStartupRecoversTheDirectorySwapGap() throws Exception {
    Fixture fixture = fixture("startup-recovery", Engine.ROCKSDB);
    PathStateBaseCompaction interrupted = new PathStateBaseCompaction(
        fixture.manifest, fixture.limits, failAfter(Stage.AFTER_OLD_BASE));
    assertThrows(IOException.class, () -> interrupted.compactThrough(
        fixture.first.getBlockNumber(), fixture.first.getBlockHash()));

    PathStateStoreManifest validated = PathStateStoreManifest.validateExisting(
        fixture.manifest.getDirectory(), Engine.ROCKSDB);
    PathStateSnapshotHead owner = PathStateSnapshotHead.open(validated, fixture.limits);

    assertArrayEquals(fixture.head.encode(), owner.getHead().encode());
    assertEquals(Kind.BASE, PathStateMetadataFile.load(validated.getBaseDirectory()
        .resolve(PathStateCurrentStore.METADATA_FILE)).getKind());
  }

  private Fixture fixture(String name, Engine engine) throws Exception {
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(
        new File(temporaryFolder.getRoot(), name).toPath(), engine);
    PathStateRootMetadata base;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), root.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
    }
    PathStateLayerLimits limits = new PathStateLayerLimits(10, Long.MAX_VALUE);
    PathStateRootMetadata first = append(manifest, base, 101, 11, limits);
    PathStateRootMetadata second = append(manifest, first, 102, 12, limits);
    PathStateRootMetadata head = append(manifest, second, 103, 13, limits);
    return new Fixture(manifest, limits, base, first, second, head);
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

  private static PathStateBaseCompaction.FaultHook failAfter(Stage failure) {
    return stage -> {
      if (stage == failure) {
        throw new IOException("injected after " + stage);
      }
    };
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
    private final PathStateLayerLimits limits;
    private final PathStateRootMetadata base;
    private final PathStateRootMetadata first;
    private final PathStateRootMetadata second;
    private final PathStateRootMetadata head;

    private Fixture(PathStateStoreManifest manifest, PathStateLayerLimits limits,
        PathStateRootMetadata base, PathStateRootMetadata first,
        PathStateRootMetadata second, PathStateRootMetadata head) {
      this.manifest = manifest;
      this.limits = limits;
      this.base = base;
      this.first = first;
      this.second = second;
      this.head = head;
    }
  }
}
