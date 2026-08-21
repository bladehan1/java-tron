package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

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
import org.tron.core.db2.stateroot.PathStateLayerPublication.RecoveryAction;
import org.tron.core.db2.stateroot.PathStateLayerPublication.Stage;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateLayerPublicationTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void faultsBeforeNativeProgressRollBackIntentIdempotently() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("before-progress-" + engine, engine, Stage.AFTER_INTENT);
      assertThrows(IOException.class, fixture::publish);
      fixture.close();

      PathStateLayerPublication recovery = new PathStateLayerPublication(fixture.manifest);
      assertEquals(RecoveryAction.ROLLED_BACK_INTENT, recovery.recover());
      assertEquals(RecoveryAction.NONE, recovery.recover());
      assertArrayEquals(fixture.base.encode(),
          new PathStateCurrentStore(fixture.manifest).current().encode());
      assertFalse(Files.exists(fixture.intentPath()));
    }
  }

  @Test
  public void faultsAfterNativeProgressCompletePublicationIdempotently() throws Exception {
    for (Engine engine : availableEngines()) {
      for (Stage stage : new Stage[]{Stage.AFTER_NODE_PROGRESS, Stage.AFTER_METADATA,
          Stage.AFTER_CURRENT}) {
        Fixture fixture = fixture("complete-" + engine + "-" + stage, engine, stage);
        assertThrows(IOException.class, fixture::publish);
        fixture.close();

        PathStateLayerPublication recovery = new PathStateLayerPublication(fixture.manifest);
        assertEquals(RecoveryAction.COMPLETED_PUBLICATION, recovery.recover());
        assertEquals(RecoveryAction.NONE, recovery.recover());
        assertSettled(fixture);
      }
    }
  }

  @Test
  public void faultAfterRetireIsAlreadySettled() throws Exception {
    Fixture fixture = fixture("after-retire", Engine.ROCKSDB, Stage.AFTER_RETIRE);
    assertThrows(IOException.class, fixture::publish);
    fixture.close();

    PathStateLayerPublication recovery = new PathStateLayerPublication(fixture.manifest);
    assertEquals(RecoveryAction.NONE, recovery.recover());
    assertSettled(fixture);
  }

  @Test
  public void recoveryRejectsNativeProgressWithoutIntentOrMetadata() throws Exception {
    Fixture fixture = fixture("orphan-progress", Engine.ROCKSDB, Stage.AFTER_NODE_PROGRESS);
    assertThrows(IOException.class, fixture::publish);
    fixture.close();
    Files.delete(fixture.intentPath());

    assertThrows(IOException.class,
        new PathStateLayerPublication(fixture.manifest)::recover);
  }

  @Test
  public void staleForkIntentCannotAdvanceCurrent() throws Exception {
    Fixture stale = fixture("stale-intent", Engine.ROCKSDB, Stage.AFTER_INTENT);
    assertThrows(IOException.class, stale::publish);
    stale.close();

    try (PathStateLayer canonical = PathStateLayer.begin(stale.manifest, stale.base, 101,
        bytes(21), stale.base.getBlockHash(), 303, P66Phase.P66_ON, bytes(22))) {
      canonical.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{7})));
      canonical.commit();
    }

    assertThrows(IOException.class, new PathStateLayerPublication(stale.manifest)::recover);
    assertArrayEquals(bytes(21),
        new PathStateCurrentStore(stale.manifest).current().getBlockHash());
  }

  private Fixture fixture(String name, Engine engine, Stage failure) throws Exception {
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(
        new File(temporaryFolder.getRoot(), name).toPath(), engine);
    PathStateRootMetadata base = publishBase(manifest);
    PathStateLayer layer = PathStateLayer.begin(manifest, base, 101, bytes(11),
        base.getBlockHash(), 303, P66Phase.P66_ON, bytes(12), stage -> {
          if (stage == failure) {
            throw new IOException("injected after " + stage);
          }
        });
    layer.apply(Arrays.asList(
        PathStateMutation.put("proposal", new byte[]{1}, new byte[]{5}),
        PathStateMutation.delete("account", new byte[]{3})));
    return new Fixture(manifest, base, layer, layer.rootHash());
  }

  private static PathStateRootMetadata publishBase(PathStateStoreManifest manifest)
      throws Exception {
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      PathStateRootMetadata base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), root.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
      return base;
    }
  }

  private static void assertSettled(Fixture fixture) throws Exception {
    PathStateRootMetadata current = new PathStateCurrentStore(fixture.manifest).current();
    assertEquals(101, current.getBlockNumber());
    assertArrayEquals(fixture.expectedRoot, current.getStateRoot());
    Path layerDirectory = fixture.manifest.getLayerDirectory(101, current.getBlockHash());
    assertArrayEquals(current.encode(),
        PathStateNodeStoreSet.loadProgress(layerDirectory, fixture.manifest).encode());
    assertFalse(Files.exists(layerDirectory.resolve(PathStateLayerPublication.INTENT_FILE)));
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openCurrent(fixture.manifest)) {
      PathStateRoot restored = stores.createRoot();
      assertArrayEquals(fixture.expectedRoot, restored.rootHash());
      restored.verifyNodeStores();
    }
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
    private final PathStateLayer layer;
    private final byte[] expectedRoot;

    private Fixture(PathStateStoreManifest manifest, PathStateRootMetadata base,
        PathStateLayer layer, byte[] expectedRoot) {
      this.manifest = manifest;
      this.base = base;
      this.layer = layer;
      this.expectedRoot = Arrays.copyOf(expectedRoot, expectedRoot.length);
    }

    private void publish() throws IOException {
      layer.commit();
    }

    private void close() throws IOException {
      layer.close();
    }

    private Path intentPath() {
      return manifest.getLayerDirectory(101, bytes(11))
          .resolve(PathStateLayerPublication.INTENT_FILE);
    }
  }
}
