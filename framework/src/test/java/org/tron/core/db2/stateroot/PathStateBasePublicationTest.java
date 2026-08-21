package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateBasePublication.RecoveryAction;
import org.tron.core.db2.stateroot.PathStateBasePublication.Stage;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateBasePublicationTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void faultsBeforeNativeMarkerRollBackIntentIdempotently() throws Exception {
    for (Engine engine : availableEngines()) {
      Fixture fixture = fixture("before-marker-" + engine, engine, Stage.AFTER_INTENT);
      assertThrows(IOException.class, fixture::publish);
      fixture.close();

      PathStateBasePublication recovery = new PathStateBasePublication(fixture.manifest);
      org.junit.Assert.assertEquals(RecoveryAction.ROLLED_BACK_INTENT, recovery.recover());
      org.junit.Assert.assertEquals(RecoveryAction.NONE, recovery.recover());
      assertFalse(new PathStateCurrentStore(fixture.manifest).isInitialized());
      assertNull(PathStateNodeStoreSet.loadProgress(fixture.manifest.getBaseDirectory(),
          fixture.manifest));
    }
  }

  @Test
  public void faultsAfterNativeMarkerCompletePublicationIdempotently() throws Exception {
    for (Engine engine : availableEngines()) {
      for (Stage stage : new Stage[]{Stage.AFTER_NODE_PROGRESS, Stage.AFTER_METADATA,
          Stage.AFTER_CURRENT}) {
        Fixture fixture = fixture("complete-" + engine + "-" + stage, engine, stage);
        assertThrows(IOException.class, fixture::publish);
        fixture.close();

        PathStateBasePublication recovery = new PathStateBasePublication(fixture.manifest);
        org.junit.Assert.assertEquals(RecoveryAction.COMPLETED_PUBLICATION, recovery.recover());
        org.junit.Assert.assertEquals(RecoveryAction.NONE, recovery.recover());
        assertSettled(fixture);
      }
    }
  }

  @Test
  public void faultAfterRetireIsAlreadySettled() throws Exception {
    Fixture fixture = fixture("after-retire", Engine.ROCKSDB, Stage.AFTER_RETIRE);
    assertThrows(IOException.class, fixture::publish);
    fixture.close();

    PathStateBasePublication recovery = new PathStateBasePublication(fixture.manifest);
    org.junit.Assert.assertEquals(RecoveryAction.NONE, recovery.recover());
    assertSettled(fixture);
  }

  @Test
  public void recoveryRejectsNativeProgressWithoutIntentOrAuthority() throws Exception {
    Fixture fixture = fixture("orphan-progress", Engine.ROCKSDB, Stage.AFTER_NODE_PROGRESS);
    assertThrows(IOException.class, fixture::publish);
    fixture.close();
    Files.delete(fixture.manifest.getBaseDirectory().resolve(PathStateBasePublication.INTENT_FILE));

    assertThrows(IOException.class, new PathStateBasePublication(fixture.manifest)::recover);
  }

  @Test
  public void publicationRejectsNodeSetFromAnotherDirectoryBeforeIntent() throws Exception {
    Fixture source = fixture("directory-source", Engine.ROCKSDB, null);
    PathStateStoreManifest target = PathStateStoreManifest.createOrOpen(
        temporaryFolder.newFolder("directory-target").toPath(), Engine.ROCKSDB);
    PathStateRootMetadata targetMetadata = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
        P66Phase.P66_ON, target.getIdentityDigest(), source.metadata.getStateRoot(), bytes(3));

    assertThrows(IllegalArgumentException.class,
        () -> new PathStateBasePublication(target).publish(source.stores, targetMetadata));
    assertFalse(Files.exists(
        target.getBaseDirectory().resolve(PathStateBasePublication.INTENT_FILE)));
    source.close();
  }

  private Fixture fixture(String name, Engine engine, Stage failure) throws Exception {
    Path root = new File(temporaryFolder.getRoot(), name).toPath();
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(root, engine);
    PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest);
    PathStateRoot stateRoot = stores.createRoot();
    stateRoot.apply(Collections.singletonList(
        PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2})));
    PathStateRootMetadata metadata = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
        P66Phase.P66_ON, manifest.getIdentityDigest(), stateRoot.rootHash(), bytes(3));
    PathStateBasePublication publication = new PathStateBasePublication(manifest, stage -> {
      if (stage == failure) {
        throw new IOException("injected after " + stage);
      }
    });
    return new Fixture(manifest, stores, metadata, publication);
  }

  private static void assertSettled(Fixture fixture) throws Exception {
    PathStateRootMetadata current = new PathStateCurrentStore(fixture.manifest).current();
    assertArrayEquals(fixture.metadata.encode(), current.encode());
    assertArrayEquals(fixture.metadata.encode(), PathStateNodeStoreSet.loadProgress(
        fixture.manifest.getBaseDirectory(), fixture.manifest).encode());
    assertFalse(Files.exists(
        fixture.manifest.getBaseDirectory().resolve(PathStateBasePublication.INTENT_FILE)));
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
    private final PathStateNodeStoreSet stores;
    private final PathStateRootMetadata metadata;
    private final PathStateBasePublication publication;

    private Fixture(PathStateStoreManifest manifest, PathStateNodeStoreSet stores,
        PathStateRootMetadata metadata, PathStateBasePublication publication) {
      this.manifest = manifest;
      this.stores = stores;
      this.metadata = metadata;
      this.publication = publication;
    }

    private void publish() throws IOException {
      publication.publish(stores, metadata);
    }

    private void close() throws IOException {
      stores.close();
    }
  }
}
