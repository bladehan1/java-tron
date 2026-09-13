package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRuntimeAdmission.Status;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateRuntimeAdmissionTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void disabledAndMissingEnabledAdmissionDoNotCreateStorage() throws Exception {
    Path disabled = temporaryFolder.getRoot().toPath().resolve("disabled");
    assertSame(Status.DISABLED,
        PathStateRuntimeAdmission.inspect(false, null, null).getStatus());
    assertFalse(Files.exists(disabled));

    Path missing = temporaryFolder.getRoot().toPath().resolve("missing");
    PathStateRuntimeAdmission.Result result = PathStateRuntimeAdmission.inspect(
        true, missing, Engine.ROCKSDB);
    assertSame(Status.REBUILD_REQUIRED, result.getStatus());
    assertNull(result.getManifest());
    assertFalse(Files.exists(missing));
  }

  @Test
  public void enabledAdmissionDistinguishesRebuildFromCurrentReady() throws Exception {
    Path root = temporaryFolder.getRoot().toPath().resolve("enabled");
    PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(root, Engine.ROCKSDB);
    PathStateRuntimeAdmission.Result empty = PathStateRuntimeAdmission.inspect(
        true, root, Engine.ROCKSDB);
    assertSame(Status.REBUILD_REQUIRED, empty.getStatus());
    assertArrayEquals(manifest.getIdentityDigest(), empty.getManifest().getIdentityDigest());

    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot state = stores.createRoot();
      PathStateRootMetadata base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), state.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
    }

    PathStateRuntimeAdmission.Result ready = PathStateRuntimeAdmission.inspect(
        true, root, Engine.ROCKSDB);
    assertSame(Status.CURRENT_READY, ready.getStatus());
    assertArrayEquals(manifest.getIdentityDigest(), ready.getManifest().getIdentityDigest());
  }

  @Test
  public void physicalAdmissionIsNonCreatingRejectsLegacyAndBindsCurrentMetadata()
      throws Exception {
    assertSame(PathStatePhysicalRuntimeAdmission.Status.DISABLED,
        PathStatePhysicalRuntimeAdmission.inspect(false, null, null).getStatus());

    Path missing = temporaryFolder.getRoot().toPath().resolve("physical-missing");
    assertSame(PathStatePhysicalRuntimeAdmission.Status.REBUILD_REQUIRED,
        PathStatePhysicalRuntimeAdmission.inspect(true, missing, Engine.ROCKSDB).getStatus());
    assertFalse(Files.exists(missing));

    Path legacy = temporaryFolder.getRoot().toPath().resolve("physical-legacy");
    PathStateStoreManifest.createOrOpen(legacy, Engine.ROCKSDB);
    assertThrows(java.io.IOException.class,
        () -> PathStatePhysicalRuntimeAdmission.inspect(true, legacy, Engine.ROCKSDB));

    Path physical = temporaryFolder.getRoot().toPath().resolve("physical-ready");
    PathStateRootMetadata expected;
    try (PathStatePhysicalStoreSet stores = PathStatePhysicalStoreSet.open(physical,
        new PathStateCanonicalizer().participantScope(), Engine.ROCKSDB)) {
      PathStateRoot root = stores.buildRootFromFlat();
      expected = PathStateRootMetadata.base(100, bytes(4), bytes(5), 300,
          P66Phase.P66_ON, stores.getFormatDigest(), root.rootHash(), bytes(6));
      stores.publishCurrent(expected);
    }
    assertSame(PathStatePhysicalRuntimeAdmission.Status.CURRENT_CANDIDATE,
        PathStatePhysicalRuntimeAdmission.inspect(true, physical, Engine.ROCKSDB).getStatus());
    try (PathStatePhysicalSnapshotHead head = PathStatePhysicalSnapshotHead.open(
        physical, Engine.ROCKSDB)) {
      assertArrayEquals(expected.encode(), head.getHead().encode());
    }
    assertFalse(Files.exists(physical.resolve("base").resolve("nodes")));
    assertFalse(Files.exists(physical.resolve("rebuild-spool")));
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }
}
