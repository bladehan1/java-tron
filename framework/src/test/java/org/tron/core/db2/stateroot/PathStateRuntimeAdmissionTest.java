package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

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

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }
}
