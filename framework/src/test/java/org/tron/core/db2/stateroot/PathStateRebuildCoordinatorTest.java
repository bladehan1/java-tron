package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.EntryConsumer;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.RebuildResult;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotSource;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateRebuildCoordinatorTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void rebuildsAndPublishesExactSnapshotAcrossNativeEngines() throws Exception {
    byte[] expectedRoot = null;
    byte[] expectedSourceDigest = null;
    for (Engine engine : availableEngines()) {
      PathStateStoreManifest manifest = manifest("rebuild-" + engine, engine);
      TestSnapshotSource source = exactSource(identity());
      source.add("proposal", new byte[]{1}, new byte[]{11});
      source.add("proposal", new byte[]{2}, new byte[]{22});
      source.add("abi", address(1), new byte[0]);

      RebuildResult result = new PathStateRebuildCoordinator().rebuild(manifest, source);

      assertEquals(27, result.getStores().size());
      assertEquals(3, result.getTotalEntries());
      assertEquals(2, result.requireStore("proposal").getEntryCount());
      assertEquals(1, result.requireStore("abi").getEntryCount());
      assertEquals(0, result.requireStore("account").getEntryCount());
      assertArrayEquals(result.getSourceDigest(), result.getMetadata().getPayloadDigest());
      assertTrue(source.getVerificationCount() >= 2);

      PathStateRootMetadata current = new PathStateCurrentStore(manifest).current();
      assertArrayEquals(result.getMetadata().encode(), current.encode());
      try (PathStateNodeStoreSet reopened = PathStateNodeStoreSet.openCurrent(manifest)) {
        PathStateRoot restored = reopened.createRoot();
        assertArrayEquals(result.getMetadata().getStateRoot(), restored.rootHash());
        restored.verifyNodeStores();
      }

      if (expectedRoot == null) {
        expectedRoot = result.getMetadata().getStateRoot();
        expectedSourceDigest = result.getSourceDigest();
      } else {
        assertArrayEquals(expectedRoot, result.getMetadata().getStateRoot());
        assertArrayEquals(expectedSourceDigest, result.getSourceDigest());
      }
      assertThrows(IOException.class,
          () -> new PathStateRebuildCoordinator().rebuild(manifest, source));
    }
  }

  @Test
  public void rejectsScopeMismatchBeforeOpeningBaseNodes() throws Exception {
    PathStateStoreManifest manifest = manifest("scope-mismatch", Engine.ROCKSDB);
    TestSnapshotSource source = exactSource(identity());
    source.removeDatabase("abi");

    assertThrows(IllegalArgumentException.class,
        () -> new PathStateRebuildCoordinator().rebuild(manifest, source));
    assertFalse(new PathStateCurrentStore(manifest).isInitialized());
    assertFalse(Files.exists(
        manifest.getBaseDirectory().resolve(PathStateNodeStoreSet.NODES_DIRECTORY)));
  }

  @Test
  public void rejectsDuplicateOrOutOfOrderPhysicalKeysWithoutPublication() throws Exception {
    for (byte[][] keys : new byte[][][]{
        {new byte[]{2}, new byte[]{1}},
        {new byte[]{1}, new byte[]{1}}
    }) {
      PathStateStoreManifest manifest = manifest("order-" + keys[0][0] + "-" + keys[1][0],
          Engine.ROCKSDB);
      TestSnapshotSource source = exactSource(identity());
      source.add("proposal", keys[0], new byte[]{1});
      source.add("proposal", keys[1], new byte[]{2});

      assertThrows(IllegalArgumentException.class,
          () -> new PathStateRebuildCoordinator().rebuild(manifest, source));
      assertFalse(new PathStateCurrentStore(manifest).isInitialized());
      assertFalse(Files.exists(manifest.getBaseDirectory()
          .resolve(PathStateCurrentStore.METADATA_FILE)));
    }
  }

  @Test
  public void rejectsSnapshotIdentityDriftBeforePublishingBase() throws Exception {
    PathStateStoreManifest manifest = manifest("identity-drift", Engine.ROCKSDB);
    TestSnapshotSource source = exactSource(identity());
    source.add("proposal", new byte[]{1}, new byte[]{2});
    source.driftAfterFirstVerification();

    IOException failure = assertThrows(IOException.class,
        () -> new PathStateRebuildCoordinator().rebuild(manifest, source));
    assertTrue(failure.getMessage().contains("identity changed"));
    assertFalse(new PathStateCurrentStore(manifest).isInitialized());
    assertFalse(Files.exists(manifest.getBaseDirectory()
        .resolve(PathStateCurrentStore.METADATA_FILE)));
  }

  private PathStateStoreManifest manifest(String name, Engine engine) throws IOException {
    Path directory = temporaryFolder.newFolder(name).toPath();
    return PathStateStoreManifest.createOrOpen(directory, engine);
  }

  private static TestSnapshotSource exactSource(SnapshotIdentity identity) {
    LinkedHashMap<String, List<Row>> stores = new LinkedHashMap<>();
    for (PathStateParticipantDescriptor.StoreIdentity store
        : PathStateParticipantDescriptor.current().getStores()) {
      stores.put(store.getDbName(), new ArrayList<>());
    }
    return new TestSnapshotSource(identity, stores);
  }

  private static SnapshotIdentity identity() {
    return new SnapshotIdentity(100, bytes(1), bytes(2), 300, P66Phase.P66_ON);
  }

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB}
        : new Engine[]{Engine.LEVELDB, Engine.ROCKSDB};
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static final class TestSnapshotSource implements SnapshotSource {

    private final SnapshotIdentity identity;
    private final Map<String, List<Row>> stores;
    private int verificationCount;
    private boolean drift;

    private TestSnapshotSource(SnapshotIdentity identity, Map<String, List<Row>> stores) {
      this.identity = identity;
      this.stores = stores;
    }

    private void add(String dbName, byte[] key, byte[] value) {
      stores.get(dbName).add(new Row(key, value));
    }

    private void removeDatabase(String dbName) {
      stores.remove(dbName);
    }

    private void driftAfterFirstVerification() {
      drift = true;
    }

    private int getVerificationCount() {
      return verificationCount;
    }

    @Override
    public SnapshotIdentity identity() {
      return identity;
    }

    @Override
    public Collection<String> databases() {
      List<String> names = new ArrayList<>(stores.keySet());
      Collections.reverse(names);
      return names;
    }

    @Override
    public void scan(String dbName, EntryConsumer consumer) throws IOException {
      for (Row row : stores.get(dbName)) {
        consumer.accept(row.key, row.value);
      }
    }

    @Override
    public void verifyIdentity(SnapshotIdentity expected) throws IOException {
      verificationCount++;
      if (!identity.sameAs(expected) || drift && verificationCount > 1) {
        throw new IOException("path-state snapshot identity changed during rebuild");
      }
    }
  }

  private static final class Row {

    private final byte[] key;
    private final byte[] value;

    private Row(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }
}
