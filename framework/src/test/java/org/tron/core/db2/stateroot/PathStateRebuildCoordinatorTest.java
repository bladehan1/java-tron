package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.EntryConsumer;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.RebuildResult;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotSource;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.StoreResult;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.protos.Protocol.Account;

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

  @Test
  public void admitsOnlyTargetP66AccountAssetPhysicalLayout() throws Exception {
    byte[] address = address(7);
    String tokenId = "1000001";

    PathStateStoreManifest offManifest = manifest("p66-off", Engine.ROCKSDB);
    TestSnapshotSource off = exactSource(identity(P66Phase.P66_OFF));
    byte[] embedded = account(address).toBuilder().putAssetV2(tokenId, 11L).build().toByteArray();
    off.add("account", address, embedded);
    RebuildResult offResult = new PathStateRebuildCoordinator().rebuild(offManifest, off);
    assertEquals(1, offResult.requireStore("account").getEntryCount());
    assertEquals(0, offResult.requireStore("account-asset").getEntryCount());

    PathStateStoreManifest onManifest = manifest("p66-on", Engine.ROCKSDB);
    TestSnapshotSource on = exactSource(identity(P66Phase.P66_ON));
    byte[] optimized = account(address).toBuilder().setAssetOptimized(true).build().toByteArray();
    on.add("account", address, optimized);
    on.add("account-asset", accountAssetKey(address, tokenId), longBytes(11L));
    RebuildResult onResult = new PathStateRebuildCoordinator().rebuild(onManifest, on);
    assertEquals(1, onResult.requireStore("account").getEntryCount());
    assertEquals(1, onResult.requireStore("account-asset").getEntryCount());
  }

  @Test
  public void rejectsMixedOrOrphanAccountAssetSnapshotWithoutPublication() throws Exception {
    byte[] address = address(8);
    String tokenId = "1000001";

    PathStateStoreManifest mixedManifest = manifest("p66-mixed", Engine.ROCKSDB);
    TestSnapshotSource mixed = exactSource(identity(P66Phase.P66_ON));
    mixed.add("account", address,
        account(address).toBuilder().putAssetV2(tokenId, 9L).build().toByteArray());
    mixed.add("account-asset", accountAssetKey(address, tokenId), longBytes(9L));
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateRebuildCoordinator().rebuild(mixedManifest, mixed));
    assertFalse(new PathStateCurrentStore(mixedManifest).isInitialized());

    PathStateStoreManifest orphanManifest = manifest("p66-orphan", Engine.ROCKSDB);
    TestSnapshotSource orphan = exactSource(identity(P66Phase.P66_ON));
    orphan.add("account-asset", accountAssetKey(address, tokenId), longBytes(10L));
    IOException orphanFailure = assertThrows(IOException.class,
        () -> new PathStateRebuildCoordinator().rebuild(orphanManifest, orphan));
    assertTrue(orphanFailure.getMessage().contains("no owning Account"));
    assertFalse(new PathStateCurrentStore(orphanManifest).isInitialized());

    PathStateStoreManifest offDirectManifest = manifest("p66-off-direct", Engine.ROCKSDB);
    TestSnapshotSource offDirect = exactSource(identity(P66Phase.P66_OFF));
    offDirect.add("account", address,
        account(address).toBuilder().putAssetV2(tokenId, 10L).build().toByteArray());
    offDirect.add("account-asset", accountAssetKey(address, tokenId), longBytes(10L));
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateRebuildCoordinator().rebuild(offDirectManifest, offDirect));
    assertFalse(new PathStateCurrentStore(offDirectManifest).isInitialized());
  }

  @Test
  public void resumesCompletedStoresWhileKeepingGenerationInvisible() throws Exception {
    PathStateStoreManifest manifest = manifest("resume", Engine.ROCKSDB);
    TestSnapshotSource first = exactSource(identity());
    first.add("proposal", new byte[]{1}, new byte[]{2});
    AtomicBoolean failed = new AtomicBoolean();
    PathStateRebuildCoordinator interrupted = new PathStateRebuildCoordinator(store -> {
      if ("account".equals(store.getDbName()) && failed.compareAndSet(false, true)) {
        throw new IOException("injected rebuild interruption");
      }
    });

    assertThrows(IOException.class, () -> interrupted.rebuild(manifest, first));
    assertFalse(new PathStateCurrentStore(manifest).isInitialized());
    assertFalse(Files.exists(manifest.getBaseDirectory()
        .resolve(PathStateCurrentStore.METADATA_FILE)));
    assertNull(PathStateNodeStoreSet.loadProgress(manifest.getBaseDirectory(), manifest));

    TestSnapshotSource resumed = exactSource(identity());
    resumed.add("proposal", new byte[]{1}, new byte[]{2});
    RebuildResult result = new PathStateRebuildCoordinator().rebuild(manifest, resumed);

    assertEquals(27, result.getStores().size());
    assertEquals(0, resumed.getScanCount("abi"));
    assertEquals(0, resumed.getScanCount("accountid-index"));
    assertEquals(0, resumed.getScanCount("account-index"));
    assertEquals(0, resumed.getScanCount("account"));
    assertEquals(1, resumed.getScanCount("account-asset"));
    assertEquals(1, resumed.getScanCount("proposal"));
    assertTrue(new PathStateCurrentStore(manifest).isInitialized());

    PathStateStoreManifest freshManifest = manifest("resume-fresh", Engine.ROCKSDB);
    TestSnapshotSource fresh = exactSource(identity());
    fresh.add("proposal", new byte[]{1}, new byte[]{2});
    RebuildResult freshResult = new PathStateRebuildCoordinator().rebuild(freshManifest, fresh);
    assertArrayEquals(freshResult.getMetadata().getStateRoot(),
        result.getMetadata().getStateRoot());
    assertArrayEquals(freshResult.getSourceDigest(), result.getSourceDigest());
    try (PathStateNodeStoreSet reopened = PathStateNodeStoreSet.openCurrent(manifest)) {
      assertNull(reopened.getRebuildCheckpoint());
      assertArrayEquals(result.getMetadata().getStateRoot(), reopened.createRoot().rootHash());
    }
  }

  @Test
  public void rejectsResumeAgainstAnotherSnapshotIdentity() throws Exception {
    PathStateStoreManifest manifest = manifest("resume-identity", Engine.ROCKSDB);
    TestSnapshotSource first = exactSource(identity());
    PathStateRebuildCoordinator interrupted = new PathStateRebuildCoordinator(store -> {
      throw new IOException("stop after first Store");
    });
    assertThrows(IOException.class, () -> interrupted.rebuild(manifest, first));

    SnapshotIdentity other = new SnapshotIdentity(101, bytes(3), bytes(4), 301,
        P66Phase.P66_ON);
    IOException failure = assertThrows(IOException.class,
        () -> new PathStateRebuildCoordinator().rebuild(manifest, exactSource(other)));
    assertTrue(failure.getMessage().contains("checkpoint snapshot identity mismatch"));
    assertFalse(new PathStateCurrentStore(manifest).isInitialized());
  }

  @Test
  public void rejectsResumeAgainstReplacedPhysicalSources() throws Exception {
    PathStateStoreManifest manifest = manifest("resume-source", Engine.ROCKSDB);
    TestSnapshotSource first = exactSource(identity());
    PathStateRebuildCoordinator interrupted = new PathStateRebuildCoordinator(store -> {
      throw new IOException("stop after first Store");
    });
    assertThrows(IOException.class, () -> interrupted.rebuild(manifest, first));

    TestSnapshotSource replacement = exactSource(identity());
    replacement.setSourceIdentityDigest(bytes(43));
    IOException failure = assertThrows(IOException.class,
        () -> new PathStateRebuildCoordinator().rebuild(manifest, replacement));
    assertTrue(failure.getMessage().contains("checkpoint source identity mismatch"));
    assertFalse(new PathStateCurrentStore(manifest).isInitialized());
  }

  @Test
  public void rebuildCheckpointCodecRejectsCorruptionAndNonPrefixStores() throws Exception {
    StoreResult abi = StoreResult.restore(1, "abi", 2, bytes(5), bytes(6));
    PathStateRebuildCheckpoint checkpoint = new PathStateRebuildCheckpoint(bytes(7), bytes(11),
        identity(), Collections.singletonList(abi), bytes(8));
    PathStateRebuildCheckpoint decoded = PathStateRebuildCheckpoint.decode(checkpoint.encode());
    assertArrayEquals(checkpoint.getManifestDigest(), decoded.getManifestDigest());
    assertArrayEquals(checkpoint.getSourceIdentityDigest(), decoded.getSourceIdentityDigest());
    assertTrue(checkpoint.getIdentity().sameAs(decoded.getIdentity()));
    assertEquals(1, decoded.getCompletedStores().size());
    assertArrayEquals(checkpoint.getPartialRoot(), decoded.getPartialRoot());

    byte[] corrupt = checkpoint.encode();
    corrupt[corrupt.length - 1] ^= 1;
    assertThrows(IOException.class, () -> PathStateRebuildCheckpoint.decode(corrupt));
    StoreResult wrongFirst = StoreResult.restore(2, "accountid-index", 0, bytes(9), bytes(10));
    assertThrows(IllegalArgumentException.class, () -> new PathStateRebuildCheckpoint(bytes(7),
        bytes(11), identity(), Collections.singletonList(wrongFirst), bytes(8)));
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
    return identity(P66Phase.P66_ON);
  }

  private static SnapshotIdentity identity(P66Phase phase) {
    return new SnapshotIdentity(100, bytes(1), bytes(2), 300, phase);
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

  private static Account account(byte[] address) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address)).build();
  }

  private static byte[] accountAssetKey(byte[] address, String tokenId) {
    byte[] token = tokenId.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    return ByteBuffer.allocate(address.length + token.length).put(address).put(token).array();
  }

  private static byte[] longBytes(long value) {
    return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
  }

  private static final class TestSnapshotSource implements SnapshotSource {

    private final SnapshotIdentity identity;
    private final Map<String, List<Row>> stores;
    private final Map<String, Integer> scanCounts = new LinkedHashMap<>();
    private byte[] sourceIdentityDigest = bytes(42);
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

    private int getScanCount(String dbName) {
      Integer count = scanCounts.get(dbName);
      return count == null ? 0 : count;
    }

    private void setSourceIdentityDigest(byte[] digest) {
      sourceIdentityDigest = java.util.Arrays.copyOf(digest, digest.length);
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
    public byte[] sourceIdentityDigest() {
      return java.util.Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
    }

    @Override
    public byte[] get(String dbName, byte[] physicalKey) {
      for (Row row : stores.get(dbName)) {
        if (java.util.Arrays.equals(row.key, physicalKey)) {
          return java.util.Arrays.copyOf(row.value, row.value.length);
        }
      }
      return null;
    }

    @Override
    public void scan(String dbName, EntryConsumer consumer) throws IOException {
      scanCounts.put(dbName, getScanCount(dbName) + 1);
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
