package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.ArchiveReadContext.HistoricalStore;
import org.tron.core.db2.archive.ArchiveReadContext.StoreAdapter;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

public class ArchiveReadSnapshotTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void readsPointAndRangeFromOnePinnedPhysicalKeyGeneration() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("snapshot").toPath())) {
      fixture.append(diff(1, entry("p/c", OldValue.absent())));
      fixture.append(diff(2,
          entry("p/a", "a1"), entry("p/b", "b1"), entry("p/e", OldValue.absent()),
          entry("p/f", OldValue.present(new byte[0]))));
      fixture.append(diff(3, entry("p/a", "a2")));
      fixture.sync();

      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put("p/a", bytes("a3"));
      latestValues.put("p/c", bytes("c1"));
      latestValues.put("p/e", bytes("e2"));
      InMemoryLatest latest = new InMemoryLatest(3, hash(3), latestValues);
      try (ArchiveReadSnapshot snapshot = fixture.snapshot(1, latest)) {
        assertValue(snapshot.get("account", bytes("p/a")), "a1");
        assertValue(snapshot.get("account", bytes("p/b")), "b1");
        assertValue(snapshot.get("account", bytes("p/c")), "c1");
        assertFalse(snapshot.get("account", bytes("p/e")).isPresent());
        assertArrayEquals(new byte[0],
            snapshot.get("account", bytes("p/f")).getValue());

        List<HistoricalRangeOverlay.Entry> range = snapshot.range("account",
            KeyRange.prefix(bytes("p/")), new Limits(10, 10, 10));
        assertEquals(Arrays.asList("p/a", "p/b", "p/c", "p/f"), keys(range));
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void rejectsMixedGenerationOrCoverageBeforeReading() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("identity").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      ServingKeyIndexGeneration serving = fixture.serving();
      CommittedHistoryReader history = fixture.history();
      InMemoryLatest wrongHash = new InMemoryLatest(1, hash(99), Collections.emptyMap());

      assertThrows(IllegalArgumentException.class, () -> ArchiveReadSnapshot.pin(
          0, 1, hash(1), serving, wrongHash, history));
      assertTrue(wrongHash.closed);

      CommittedHistoryReader secondHistory = fixture.history();
      InMemoryLatest wrongCoverage = new InMemoryLatest(0, hash(0), Collections.emptyMap());
      assertThrows(IllegalArgumentException.class, () -> ArchiveReadSnapshot.pin(
          0, 0, hash(0), serving, wrongCoverage, secondHistory));
      assertTrue(wrongCoverage.closed);
    }
  }

  @Test
  public void rejectsWrongKeyBetweenAuthoritativeIndexAndBody() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("wrong-key").toPath())) {
      BlockReverseDiff body = diff(1, entry("actual", "old"));
      HistoryLocation location = fixture.bodies.append(body);
      HistoryIndexRecord wrongIndex = new HistoryIndexRecord(body.getMeta(), location,
          Collections.singletonList(new KeyGroup("account",
              Collections.singletonList(bytes("indexed")))));
      HistoryIndexLocation indexLocation = fixture.index.append(wrongIndex);
      fixture.markers.add(marker(body.getMeta(), location, indexLocation));
      fixture.sync();

      try (ArchiveReadSnapshot snapshot = fixture.snapshot(0,
          new InMemoryLatest(1, hash(1), Collections.emptyMap()))) {
        assertThrows(ArchivePersistenceException.class,
            () -> snapshot.get("account", bytes("indexed")));
      }
    }
  }

  @Test
  public void rejectsDigestMismatchAndMissingSegment() throws Exception {
    assertUnreadableBody("digest", location -> new HistoryLocation(location.getSegmentId(),
        location.getOffset(), location.getRecordLength(), location.getBodyChecksum(), hash(99)),
        IllegalArgumentException.class);
    assertUnreadableBody("missing", location -> new HistoryLocation(99, location.getOffset(),
        location.getRecordLength(), location.getBodyChecksum(), location.getBodyDigest()),
        IOException.class);
  }

  @Test
  public void releasesHistoryPinWhenCommittedPrefixValidationFails() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("failed-history-pin").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      AtomicBoolean released = new AtomicBoolean();
      ServingKeyIndexGeneration.AuthoritativeIndexReader failingIndex = location -> {
        throw new IOException("injected index failure");
      };

      assertThrows(IOException.class, () -> new CommittedHistoryReader(0, hash(0),
          fixture.markers, failingIndex, fixture.bodies::read, () -> released.set(true)));
      assertTrue(released.get());
    }
  }

  @Test
  public void bindsEveryVersionedPhysicalStoreToOneRequestSnapshot() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("read-context").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put("key", bytes("new"));
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), latestValues);
      AdapterSet adapterSet = rawAdapters();

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapterSet.adapters)) {
        HistoricalStore<byte[]> account = context.store(adapterSet.account);
        assertArrayEquals(bytes("old"), account.get(bytes("key")).orElseThrow(AssertionError::new));
        assertFalse(account.has(bytes("missing")));
        assertEquals(0, context.getTargetBlock());
        assertEquals(1, context.getPinnedBlock());
        assertTrue(context.getAdapterDbNames().contains("account-asset"));
        assertFalse(context.getAdapterDbNames().contains("accountTrie"));
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void contextResolvesHistoricalAccountAssetBeforePinnedHead() throws Exception {
    byte[] address = address(41);
    String tokenId = "1000001";
    byte[] account = account(address, false, tokenId, 17L);
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, tokenId);
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-account-asset-context").toPath())) {
      fixture.append(diff(1,
          new DbGroup("properties", Collections.singletonList(new Entry(
              HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey(),
              OldValue.present(ByteArray.fromLong(0L))))),
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(account)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.absent())))));
      fixture.sync();
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), Collections.emptyMap());

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), rawAdapters().adapters)) {
        HistoricalAccountAssetBalanceResolver.Result result =
            context.resolveAccountAsset(address, tokenId);
        assertEquals(0L, result.getBlockNumber());
        assertEquals(Phase.P66_OFF, result.getPhase());
        assertEquals(17L, result.getBalance());
        assertArrayEquals(account, result.getAccountValue());
        byte[] callerCopy = result.getAccountValue();
        callerCopy[0] ^= 1;
        assertArrayEquals(account, result.getAccountValue());

        HistoricalAccountAssetPrefixResolver.Result all = context.resolveAccountAssets(address,
            new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000));
        assertEquals(1, all.getBalances().size());
        assertEquals(tokenId, all.getBalances().get(0).getTokenId());
        assertEquals(17L, all.getBalances().get(0).getBalance());
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void resolvesFixedP66TransitionVectorsAcrossCommittedSnapshots() throws Exception {
    byte[] address = address(43);
    String tokenId = "1000007";
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, tokenId);
    byte[] offAtZero = account(address, false, tokenId, 10L);
    byte[] offAtOne = account(address, false, tokenId, 20L);
    byte[] activationAtTwo = optimizedAccount(address, 2_000L);
    byte[] onAtThree = optimizedAccount(address, 3_000L);
    byte[] onAtFour = optimizedAccount(address, 4_000L);
    HistoricalAccountAssetPrefixResolver.Limits limits =
        new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000);

    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("p66-transition-vectors").toPath())) {
      fixture.append(diff(1,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(offAtZero))))));
      BlockReverseDiff activation = diff(2,
          new DbGroup("properties", Collections.singletonList(new Entry(
              HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey(),
              OldValue.present(ByteArray.fromLong(0L))))),
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(offAtOne)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.absent()))));
      fixture.append(activation);
      fixture.append(diff(3,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(activationAtTwo)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.present(ByteArray.fromLong(30L)))))));
      fixture.append(diff(4,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(onAtThree)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.present(ByteArray.fromLong(40L)))))));
      fixture.sync();

      Entry activationReverseAsset = activation.getGroups().stream()
          .filter(group -> "account-asset".equals(group.getDbName()))
          .findFirst().orElseThrow(AssertionError::new).getEntries().get(0);
      assertArrayEquals(directKey, activationReverseAsset.getKey());
      assertFalse(activationReverseAsset.getOldValue().isPresent());

      Map<String, Map<String, byte[]>> latestValues = new HashMap<>();
      latestValues.put("properties", Collections.singletonMap(
          text(HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey()),
          ByteArray.fromLong(1L)));
      latestValues.put("account", Collections.singletonMap(text(address), onAtFour));
      latestValues.put("account-asset", Collections.singletonMap(
          text(directKey), ByteArray.fromLong(50L)));

      assertAccountAssetVector(fixture, latestValues, 1L, Phase.P66_OFF, offAtOne,
          tokenId, 20L, limits);
      // The request API reports the activation target as P66_ON because both use the same
      // canonical direct-row layout; the durable mutation plan retains P66_ACTIVATION.
      assertAccountAssetVector(fixture, latestValues, 2L, Phase.P66_ON, activationAtTwo,
          tokenId, 30L, limits);
      assertAccountAssetVector(fixture, latestValues, 3L, Phase.P66_ON, onAtThree,
          tokenId, 40L, limits);
    }
  }

  @Test
  public void accountAssetContextRejectsForeignAdaptersAndUseAfterClose() throws Exception {
    byte[] address = address(42);
    String tokenId = "1000001";
    Map<String, byte[]> latestValues = new HashMap<>();
    latestValues.put(text(HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey()),
        ByteArray.fromLong(1L));
    latestValues.put(text(address), account(address, true, null, 0L));
    InMemoryLatest latest = new InMemoryLatest(0, hash(0), latestValues);
    AdapterSet adapters = rawAdapters();
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("closed-account-asset-context").toPath())) {
      ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapters.adapters);
      StoreAdapter<byte[]> foreignAccount = StoreAdapter.define("account", value -> value);
      assertThrows(IllegalArgumentException.class, () -> context.store(foreignAccount));
      StoreAdapter<byte[]> foreignAbi = StoreAdapter.define("abi", value -> value);
      assertThrows(IllegalArgumentException.class, () -> context.store(foreignAbi));
      assertEquals(0L, context.resolveAccountAsset(address, tokenId).getBalance());

      context.close();
      assertTrue(latest.closed);
      assertThrows(IllegalStateException.class,
          () -> context.resolveAccountAsset(address, tokenId));
      assertThrows(IllegalStateException.class,
          () -> context.resolveAccountAssets(address,
              new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000)));
    }
  }

  @Test
  public void rejectsIncompleteOrDerivedStoreAdaptersAndReleasesSnapshot() throws Exception {
    assertThrows(IllegalArgumentException.class,
        () -> StoreAdapter.define("accountTrie", value -> value));
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("adapter-set").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), Collections.emptyMap());
      AdapterSet adapterSet = rawAdapters();
      adapterSet.adapters.remove(adapterSet.account);

      assertThrows(IllegalArgumentException.class, () -> ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapterSet.adapters));
      assertTrue(latest.closed);
    }
  }

  @Test
  public void resolvesLogicalStorageWithHistoricalContractFromTheSameContext() throws Exception {
    byte[] address = Hex.decode("410102030405060708090a0b0c0d0e0f1011121314");
    byte[] slot = Hex.decode(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    byte[] transactionHash = Hex.decode(
        "f0e0d0c0b0a090807060504030201000112233445566778899aabbccddeeff00");
    byte[] physicalKey = Hex.decode(
        "9397a7a785754542ff19d0968c0f92d4dea5e526567e92b0321816a4e895bd2d");
    SmartContract historicalContract = SmartContract.newBuilder().setVersion(1)
        .setTrxHash(ByteString.copyFrom(transactionHash)).build();
    SmartContract latestContract = SmartContract.newBuilder().setVersion(0).build();

    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("logical-storage").toPath())) {
      fixture.append(diff(1,
          new DbGroup("contract", Collections.singletonList(
              new Entry(address, OldValue.present(historicalContract.toByteArray())))),
          new DbGroup("storage-row", Collections.singletonList(
              new Entry(physicalKey, OldValue.present(bytes("historical-word")))))));
      fixture.sync();
      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put(text(address), latestContract.toByteArray());
      AdapterSet adapterSet = rawAdapters();

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(1, hash(1), latestValues)),
          adapterSet.adapters)) {
        assertArrayEquals(bytes("historical-word"),
            context.getStorage(address, slot).orElseThrow(AssertionError::new));
      }
    }
  }

  @Test
  public void logicalStorageFailsClosedForMissingCorruptOrClosedContractContext()
      throws Exception {
    byte[] address = Hex.decode("410102030405060708090a0b0c0d0e0f1011121314");
    byte[] slot = new byte[32];
    AdapterSet adapterSet = rawAdapters();
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("missing-contract").toPath())) {
      ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(0, hash(0), Collections.emptyMap())),
          adapterSet.adapters);
      assertThrows(ArchivePersistenceException.class, () -> context.getStorage(address, slot));
      context.close();
      assertThrows(IllegalStateException.class, () -> context.getStorage(address, slot));
    }

    adapterSet = rawAdapters();
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("corrupt-contract").toPath())) {
      fixture.append(diff(1, new DbGroup("contract", Collections.singletonList(
          new Entry(address, OldValue.present(new byte[]{(byte) 0x80}))))));
      fixture.sync();
      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(1, hash(1), Collections.emptyMap())),
          adapterSet.adapters)) {
        assertThrows(ArchivePersistenceException.class, () -> context.getStorage(address, slot));
      }
    }
  }

  private void assertUnreadableBody(String name, LocationMutation mutation,
      Class<? extends Throwable> error) throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder(name).toPath())) {
      BlockReverseDiff body = diff(1, entry("key", "old"));
      HistoryLocation actual = fixture.bodies.append(body);
      HistoryLocation referenced = mutation.apply(actual);
      HistoryIndexLocation indexLocation = fixture.index.append(
          new HistoryIndexRecord(body.getMeta(), referenced,
              Collections.singletonList(new KeyGroup("account",
                  Collections.singletonList(bytes("key"))))));
      fixture.markers.add(marker(body.getMeta(), referenced, indexLocation));
      fixture.sync();

      try (ArchiveReadSnapshot snapshot = fixture.snapshot(0,
          new InMemoryLatest(1, hash(1), Collections.emptyMap()))) {
        assertThrows(error, () -> snapshot.get("account", bytes("key")));
      }
    }
  }

  private static BlockReverseDiff diff(int block, Entry... entries) {
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block),
        hash(block - 1), block * 3_000L), Collections.singletonList(
        new DbGroup("account", Arrays.asList(entries))));
  }

  private static BlockReverseDiff diff(int block, DbGroup... groups) {
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block),
        hash(block - 1), block * 3_000L), Arrays.asList(groups));
  }

  private static Entry entry(String key, String oldValue) {
    return entry(key, OldValue.present(bytes(oldValue)));
  }

  private static Entry entry(String key, OldValue oldValue) {
    return new Entry(bytes(key), oldValue);
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta, HistoryLocation body,
      HistoryIndexLocation index) {
    return new HistoryCommitMarker(meta, meta.getEpoch() - 1, body, index, new byte[16],
        new ArrayList<>(ArchiveStoreScope.getStateDatabases()));
  }

  private static void assertValue(OldValue value, String expected) {
    assertTrue(value.isPresent());
    assertArrayEquals(bytes(expected), value.getValue());
  }

  private static List<String> keys(List<HistoricalRangeOverlay.Entry> entries) {
    List<String> result = new ArrayList<>();
    entries.forEach(entry -> result.add(text(entry.getKey())));
    return result;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] account(byte[] address, boolean optimized, String tokenId, long balance) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(optimized);
    if (tokenId != null) {
      builder.putAsset("asset-name", balance).putAssetV2(tokenId, balance);
    }
    return builder.build().toByteArray();
  }

  private static byte[] optimizedAccount(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(true).setBalance(balance)
        .build().toByteArray();
  }

  private static void assertAccountAssetVector(Fixture fixture,
      Map<String, Map<String, byte[]>> latestValues, long target, Phase expectedPhase,
      byte[] expectedAccount,
      String tokenId, long expectedBalance,
      HistoricalAccountAssetPrefixResolver.Limits limits) throws Exception {
    InMemoryLatest latest = InMemoryLatest.scoped(4, hash(4), latestValues);
    ArchiveReadSnapshot snapshot = fixture.snapshot(target, latest);
    assertArrayEquals(hash(4), snapshot.getPinnedHash());
    snapshot.requirePinnedIdentity();
    try (ArchiveReadContext context = ArchiveReadContext.open(
        snapshot, rawAdapters().adapters)) {
      assertEquals(target, context.getTargetBlock());
      assertEquals(4L, context.getPinnedBlock());
      HistoricalAccountAssetBalanceResolver.Result exact =
          context.resolveAccountAsset(address(43), tokenId);
      assertEquals(expectedPhase, exact.getPhase());
      assertArrayEquals(expectedAccount, exact.getAccountValue());
      assertEquals(expectedBalance, exact.getBalance());

      HistoricalAccountAssetPrefixResolver.Result prefix =
          context.resolveAccountAssets(address(43), limits);
      assertEquals(expectedPhase, prefix.getPhase());
      assertArrayEquals(expectedAccount, prefix.getAccountValue());
      assertEquals(1, prefix.getBalances().size());
      assertEquals(tokenId, prefix.getBalances().get(0).getTokenId());
      assertEquals(expectedBalance, prefix.getBalances().get(0).getBalance());
      snapshot.requirePinnedIdentity();
    }
    assertTrue(latest.closed);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String text(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  private static AdapterSet rawAdapters() {
    List<StoreAdapter<?>> adapters = new ArrayList<>();
    StoreAdapter<byte[]> account = StoreAdapter.define("account",
        value -> Arrays.copyOf(value, value.length));
    for (String dbName : ArchiveStoreScope.getStateDatabases()) {
      adapters.add("account".equals(dbName) ? account : StoreAdapter.define(dbName,
          value -> Arrays.copyOf(value, value.length)));
    }
    return new AdapterSet(account, adapters);
  }

  @FunctionalInterface
  private interface LocationMutation {
    HistoryLocation apply(HistoryLocation location);
  }

  private static final class AdapterSet {
    private final StoreAdapter<byte[]> account;
    private final List<StoreAdapter<?>> adapters;

    private AdapterSet(StoreAdapter<byte[]> account, List<StoreAdapter<?>> adapters) {
      this.account = account;
      this.adapters = adapters;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final HistorySegmentStore bodies;
    private final HistoryIndexStore index;
    private final List<HistoryCommitMarker> markers = new ArrayList<>();

    private Fixture(Path archive) throws IOException {
      bodies = new HistorySegmentStore(archive, new BlockHistoryCodec(), 4096);
      index = new HistoryIndexStore(archive, new HistoryIndexCodec());
    }

    private void append(BlockReverseDiff diff) throws IOException {
      HistoryLocation body = bodies.append(diff);
      HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
      List<String> databases = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
      markers.add(new HistoryCommitMarker(diff.getMeta(), diff.getMeta().getEpoch() - 1, body,
          indexLocation, new byte[16], databases));
    }

    private void sync() throws IOException {
      bodies.sync();
      index.sync();
    }

    private ServingKeyIndexGeneration serving() throws IOException {
      return ServingKeyIndexGeneration.rebuild(
          "read-generation", 0, hash(0), markers, index::read,
          new ArrayList<>(ArchiveStoreScope.getStateDatabases()),
          ServingKeyIndexGeneration.IndexLayout.prototypeDefaults());
    }

    private CommittedHistoryReader history() throws IOException {
      return new CommittedHistoryReader(0, hash(0), markers, index::read, bodies::read,
          new ArrayList<>(ArchiveStoreScope.getStateDatabases()));
    }

    private ArchiveReadSnapshot snapshot(long target, InMemoryLatest latest) throws IOException {
      return ArchiveReadSnapshot.pin(target, markers.size(), hash(markers.size()), serving(),
          latest, history());
    }

    @Override
    public void close() throws Exception {
      index.close();
      bodies.close();
    }
  }

  private static final class InMemoryLatest implements PinnedLatestState {
    private final long block;
    private final byte[] hash;
    private final Map<String, byte[]> values;
    private final Map<String, Map<String, byte[]>> scopedValues;
    private boolean closed;

    private InMemoryLatest(long block, byte[] hash, Map<String, byte[]> values) {
      this.block = block;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.values = new HashMap<>();
      this.scopedValues = null;
      values.forEach((key, value) -> this.values.put(key, Arrays.copyOf(value, value.length)));
    }

    private InMemoryLatest(long block, byte[] hash,
        Map<String, Map<String, byte[]>> scopedValues, boolean scoped) {
      this.block = block;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.values = Collections.emptyMap();
      this.scopedValues = new HashMap<>();
      scopedValues.forEach((dbName, rows) -> {
        Map<String, byte[]> copy = new HashMap<>();
        rows.forEach((key, value) -> copy.put(key, Arrays.copyOf(value, value.length)));
        this.scopedValues.put(dbName, copy);
      });
    }

    private static InMemoryLatest scoped(long block, byte[] hash,
        Map<String, Map<String, byte[]>> values) {
      return new InMemoryLatest(block, hash, values, true);
    }

    @Override
    public long getBlockNumber() {
      return block;
    }

    @Override
    public byte[] getBlockHash() {
      return Arrays.copyOf(hash, hash.length);
    }

    @Override
    public OldValue get(String dbName, byte[] physicalRawKey) {
      Map<String, byte[]> rows = scopedValues == null ? values
          : scopedValues.getOrDefault(dbName, Collections.emptyMap());
      return OldValue.fromNullable(rows.get(text(physicalRawKey)));
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive, int maxEntries) {
      List<HistoricalRangeOverlay.Entry> result = new ArrayList<>();
      Map<String, byte[]> rows = scopedValues == null ? values
          : scopedValues.getOrDefault(dbName, Collections.emptyMap());
      rows.forEach((key, value) -> {
        byte[] rawKey = bytes(key);
        if (BlockReverseDiff.compareUnsigned(rawKey, lowerInclusive) >= 0
            && (upperExclusive == null
            || BlockReverseDiff.compareUnsigned(rawKey, upperExclusive) < 0)) {
          result.add(new HistoricalRangeOverlay.Entry(rawKey, value));
        }
      });
      result.sort(Comparator.comparing(HistoricalRangeOverlay.Entry::getKey,
          BlockReverseDiff::compareUnsigned));
      return result;
    }

    @Override
    public void close() {
      closed = true;
    }

  }
}
