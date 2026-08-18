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
import org.tron.core.db2.archive.ArchiveReadContext.HistoricalStore;
import org.tron.core.db2.archive.ArchiveReadContext.StoreAdapter;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;
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
    private boolean closed;

    private InMemoryLatest(long block, byte[] hash, Map<String, byte[]> values) {
      this.block = block;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.values = new HashMap<>();
      values.forEach((key, value) -> this.values.put(key, Arrays.copyOf(value, value.length)));
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
      return OldValue.fromNullable(values.get(text(physicalRawKey)));
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive) {
      List<HistoricalRangeOverlay.Entry> result = new ArrayList<>();
      values.forEach((key, value) -> {
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
