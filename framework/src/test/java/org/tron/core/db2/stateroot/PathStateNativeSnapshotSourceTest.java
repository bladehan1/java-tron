package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.tron.common.TestConstants.TEST_CONF;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.utils.MarketUtils;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.common.LevelDB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;

public class PathStateNativeSnapshotSourceTest {

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  @BeforeClass
  public static void initConfiguration() {
    Args.setParam(new String[0], TEST_CONF);
  }

  @AfterClass
  public static void clearConfiguration() {
    Args.clearParam();
  }

  @Test
  public void pinsExactStoresPagesLexicallyAndOrdersMarketRows() throws Exception {
    Registry registry = registry();
    registry.probes.get("proposal").add(new byte[]{1}, new byte[]{11});
    registry.probes.get("proposal").add(new byte[]{1, 0}, new byte[]{12});
    registry.probes.get("proposal").add(new byte[]{2}, new byte[]{22});
    byte[] lowPrice = marketKey(2, 1);
    byte[] highPrice = marketKey(1, 1);
    assertTrue(compareUnsigned(lowPrice, highPrice) > 0);
    registry.probes.get(PathStateParticipantDescriptor.MARKET_PRICE_DATABASE)
        .add(lowPrice, new byte[]{1});
    registry.probes.get(PathStateParticipantDescriptor.MARKET_PRICE_DATABASE)
        .add(highPrice, new byte[]{2});

    List<byte[]> proposalKeys = new ArrayList<>();
    List<byte[]> marketKeys = new ArrayList<>();
    try (PathStateNativeSnapshotSource source = PathStateNativeSnapshotSource.acquire(
        registry.manager, Collections.emptyMap(), PathStateNativeSnapshotSourceTest::identity,
        2, 10)) {
      assertEquals(27, source.databases().size());
      source.scan("proposal", (key, value) -> proposalKeys.add(key));
      source.scan(PathStateParticipantDescriptor.MARKET_PRICE_DATABASE,
          (key, value) -> marketKeys.add(key));
      source.verifyIdentity(identity());
    }

    assertEquals(3, proposalKeys.size());
    assertArrayEquals(new byte[]{1}, proposalKeys.get(0));
    assertArrayEquals(new byte[]{1, 0}, proposalKeys.get(1));
    assertArrayEquals(new byte[]{2}, proposalKeys.get(2));
    assertArrayEquals(lowPrice, marketKeys.get(0));
    assertArrayEquals(highPrice, marketKeys.get(1));
    assertEquals(27, registry.totalPins());
    assertEquals(27, registry.totalCloses());
  }

  @Test
  public void acceptsSupplementalAccountAssetAndRejectsMarketOverflow() throws Exception {
    Registry registry = registry();
    Probe accountAsset = registry.probes.get("account-asset");
    registry.manager.getDbs().removeIf(database -> "account-asset".equals(database.getDbName()));
    accountAsset.add(marketKey(1, 2), new byte[]{1});
    Probe market = registry.probes.get(PathStateParticipantDescriptor.MARKET_PRICE_DATABASE);
    market.add(marketKey(1, 2), new byte[]{1});
    market.add(marketKey(2, 3), new byte[]{2});

    try (PathStateNativeSnapshotSource source = PathStateNativeSnapshotSource.acquire(
        registry.manager, Collections.singletonMap("account-asset", accountAsset.store),
        PathStateNativeSnapshotSourceTest::identity, 2, 1)) {
      assertEquals(27, source.databases().size());
      assertThrows(IOException.class,
          () -> source.scan(PathStateParticipantDescriptor.MARKET_PRICE_DATABASE,
              (key, value) -> { }));
    }
    assertEquals(registry.totalPins(), registry.totalCloses());
  }

  @Test
  public void identityDriftDuringAcquireReleasesEveryPinnedStore() {
    Registry registry = registry();
    AtomicInteger reads = new AtomicInteger();
    assertThrows(IOException.class, () -> PathStateNativeSnapshotSource.acquire(
        registry.manager, Collections.emptyMap(),
        () -> reads.incrementAndGet() == 1 ? identity() : identity(2), 2, 10));
    assertEquals(27, registry.totalPins());
    assertEquals(27, registry.totalCloses());
  }

  @Test
  public void rejectsUnflushedRevokingLayersBeforePinning() throws Exception {
    Registry registry = registry();
    try (ISession ignored = registry.manager.buildSession(true)) {
      assertThrows(IOException.class, () -> PathStateNativeSnapshotSource.acquire(
          registry.manager, Collections.emptyMap(), PathStateNativeSnapshotSourceTest::identity,
          2, 10));
    }
    assertEquals(0, registry.totalPins());
  }

  private static Registry registry() {
    SnapshotManager manager = new SnapshotManager("");
    LinkedHashMap<String, Probe> probes = new LinkedHashMap<>();
    for (PathStateParticipantDescriptor.StoreIdentity participant
        : PathStateParticipantDescriptor.current().getStores()) {
      Probe probe = new Probe(participant.getDbName());
      probes.put(participant.getDbName(), probe);
      manager.getDbs().add(new Chainbase(new SnapshotRoot(probe.store)));
    }
    return new Registry(manager, probes);
  }

  private static SnapshotIdentity identity() {
    return identity(1);
  }

  private static SnapshotIdentity identity(int suffix) {
    byte[] blockHash = new byte[32];
    blockHash[31] = (byte) suffix;
    return new SnapshotIdentity(100 + suffix, blockHash, new byte[32], 1_000L + suffix,
        P66Phase.P66_ON);
  }

  private static byte[] marketKey(long sell, long buy) {
    return MarketUtils.createPairPriceKey(ByteArray.fromString("100"),
        ByteArray.fromString("200"), sell, buy);
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    for (int index = 0; index < Math.min(left.length, right.length); index++) {
      int compared = Integer.compare(left[index] & 0xff, right[index] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static final class Registry {
    private final SnapshotManager manager;
    private final Map<String, Probe> probes;

    private Registry(SnapshotManager manager, Map<String, Probe> probes) {
      this.manager = manager;
      this.probes = probes;
    }

    private int totalPins() {
      return probes.values().stream().mapToInt(probe -> probe.pins.get()).sum();
    }

    private int totalCloses() {
      return probes.values().stream().mapToInt(probe -> probe.closes.get()).sum();
    }
  }

  private static final class Probe {
    private final String dbName;
    private final String sourceIdentity;
    private final AtomicInteger pins = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final List<Map.Entry<byte[], byte[]>> rows = new ArrayList<>();
    private final FakeLevelDB store;

    private Probe(String dbName) {
      this.dbName = dbName;
      sourceIdentity = "test:" + dbName;
      store = new FakeLevelDB(this);
    }

    private void add(byte[] key, byte[] value) {
      rows.add(new SimpleImmutableEntry<>(Arrays.copyOf(key, key.length),
          Arrays.copyOf(value, value.length)));
      rows.sort((left, right) -> compareUnsigned(left.getKey(), right.getKey()));
    }

    private StoreSnapshot pin(long blockNumber, byte[] blockHash) {
      pins.incrementAndGet();
      byte[] expectedHash = Arrays.copyOf(blockHash, blockHash.length);
      List<Map.Entry<byte[], byte[]>> pinnedRows = new ArrayList<>(rows);
      return new StoreSnapshot() {
        private boolean closed;

        @Override
        public String getDbName() {
          return dbName;
        }

        @Override
        public String getSourceIdentity() {
          return sourceIdentity;
        }

        @Override
        public long getBlockNumber() {
          return blockNumber;
        }

        @Override
        public byte[] getBlockHash() {
          return Arrays.copyOf(expectedHash, expectedHash.length);
        }

        @Override
        public byte[] get(byte[] physicalRawKey) {
          for (Map.Entry<byte[], byte[]> row : pinnedRows) {
            if (Arrays.equals(row.getKey(), physicalRawKey)) {
              return Arrays.copyOf(row.getValue(), row.getValue().length);
            }
          }
          return null;
        }

        @Override
        public List<Map.Entry<byte[], byte[]>> range(byte[] lowerInclusive,
            byte[] upperExclusive, int maxEntries) {
          List<Map.Entry<byte[], byte[]>> result = new ArrayList<>();
          for (Map.Entry<byte[], byte[]> row : pinnedRows) {
            if (compareUnsigned(row.getKey(), lowerInclusive) >= 0
                && (upperExclusive == null
                || compareUnsigned(row.getKey(), upperExclusive) < 0)) {
              result.add(row);
              if (result.size() == maxEntries) {
                break;
              }
            }
          }
          return result;
        }

        @Override
        public void close() {
          if (!closed) {
            closed = true;
            closes.incrementAndGet();
          }
        }
      };
    }
  }

  private static final class FakeLevelDB extends LevelDB {
    private final Probe probe;

    private FakeLevelDB(Probe probe) {
      super(mock(LevelDbDataSourceImpl.class));
      this.probe = probe;
    }

    @Override
    public String getDbName() {
      return probe.dbName;
    }

    @Override
    public String getSourceIdentity() {
      return probe.sourceIdentity;
    }

    @Override
    public StoreSnapshot pin(long blockNumber, byte[] blockHash) {
      return probe.pin(blockNumber, blockHash);
    }

    @Override
    public DBIterator iterator() {
      return mock(DBIterator.class);
    }
  }
}
