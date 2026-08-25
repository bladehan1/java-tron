package org.tron.program;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.db.Manager;
import org.tron.core.db.RevokingDatabase;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.HistoricalAccountAssetBalanceResolver;
import org.tron.core.db2.archive.HistoricalAccountAssetPrefixResolver;
import org.tron.core.db2.archive.HistoricalAccountAssetPrefixResolver.Balance;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.P66AccountAssetCodec;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.AccountAssetStore;

/** Opt-in startup diagnostic for request-owned State Archive point reads. */
@Slf4j(topic = "DB")
public final class ArchiveStateDiagnostic {

  public static final String ENABLE_PROPERTY = "tron.stateArchive.startupDiagnostic";
  private static final String ACCOUNT_ASSET_DATABASE = "account-asset";
  private static final int ABSENT_KEY_ATTEMPTS = 16;

  private ArchiveStateDiagnostic() {
  }

  /** Runs after Spring recovery and before FullNode services start. */
  public static void runIfEnabled(TronApplicationContext context) {
    if (!Boolean.parseBoolean(System.getProperty(ENABLE_PROPERTY, "false"))) {
      return;
    }
    RevokingDatabase revokingDatabase = context.getBean(RevokingDatabase.class);
    if (!(revokingDatabase instanceof SnapshotManager)) {
      throw new IllegalStateException("State Archive diagnostic requires SnapshotManager");
    }
    Report report = run(context.getBean(Manager.class), (SnapshotManager) revokingDatabase);
    logger.info("State archive startup diagnostic complete: block={}, stores={}, present={}, "
            + "absent={}, p66Phase={}, p66Balance={}, p66PrefixEntries={}",
        report.getBlockNumber(), report.getStoreCount(), report.getPresentCount(),
        report.getAbsentCount(), report.getP66Phase(), report.getP66Balance(),
        report.getP66PrefixCount());
  }

  static Report run(Manager manager, SnapshotManager snapshotManager) {
    Objects.requireNonNull(manager, "manager");
    Objects.requireNonNull(snapshotManager, "snapshotManager");
    long blockNumber = manager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
    Map<String, StoreView> stores = collectStores(manager, snapshotManager);
    List<String> expectedStores = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(expectedStores);
    if (!stores.keySet().equals(new TreeSet<>(expectedStores))) {
      throw new ArchivePersistenceException(
          "State Archive diagnostic Store scope is not exact-27: " + stores.keySet());
    }

    int presentCount = 0;
    int absentCount = 0;
    for (String dbName : expectedStores) {
      Sample sample = stores.get(dbName).sample(dbName);
      OldValue historical = manager.getArchiveStateValue(blockNumber, dbName, sample.key);
      OldValue current = OldValue.fromNullable(sample.value);
      if (!current.equals(historical)) {
        throw new ArchivePersistenceException(
            "State Archive diagnostic mismatch at block " + blockNumber + " Store " + dbName);
      }
      if (historical.isPresent()) {
        presentCount++;
      } else {
        absentCount++;
      }
      logger.info("State archive diagnostic point: block={}, store={}, sample={}, key={}, "
              + "present={}, valueSha256={}",
          blockNumber, dbName, sample.presentSample ? "present" : "absent",
          Hex.toHexString(sample.key), historical.isPresent(), digest(historical));
    }

    AccountAssetStore accountAssetStore = manager.getAccountAssetStore();
    Sample assetSample = StoreView.from(accountAssetStore.getDbSource())
        .requirePresentSample(ACCOUNT_ASSET_DATABASE);
    DecodedAssetRow decoded = new P66AccountAssetCodec()
        .decodePresentAssetRow(assetSample.key, assetSample.value);
    HistoricalAccountAssetBalanceResolver.Result logical =
        manager.getArchiveAccountAssetBalance(blockNumber, decoded.getAccountAddress(),
            decoded.getTokenId());
    if (!logical.isAccountPresent() || logical.getPhase() != Phase.P66_ON
        || logical.getBalance() != decoded.getBalance()) {
      throw new ArchivePersistenceException(
          "State Archive diagnostic P66 AccountAsset mismatch at block " + blockNumber);
    }
    logger.info("State archive diagnostic P66: block={}, address={}, tokenId={}, phase={}, "
            + "balance={}",
        blockNumber, Hex.toHexString(decoded.getAccountAddress()), decoded.getTokenId(),
        logical.getPhase(), logical.getBalance());

    Map<String, Long> currentBalances = currentAccountAssetBalances(
        accountAssetStore, decoded.getAccountAddress());
    HistoricalAccountAssetPrefixResolver.Limits limits = prefixLimits(
        accountAssetStore, decoded.getAccountAddress());
    HistoricalAccountAssetPrefixResolver.Result prefix = manager.getArchiveAccountAssets(
        blockNumber, decoded.getAccountAddress(), limits);
    if (!prefix.isAccountPresent() || prefix.getPhase() != Phase.P66_ON
        || prefix.getBalances().size() != currentBalances.size()) {
      throw new ArchivePersistenceException(
          "State Archive diagnostic P66 AccountAsset prefix mismatch at block " + blockNumber);
    }
    for (Balance balance : prefix.getBalances()) {
      Long currentBalance = currentBalances.get(balance.getTokenId());
      if (currentBalance == null || currentBalance != balance.getBalance()) {
        throw new ArchivePersistenceException(
            "State Archive diagnostic P66 AccountAsset prefix value mismatch at block "
                + blockNumber);
      }
    }
    logger.info("State archive diagnostic P66 prefix: block={}, address={}, phase={}, entries={}",
        blockNumber, Hex.toHexString(decoded.getAccountAddress()), prefix.getPhase(),
        prefix.getBalances().size());
    return new Report(blockNumber, expectedStores.size(), presentCount, absentCount,
        logical.getPhase(), logical.getBalance(), prefix.getBalances().size());
  }

  private static Map<String, Long> currentAccountAssetBalances(AccountAssetStore store,
      byte[] address) {
    Map<String, Long> balances = new java.util.TreeMap<>();
    P66AccountAssetCodec codec = new P66AccountAssetCodec();
    for (Map.Entry<WrappedByteArray, byte[]> entry : store.prefixQuery(address).entrySet()) {
      DecodedAssetRow decoded = codec.decodePresentAssetRow(
          entry.getKey().getBytes(), entry.getValue());
      if (!Arrays.equals(address, decoded.getAccountAddress())
          || balances.put(decoded.getTokenId(), decoded.getBalance()) != null) {
        throw new ArchivePersistenceException(
            "State Archive diagnostic current AccountAsset prefix is invalid");
      }
    }
    if (balances.isEmpty()) {
      throw new ArchivePersistenceException(
          "State Archive diagnostic requires a nonempty AccountAsset prefix");
    }
    return balances;
  }

  private static HistoricalAccountAssetPrefixResolver.Limits prefixLimits(
      AccountAssetStore store, byte[] address) {
    int entries = 0;
    int maxKeyBytes = 1;
    int maxValueBytes = 1;
    long totalBytes = 0L;
    for (Map.Entry<WrappedByteArray, byte[]> entry : store.prefixQuery(address).entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = Objects.requireNonNull(entry.getValue(), "AccountAsset prefix value");
      entries++;
      maxKeyBytes = Math.max(maxKeyBytes, key.length);
      maxValueBytes = Math.max(maxValueBytes, value.length);
      totalBytes = Math.addExact(totalBytes, Math.addExact((long) key.length, value.length));
    }
    if (entries == 0) {
      throw new ArchivePersistenceException(
          "State Archive diagnostic requires AccountAsset prefix limits");
    }
    return new HistoricalAccountAssetPrefixResolver.Limits(
        1, entries, entries, maxKeyBytes, maxValueBytes, totalBytes);
  }

  private static Map<String, StoreView> collectStores(Manager manager,
      SnapshotManager snapshotManager) {
    Map<String, StoreView> stores = new java.util.TreeMap<>();
    for (Chainbase database : snapshotManager.getDbs()) {
      if (!ArchiveStoreScope.isStateDatabase(database.getDbName())) {
        continue;
      }
      if (stores.put(database.getDbName(), StoreView.from(database)) != null) {
        throw new ArchivePersistenceException(
            "Duplicate State Archive diagnostic Store " + database.getDbName());
      }
    }
    if (!stores.containsKey(ACCOUNT_ASSET_DATABASE)) {
      AccountAssetStore accountAssetStore = manager.getAccountAssetStore();
      if (accountAssetStore == null) {
        throw new ArchivePersistenceException(
            "State Archive diagnostic requires account-asset Store");
      }
      stores.put(ACCOUNT_ASSET_DATABASE, StoreView.from(accountAssetStore.getDbSource()));
    }
    return stores;
  }

  private static String digest(OldValue value) {
    return value.isPresent() ? Hashing.sha256().hashBytes(value.getValue()).toString() : "absent";
  }

  private interface StoreView {

    Iterator<Map.Entry<byte[], byte[]>> iterator();

    byte[] get(byte[] key);

    default Sample sample(String dbName) {
      Iterator<Map.Entry<byte[], byte[]>> iterator = iterator();
      try {
        if (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          return new Sample(entry.getKey(), entry.getValue(), true);
        }
      } finally {
        close(iterator, dbName);
      }
      for (int attempt = 0; attempt < ABSENT_KEY_ATTEMPTS; attempt++) {
        byte[] key = Hashing.sha256().hashString(
            "state-archive-diagnostic\u0000" + dbName + "\u0000" + attempt,
            StandardCharsets.UTF_8).asBytes();
        if (get(key) == null) {
          return new Sample(key, null, false);
        }
      }
      throw new ArchivePersistenceException(
          "Unable to construct absent State Archive diagnostic key for " + dbName);
    }

    default Sample requirePresentSample(String dbName) {
      Sample sample = sample(dbName);
      if (!sample.presentSample) {
        throw new ArchivePersistenceException(
            "State Archive diagnostic requires a present sample for " + dbName);
      }
      return sample;
    }

    static StoreView from(Chainbase database) {
      return new StoreView() {
        @Override
        public Iterator<Map.Entry<byte[], byte[]>> iterator() {
          return database.iterator();
        }

        @Override
        public byte[] get(byte[] key) {
          return database.getUnchecked(key);
        }
      };
    }

    static StoreView from(DbSourceInter<byte[]> database) {
      return new StoreView() {
        @Override
        public Iterator<Map.Entry<byte[], byte[]>> iterator() {
          return database.iterator();
        }

        @Override
        public byte[] get(byte[] key) {
          return database.getData(key);
        }
      };
    }

    static void close(Iterator<?> iterator, String dbName) {
      if (!(iterator instanceof AutoCloseable)) {
        return;
      }
      try {
        ((AutoCloseable) iterator).close();
      } catch (Exception failure) {
        throw new ArchivePersistenceException(
            "Failed to close State Archive diagnostic iterator for " + dbName, failure);
      }
    }
  }

  private static final class Sample {
    private final byte[] key;
    private final byte[] value;
    private final boolean presentSample;

    private Sample(byte[] key, byte[] value, boolean presentSample) {
      this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
      this.presentSample = presentSample;
    }
  }

  static final class Report {
    private final long blockNumber;
    private final int storeCount;
    private final int presentCount;
    private final int absentCount;
    private final Phase p66Phase;
    private final long p66Balance;
    private final int p66PrefixCount;

    private Report(long blockNumber, int storeCount, int presentCount, int absentCount,
        Phase p66Phase, long p66Balance, int p66PrefixCount) {
      this.blockNumber = blockNumber;
      this.storeCount = storeCount;
      this.presentCount = presentCount;
      this.absentCount = absentCount;
      this.p66Phase = p66Phase;
      this.p66Balance = p66Balance;
      this.p66PrefixCount = p66PrefixCount;
    }

    long getBlockNumber() {
      return blockNumber;
    }

    int getStoreCount() {
      return storeCount;
    }

    int getPresentCount() {
      return presentCount;
    }

    int getAbsentCount() {
      return absentCount;
    }

    Phase getP66Phase() {
      return p66Phase;
    }

    long getP66Balance() {
      return p66Balance;
    }

    int getP66PrefixCount() {
      return p66PrefixCount;
    }
  }
}
