package org.tron.core.db2.core;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.tron.common.cache.CacheManager;
import org.tron.common.cache.CacheType;
import org.tron.common.cache.TronCache;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.TxCacheDB;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.metrics.ExecutionAttribution;
import org.tron.core.store.AccountAssetStore;

public class SnapshotRoot extends AbstractSnapshot<byte[], byte[]> {

  @Getter
  private Snapshot solidity;
  private boolean isAccountDB;
  private boolean coupledMutationsMaterialized;

  void useMaterializedCoupledMutations() {
    coupledMutationsMaterialized = true;
  }

  boolean isCoupledMutationsMaterialized() {
    return coupledMutationsMaterialized;
  }

  private TronCache<WrappedByteArray, WrappedByteArray> cache;
  private static final List<String> CACHE_DBS = CommonParameter.getInstance()
      .getStorage().getCacheDbs();

  public SnapshotRoot(DB<byte[], byte[]> db) {
    this.db = db;
    solidity = this;
    isAccountDB = "account".equalsIgnoreCase(db.getDbName());
    if (CACHE_DBS.contains(this.db.getDbName())) {
      this.cache = CacheManager.allocate(CacheType.findByType(this.db.getDbName()));
    }
    isOptimized = "properties".equalsIgnoreCase(db.getDbName());
  }

  private boolean needOptAsset() {
    return isAccountDB && !coupledMutationsMaterialized && assetOptimizationEnabled();
  }

  /**
   * Direct root writes only happen while no revoking session exists, so the legacy eager
   * Account-to-AccountAsset migration still applies to them; session-bound writes are folded
   * by the P66 materializer inside the block layer instead.
   */
  private boolean needOptAssetOnDirectWrite() {
    return isAccountDB && assetOptimizationEnabled();
  }

  private boolean assetOptimizationEnabled() {
    return ChainBaseManager.getInstance().getDynamicPropertiesStore()
            .getAllowAccountAssetOptimizationFromRoot() == 1;
  }

  @Override
  public byte[] get(byte[] key) {
    long readStarted = ExecutionAttribution.sample(getDbName(), "root");
    WrappedByteArray cache = getCache(key);
    if (cache != null) {
      ExecutionAttribution.sampled(getDbName(), "root", readStarted, 0, true);
      return cache.getBytes();
    }
    long nativeStarted = ExecutionAttribution.sample(getDbName(), "database");
    byte[] value = db.get(key);
    ExecutionAttribution.sampled(getDbName(), "database", nativeStarted, 0, false);
    putCache(key, value);
    ExecutionAttribution.sampled(getDbName(), "root", readStarted, 0, false);
    return value;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    OutOfBlockWriteGuard.check(getDbName(), key, value, db::get);
    byte[] v = value;
    if (needOptAssetOnDirectWrite()) {
      if (ByteArray.isEmpty(value)) {
        remove(key);
        return;
      }
      AccountAssetStore assetStore =
              ChainBaseManager.getInstance().getAccountAssetStore();
      AccountCapsule item = new AccountCapsule(value);
      if (!item.getAssetOptimized()) {
        assetStore.deleteAccount(item.createDbKey());
        item.setAssetOptimized(true);
      }
      assetStore.putAccount(item.getInstance());
      item.clearAsset();
      v = item.getData();
    }
    db.put(key, v);
    putCache(key, v);
  }

  @Override
  public void remove(byte[] key) {
    OutOfBlockWriteGuard.check(getDbName(), key, null, db::get);
    if (needOptAssetOnDirectWrite()) {
      ChainBaseManager.getInstance().getAccountAssetStore().deleteAccount(key);
    }
    db.remove(key);
    putCache(key, null);
  }

  @Override
  public void merge(Snapshot from) {
    SnapshotImpl snapshot = (SnapshotImpl) from;
    Map<WrappedByteArray, WrappedByteArray> batch = Streams.stream(snapshot.db)
        .map(e -> Maps.immutableEntry(WrappedByteArray.of(e.getKey().getBytes()),
            WrappedByteArray.of(e.getValue().getBytes())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    OutOfBlockWriteGuard.checkBatch(getDbName(), batch, db::get);
    if (needOptAsset()) {
      processAccount(batch);
    } else {
      ((Flusher) db).flush(batch);
      putCache(batch);
    }
  }

  public void merge(List<Snapshot> snapshots) {
    Map<WrappedByteArray, WrappedByteArray> batch = new HashMap<>();
    for (Snapshot snapshot : snapshots) {
      SnapshotImpl from = (SnapshotImpl) snapshot;
      Streams.stream(from.db)
          .map(e -> Maps.immutableEntry(WrappedByteArray.of(e.getKey().getBytes()),
              WrappedByteArray.of(e.getValue().getBytes())))
          .forEach(e -> batch.put(e.getKey(), e.getValue()));
    }
    OutOfBlockWriteGuard.checkBatch(getDbName(), batch, db::get);
    if (needOptAsset()) {
      processAccount(batch);
    } else {
      ((Flusher) db).flush(batch);
      putCache(batch);
    }
  }

  /**
   * RECOVERY_ONLY legacy replay (07-C): the same merge as {@link #merge(Snapshot)}, but every
   * root write is synchronous. {@code processAccount} keeps writing its asset rows through
   * {@code AccountAssetStore.updateByBatch}, i.e. into the lane's replay layer, so the precedence
   * between those rows and the checkpoint's own account-asset rows is exactly the non-synchronous
   * one; the lane root then merges them with its own synchronous batch.
   */
  void mergeLegacyRecoverySynced(Snapshot from) {
    SnapshotImpl snapshot = (SnapshotImpl) from;
    Map<WrappedByteArray, WrappedByteArray> batch = Streams.stream(snapshot.db)
        .map(e -> Maps.immutableEntry(WrappedByteArray.of(e.getKey().getBytes()),
            WrappedByteArray.of(e.getValue().getBytes())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    OutOfBlockWriteGuard.checkBatch(getDbName(), batch, db::get);
    if (needOptAsset()) {
      processAccount(batch, true, false);
    } else if (db instanceof TxCacheDB) {
      // In-memory bloom only; it has no durable state to sync.
      ((Flusher) db).flush(batch);
      putCache(batch);
    } else {
      ((Flusher) db).flushSynced(batch);
      putCache(batch);
    }
  }

  /** Applies a fully coalesced common-checkpoint Store batch with an explicit sync barrier. */
  void applyCheckpointMutations(Map<WrappedByteArray, WrappedByteArray> batch) {
    applyCheckpointMutations(batch, true);
  }

  void applyCheckpointMutations(Map<WrappedByteArray, WrappedByteArray> batch, boolean sync) {
    // Authorized Common redo bypasses the direct-root writer guard.
    if (needOptAsset()) {
      processAccount(batch, sync);
    } else {
      if (sync) {
        ((Flusher) db).flushSynced(batch);
      } else {
        ((Flusher) db).flushUnsynced(batch);
      }
      putCache(batch);
    }
  }

  /**
   * Applies one coalesced common-checkpoint batch behind a deferred durability barrier: the bulk
   * is written unsynced and the tail entry carries the store's single synced write. The tail
   * follows the bulk in the same WAL, so forcing it forces the whole batch — one fsync per
   * touched database, which lets the stores replay in parallel instead of serial write+fsync
   * interleaves.
   */
  void applyCheckpointMutationsDeferred(Map<WrappedByteArray, WrappedByteArray> batch) {
    // Authorized Common redo bypasses the direct-root writer guard.
    if (needOptAsset()) {
      processAccountDeferred(batch);
    } else {
      flushSplit((Flusher) db, batch);
      putCache(batch);
    }
  }

  private static void flushSplit(Flusher flusher,
      Map<WrappedByteArray, WrappedByteArray> batch) {
    if (batch.size() <= 1) {
      if (!batch.isEmpty()) {
        flusher.flushSynced(batch);
      }
      return;
    }
    Map<WrappedByteArray, WrappedByteArray> bulk = new LinkedHashMap<>(batch.size());
    Map.Entry<WrappedByteArray, WrappedByteArray> tail = null;
    for (Map.Entry<WrappedByteArray, WrappedByteArray> entry : batch.entrySet()) {
      if (tail != null) {
        bulk.put(tail.getKey(), tail.getValue());
      }
      tail = entry;
    }
    flusher.flushUnsynced(bulk);
    flusher.flushSynced(Collections.singletonMap(tail.getKey(), tail.getValue()));
  }

  private static void updateByBatchSplit(AccountAssetStore assetStore, Map<byte[], byte[]> rows) {
    if (rows.size() <= 1) {
      assetStore.updateByBatchSynced(rows);
      return;
    }
    Map<byte[], byte[]> bulk = new HashMap<>(rows.size());
    Map.Entry<byte[], byte[]> tail = null;
    for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
      if (tail != null) {
        bulk.put(tail.getKey(), tail.getValue());
      }
      tail = entry;
    }
    assetStore.updateByBatch(bulk);
    assetStore.updateByBatchSynced(Collections.singletonMap(tail.getKey(), tail.getValue()));
  }

  private void processAccount(Map<WrappedByteArray, WrappedByteArray> batch) {
    processAccount(batch, false, false);
  }

  private void processAccount(Map<WrappedByteArray, WrappedByteArray> batch, boolean synced) {
    processAccount(batch, synced, synced);
  }

  private void processAccount(Map<WrappedByteArray, WrappedByteArray> batch, boolean synced,
      boolean syncedAssets) {
    AccountAssetStore assetStore = ChainBaseManager.getInstance().getAccountAssetStore();
    Map<WrappedByteArray, WrappedByteArray> accounts = new HashMap<>();
    Map<WrappedByteArray, WrappedByteArray> assets = new HashMap<>();
    foldAccountBatch(batch, assetStore, accounts, assets);
    if (synced) {
      ((Flusher) db).flushSynced(accounts);
    } else {
      ((Flusher) db).flush(accounts);
    }
    putCache(accounts);
    if (assets.size() > 0) {
      if (syncedAssets) {
        assetStore.updateByBatchSynced(AccountAssetStore.convert(assets));
      } else {
        assetStore.updateByBatch(AccountAssetStore.convert(assets));
      }
    }
  }

  private void processAccountDeferred(Map<WrappedByteArray, WrappedByteArray> batch) {
    AccountAssetStore assetStore = ChainBaseManager.getInstance().getAccountAssetStore();
    Map<WrappedByteArray, WrappedByteArray> accounts = new HashMap<>();
    Map<WrappedByteArray, WrappedByteArray> assets = new HashMap<>();
    foldAccountBatch(batch, assetStore, accounts, assets);
    flushSplit((Flusher) db, accounts);
    putCache(accounts);
    if (!assets.isEmpty()) {
      updateByBatchSplit(assetStore, AccountAssetStore.convert(assets));
    }
  }

  private static void foldAccountBatch(Map<WrappedByteArray, WrappedByteArray> batch,
      AccountAssetStore assetStore, Map<WrappedByteArray, WrappedByteArray> accounts,
      Map<WrappedByteArray, WrappedByteArray> assets) {
    batch.forEach((k, v) -> {
      if (ByteArray.isEmpty(v.getBytes())) {
        accounts.put(k, v);
        assets.putAll(assetStore.getDeletedAssets(k.getBytes()));
      } else {
        AccountCapsule item = new AccountCapsule(v.getBytes());
        if (!item.getAssetOptimized()) {
          assets.putAll(assetStore.getDeletedAssets(k.getBytes()));
          item.setAssetOptimized(true);
        }
        assets.putAll(assetStore.getAssets(item.getInstance()));
        item.clearAsset();
        accounts.put(k, WrappedByteArray.of(item.getData()));
      }
    });
  }

  private boolean cached() {
    return Objects.nonNull(this.cache);
  }

  private void putCache(byte[] key, byte[] value) {
    if (cached()) {
      cache.put(WrappedByteArray.of(key), WrappedByteArray.of(value));
    }
  }

  private void putCache(Map<WrappedByteArray, WrappedByteArray> values) {
    if (cached()) {
      values.forEach(cache::put);
    }
  }

  private WrappedByteArray getCache(byte[] key) {
    if (cached()) {
      return cache.getIfPresent(WrappedByteArray.of(key));
    }
    return null;
  }

  // second cache

  @Override
  public Snapshot retreat() {
    return this;
  }

  @Override
  public Snapshot getRoot() {
    return this;
  }

  @Override
  public Iterator<Map.Entry<byte[], byte[]>> iterator() {
    return db.iterator();
  }

  @Override
  public void close() {
    if (cached()) {
      CacheManager.release(cache);
    }
    ((Flusher) db).close();
  }

  @Override
  public void reset() {
    if (cached()) {
      CacheManager.release(cache);
    }
    ((Flusher) db).reset();
  }

  @Override
  public void resetSolidity() {
    solidity = this;
  }

  @Override
  public void updateSolidity() {
    solidity = solidity.getNext();
  }

  @Override
  public String getDbName() {
    return db.getDbName();
  }

  @Override
  public Snapshot newInstance() {
    SnapshotRoot replacement = new SnapshotRoot(db.newInstance());
    replacement.coupledMutationsMaterialized = coupledMutationsMaterialized;
    return replacement;
  }

  @Override
  public void reloadToMem() { }
}
