package org.tron.core.db2.core;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.store.AccountAssetStore;

public class SnapshotRoot extends AbstractSnapshot<byte[], byte[]> {

  @Getter
  private Snapshot solidity;
  private boolean isAccountDB;
  private boolean coupledMutationsMaterialized;

  void useMaterializedCoupledMutations() {
    coupledMutationsMaterialized = true;
  }

  private TronCache<WrappedByteArray, WrappedByteArray> cache;
  private static final int CHECKPOINT_FLUSH_CHUNK_SIZE = 4096;
  private static final byte[] CHECKPOINT_HEAD_KEY = ("\0" + "common-checkpoint-head-v1")
      .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
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
    if (needOptAsset()) {
      processAccount(batch);
    } else {
      ((Flusher) db).flush(batch);
      putCache(batch);
    }
  }

  /**
   * Applies a fully coalesced common-checkpoint Store batch as unsynced chunks of at most 4096
   * entries (releasing the underlying Store between chunks). Checkpoint durability is anchored
   * by the common checkpoint version store, so business databases never fsync; the per-Store
   * checkpoint head key written last (still unsynced, hence after every data chunk in WAL order)
   * records exactly how much of the checkpoint survived a power loss. The legacy
   * Account-to-AccountAsset migration path keeps its single synced flush because it also writes
   * a second Store that is not covered by the version store in that mode.
   */
  void applyCheckpointMutations(Map<WrappedByteArray, WrappedByteArray> batch) {
    if (needOptAsset()) {
      processAccount(batch, true);
      return;
    }
    if (batch.isEmpty()) {
      return;
    }
    List<Map.Entry<WrappedByteArray, WrappedByteArray>> entries = new ArrayList<>(
        batch.entrySet());
    for (int from = 0; from < entries.size(); from += CHECKPOINT_FLUSH_CHUNK_SIZE) {
      flushCheckpointChunk(entries, from,
          Math.min(from + CHECKPOINT_FLUSH_CHUNK_SIZE, entries.size()));
    }
    putCache(batch);
  }

  private void flushCheckpointChunk(List<Map.Entry<WrappedByteArray, WrappedByteArray>> entries,
      int from, int to) {
    Map<WrappedByteArray, WrappedByteArray> chunk = new HashMap<>();
    for (int index = from; index < to; index++) {
      Map.Entry<WrappedByteArray, WrappedByteArray> entry = entries.get(index);
      chunk.put(entry.getKey(), entry.getValue());
    }
    ((Flusher) db).flush(chunk);
  }

  /**
   * Records the newest applied common-checkpoint head inside the Store itself, unsynced. The
   * NUL-prefixed internal key cannot collide with any business key and is read back only by the
   * startup replay check.
   */
  void applyCheckpointHead(long head) {
    db.put(CHECKPOINT_HEAD_KEY, ByteBuffer.allocate(Long.BYTES).putLong(head).array());
  }

  /** Returns the newest applied common-checkpoint head, or null when never recorded. */
  Long getCheckpointHead() {
    byte[] value = db.get(CHECKPOINT_HEAD_KEY);
    if (value == null) {
      return null;
    }
    if (value.length != Long.BYTES) {
      throw new IllegalStateException("common checkpoint head record is corrupt in "
          + db.getDbName());
    }
    return ByteBuffer.wrap(value).getLong();
  }

  private void processAccount(Map<WrappedByteArray, WrappedByteArray> batch) {
    processAccount(batch, false);
  }

  private void processAccount(Map<WrappedByteArray, WrappedByteArray> batch, boolean synced) {
    AccountAssetStore assetStore = ChainBaseManager.getInstance().getAccountAssetStore();
    Map<WrappedByteArray, WrappedByteArray> accounts = new HashMap<>();
    Map<WrappedByteArray, WrappedByteArray> assets = new HashMap<>();
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
    if (synced) {
      ((Flusher) db).flushSynced(accounts);
    } else {
      ((Flusher) db).flush(accounts);
    }
    putCache(accounts);
    if (assets.size() > 0) {
      if (synced) {
        assetStore.updateByBatchSynced(AccountAssetStore.convert(assets));
      } else {
        assetStore.updateByBatch(AccountAssetStore.convert(assets));
      }
    }
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
