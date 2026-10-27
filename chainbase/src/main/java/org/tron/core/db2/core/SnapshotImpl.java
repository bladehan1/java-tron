package org.tron.core.db2.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.primitives.Bytes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.common.HashDB;
import org.tron.core.db2.common.Key;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.common.Value.Operator;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.metrics.ExecutionAttribution;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;

public class SnapshotImpl extends AbstractSnapshot<Key, Value> {

  @Getter
  protected Snapshot root;

  @Getter
  private BlockSnapshotMeta blockSnapshotMeta;

  @Getter
  private BlockReverseDiff preparedArchiveBlock;

  @Getter
  private PathStateSnapshotDelta preparedPathStateDelta;

  /**
   * Maintenance layer (design v2 ch02 §3.11.3, WP-02-12): a layer without block identity right
   * above the root that collects the startup out-of-block writes and is folded into the first
   * finalized block above it. Set by {@link SnapshotManager} after construction only, so the
   * optimized root copy of {@code properties} is not a maintenance write. Never set in form OFF.
   */
  private boolean maintenance;
  /** Whether the maintenance write window is open (startup defaults and migrations only). */
  private volatile boolean maintenanceWritesOpen;

  /** Tombstone for a cached miss; a distinct instance, compared by reference. */
  private static final byte[] NULL_ENTRY = new byte[0];

  /**
   * Block-scoped read-through cache of the active session head layer, or null when disabled.
   * Enabled by {@link SnapshotManager#advance()} on the freshly advanced head and disabled on
   * the layer it covers, so at most one layer per store holds a live cache and no cache
   * survives in the committed-block stack (bounded by one block of reads, dropped with the
   * layer). Entries memoize the state visible below this layer's own {@code db}: writes only
   * ever land in {@code db}, and {@code db} is probed before the cache on every read, so a
   * session put/remove always shadows a cached entry. The cache never feeds merge, flush,
   * checkpoint, iterator or collect paths, which all read {@code db} directly.
   */
  private volatile ConcurrentMap<BlockReadCacheKey, byte[]> blockReadCache;
  private final LongAdder blockReadCacheHits = new LongAdder();

  SnapshotImpl(Snapshot snapshot) {
    root = snapshot.getRoot();
    synchronized (this) {
      db = new HashDB(SnapshotImpl.class.getSimpleName() + ":" + root.getDbName());
    }
    previous = snapshot;
    snapshot.setNext(this);
    isOptimized = snapshot.isOptimized();
    if (isOptimized &&  root == previous) {
      Streams.stream(root.iterator()).forEach( e -> put(e.getKey(),e.getValue()));
    }
  }

  /**
   * Publishes the immutable block identity and optional archive payload on this layer.
   *
   * <p>The caller validates the prepared payload before this ownership-transfer point.
   */
  void attachArchiveBlock(BlockSnapshotMeta meta, BlockReverseDiff reverseDiff) {
    attachBlockArtifacts(meta, reverseDiff, null);
  }

  /** Atomically binds all prepared block-final artifacts owned by this Snapshot layer. */
  void attachBlockArtifacts(BlockSnapshotMeta meta, BlockReverseDiff reverseDiff,
      PathStateSnapshotDelta pathStateDelta) {
    BlockSnapshotMeta admitted = Objects.requireNonNull(meta, "meta");
    if (reverseDiff != null && !admitted.equals(reverseDiff.getMeta())) {
      throw new IllegalArgumentException("archive payload differs from Snapshot block identity");
    }
    if (pathStateDelta != null && !admitted.equals(pathStateDelta.getMeta())) {
      throw new IllegalArgumentException("path-state delta differs from Snapshot block identity");
    }
    blockSnapshotMeta = meta;
    preparedArchiveBlock = reverseDiff;
    preparedPathStateDelta = pathStateDelta;
  }

  @Override
  public byte[] get(byte[] key) {
    ConcurrentMap<BlockReadCacheKey, byte[]> cache = blockReadCache;
    if (cache == null) {
      return get(this, key);
    }
    return getWithBlockCache(key, cache);
  }

  /**
   * Read order on the active session head: own {@code db}, then the block read cache, then a
   * descent below this layer. {@code db} first is what makes the cache write-safe: a session
   * put stores a non-null Value whose bytes the next read returns before ever consulting the
   * cache, and a remove stores a DELETE Value (non-null Value, null bytes) that likewise wins
   * over any cached entry and correctly yields null for a key still present below. The cached
   * value (or NULL_ENTRY tombstone) only restates what the descent returned, and the descended
   * state cannot change while this layer is the session head: merge/flush fold layers without
   * altering visible state, and {@link SnapshotManager#advance()} disables this cache before a
   * new session layer is stacked on top.
   */
  private byte[] getWithBlockCache(byte[] key, ConcurrentMap<BlockReadCacheKey, byte[]> cache) {
    long readStarted = ExecutionAttribution.sample(getDbName(), "snapshot");
    Value own = db.get(Key.of(key));
    if (own != null) {
      ExecutionAttribution.sampled(getDbName(), "snapshot", readStarted, 1, true);
      return own.getBytes();
    }
    byte[] cached = cache.get(BlockReadCacheKey.of(key));
    if (cached != null) {
      blockReadCacheHits.increment();
      ExecutionAttribution.sampled(getDbName(), "snapshot", readStarted, 0, true);
      return cached == NULL_ENTRY ? null : cached;
    }
    byte[] result = get(previous, key);
    cache.putIfAbsent(BlockReadCacheKey.copyOf(key), result == null ? NULL_ENTRY : result);
    ExecutionAttribution.sampled(getDbName(), "snapshot", readStarted, 1, false);
    return result;
  }

  private byte[] get(Snapshot head, byte[] key) {
    long readStarted = ExecutionAttribution.sample(getDbName(), "snapshot");
    int layers = 0;
    Snapshot snapshot = head;
    Value value;

    while (Snapshot.isImpl(snapshot)) {
      layers++;
      if ((value = ((SnapshotImpl) snapshot).db.get(Key.of(key))) != null) {
        ExecutionAttribution.sampled(getDbName(), "snapshot", readStarted, layers, true);
        return value.getBytes();
      }

      snapshot = snapshot.getPrevious();
    }

    byte[] result = snapshot == null ? null : snapshot.get(key);
    ExecutionAttribution.sampled(getDbName(), "snapshot", readStarted, layers, false);
    return result;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    Preconditions.checkNotNull(key, "key in db is not null.");
    Preconditions.checkNotNull(value, "value in db is not null.");
    if (maintenance && !admitMaintenanceWrite(key, value)) {
      return;
    }

    db.put(Key.copyOf(key), Value.copyOf(Value.Operator.PUT, value));
  }

  @Override
  public void remove(byte[] key) {
    Preconditions.checkNotNull(key, "key in db is not null.");
    if (maintenance && !admitMaintenanceWrite(key, null)) {
      return;
    }
    db.put(Key.of(key), Value.of(Value.Operator.DELETE, null));
  }

  void markMaintenance() {
    maintenance = true;
  }

  /**
   * Turns on the block read cache for this layer; called by {@link SnapshotManager#advance()}
   * on the newly created session head only.
   */
  void enableBlockReadCache() {
    blockReadCacheHits.reset();
    blockReadCache = new ConcurrentHashMap<>();
  }

  /** Drops the block read cache; the layer keeps serving reads through the plain descent. */
  void disableBlockReadCache() {
    blockReadCache = null;
  }

  boolean hasBlockReadCache() {
    return blockReadCache != null;
  }

  int getBlockReadCacheSize() {
    ConcurrentMap<BlockReadCacheKey, byte[]> cache = blockReadCache;
    return cache == null ? 0 : cache.size();
  }

  long getBlockReadCacheHits() {
    return blockReadCacheHits.sum();
  }

  boolean isMaintenance() {
    return maintenance;
  }

  void setMaintenanceWritesOpen(boolean open) {
    maintenanceWritesOpen = open;
  }

  /**
   * Outside the maintenance write window the phase-one guard still holds (ch02 §3.11.2): an
   * exact-27 write that changes the visible state is refused before it is recorded, an identical
   * restatement is a no-op. Derived (non exact-27) Stores are never guarded, as on the root.
   * Returns whether the write must be recorded.
   */
  private boolean admitMaintenanceWrite(byte[] key, byte[] value) {
    if (maintenanceWritesOpen || !ArchiveStoreScope.isStateDatabase(getDbName())) {
      return true;
    }
    byte[] current = get(key);
    if (value == null ? current == null : current != null && Arrays.equals(current, value)) {
      return false;
    }
    throw OutOfBlockWriteGuard.refuse(getDbName(), key);
  }

  // we have a 3x3 matrix of all possibilities when merging previous snapshot and current snapshot :
  //                  -------------- snapshot -------------
  //                 /                                     \
  //                +------------+------------+------------+
  //                | put(Y)     | delete(Y)  | nop        |
  //   +------------+------------+------------+------------+
  // / | put(X)     | put(Y)     | del        | put(X)     |
  // p +------------+------------+------------+------------+
  // r | delete(X)  | put(Y)     | del        | del        |
  // e +------------+------------+------------+------------+
  // | | nop        | put(Y)     | del        | nop        |
  // \ +------------+------------+------------+------------+
  @Override
  public void merge(Snapshot from) {
    SnapshotImpl fromImpl = (SnapshotImpl) from;
    Streams.stream(fromImpl.db).forEach(e -> db.put(e.getKey(), e.getValue()));
  }

  public void mergeAhead(Snapshot from) {
    if (from instanceof SnapshotRoot) {
      return ;
    }
    SnapshotImpl fromImpl = (SnapshotImpl) from;
    Streams.stream(fromImpl.db).forEach(e -> {
      if (db.get(e.getKey()) == null) {
        db.put(e.getKey(), e.getValue());
      }
    }
    );
  }

  @Override
  public Snapshot retreat() {
    return previous;
  }

  @Override
  public Snapshot getSolidity() {
    return root.getSolidity();
  }

  @Override
  public Iterator<Map.Entry<byte[], byte[]>> iterator() {
    Map<WrappedByteArray, WrappedByteArray> all = new HashMap<>();
    collect(all);
    Set<WrappedByteArray> keys = new HashSet<>(all.keySet());
    all.entrySet()
        .removeIf(entry -> entry.getValue() == null || entry.getValue().getBytes() == null);
    return Iterators.concat(
        Iterators.transform(all.entrySet().iterator(),
            e -> Maps.immutableEntry(e.getKey().getBytes(), e.getValue().getBytes())),
        Iterators.filter(getRoot().iterator(),
            e -> !keys.contains(WrappedByteArray.of(e.getKey()))));
  }

  synchronized void collect(Map<WrappedByteArray, WrappedByteArray> all) {
    Snapshot next = getRoot().getNext();
    while (next != null) {
      Streams.stream(((SnapshotImpl) next).db)
          .forEach(e -> all.put(WrappedByteArray.of(e.getKey().getBytes()),
              WrappedByteArray.of(e.getValue().getBytes())));
      next = next.getNext();
    }
  }

  synchronized void collect(Map<WrappedByteArray, WrappedByteArray> all, byte[] prefix) {
    Snapshot next = getRoot().getNext();
    while (next != null) {
      Streams.stream(((SnapshotImpl) next).db).filter(e -> Bytes.indexOf(
              Objects.requireNonNull(e.getKey().getBytes()), prefix) == 0)
          .forEach(e -> all.put(WrappedByteArray.of(e.getKey().getBytes()),
              WrappedByteArray.of(e.getValue().getBytes())));
      next = next.getNext();
    }
  }

  /**
   * Note: old --> new
   * In the snapshot, there may be same keys.
   * If we use Map to get all the data, the later will overwrite the previous value.
   * So, if we use list, we need to exclude duplicate keys.
   * More than that, there will be some item which has been deleted, but just assigned in Operator,
   * so we need Operator value to determine next step.
   * */
  synchronized void collectUnique(Map<WrappedByteArray, Operator> all) {
    Snapshot next = getRoot().getNext();
    while (next != null) {
      Streams.stream(((SnapshotImpl) next).db)
          .forEach(e -> all.put(WrappedByteArray.of(e.getKey().getBytes()),
              e.getValue().getOperator()));
      next = next.getNext();
    }
  }



  @Override
  public void close() {
    getRoot().close();
  }

  @Override
  public void reset() {
    getRoot().reset();
  }

  @Override
  public void resetSolidity() {
    root.resetSolidity();
  }

  @Override
  public void updateSolidity() {
    root.updateSolidity();
  }

  @Override
  public String getDbName() {
    return root.getDbName();
  }

  @Override
  public Snapshot newInstance() {
    return new SnapshotImpl(this);
  }

  @Override
  public void reloadToMem() {
      mergeAhead(previous);
  }

  /**
   * Cache key with a precomputed hash: {@link WrappedByteArray#hashCode()} recomputes
   * {@code Arrays.hashCode} on every probe, which the hot read path would otherwise pay on
   * each lookup. Lookup keys wrap the caller's array; stored keys own a copy.
   */
  private static final class BlockReadCacheKey {
    private final byte[] bytes;
    private final int hash;

    private BlockReadCacheKey(byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
    }

    static BlockReadCacheKey of(byte[] bytes) {
      return new BlockReadCacheKey(bytes);
    }

    static BlockReadCacheKey copyOf(byte[] bytes) {
      return new BlockReadCacheKey(Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public boolean equals(Object o) {
      return o == this || o instanceof BlockReadCacheKey
          && Arrays.equals(bytes, ((BlockReadCacheKey) o).bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }
}
