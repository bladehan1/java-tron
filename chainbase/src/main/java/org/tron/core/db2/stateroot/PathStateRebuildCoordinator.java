package org.tron.core.db2.stateroot;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.core.capsule.utils.MarketUtils;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateParticipantDescriptor.StoreIdentity;

/** Builds and atomically publishes the first current path-state root from one admitted snapshot. */
public final class PathStateRebuildCoordinator {

  private static final Logger logger = LoggerFactory.getLogger("DB");
  private static final String STORE_DIGEST_DOMAIN = "path-state-rebuild-store/v1";
  private static final String SOURCE_DIGEST_DOMAIN = "path-state-rebuild-source/v1";
  private static final long PROGRESS_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final long PROGRESS_LOG_ROW_MASK = 1023L;
  private static final int LARGE_STORE_WORKERS = 1;
  private static final int SMALL_STORE_WORKERS = 1;
  private static final Set<String> LARGE_STORES = Collections.unmodifiableSet(
      new LinkedHashSet<>(Arrays.asList(
          "account", "account-asset", "delegation", "storage-row")));

  private final PathStateParticipantDescriptor descriptor;
  private final PathStateCanonicalizer canonicalizer;
  private final FaultHook faultHook;

  public PathStateRebuildCoordinator() {
    this(store -> { });
  }

  PathStateRebuildCoordinator(FaultHook faultHook) {
    descriptor = PathStateParticipantDescriptor.current();
    canonicalizer = new PathStateCanonicalizer();
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  /**
   * Consumes every exact-27 Store from one caller-owned native snapshot and publishes BASE(P0).
   *
   * <p>The source must keep all Store snapshots pinned until {@link SnapshotSource#verifyIdentity}
   * returns. Each Store scan must use the comparator declared by the manifest and supply strictly
   * increasing physical keys. The coordinator does not expose a reusable Store iterator.
   */
  public RebuildResult rebuild(PathStateStoreManifest manifest, SnapshotSource source)
      throws IOException {
    return rebuildInternal(manifest, source, null, PathStateLayerLimits.defaults());
  }

  /** Publishes BASE(P0) through the catch-up handoff and drains every queued transition. */
  public RebuildResult rebuild(PathStateStoreManifest manifest, SnapshotSource source,
      PathStateCatchUpQueue catchUpQueue, PathStateLayerLimits layerLimits) throws IOException {
    return rebuildInternal(manifest, source,
        Objects.requireNonNull(catchUpQueue, "catchUpQueue"), layerLimits);
  }

  private RebuildResult rebuildInternal(PathStateStoreManifest manifest, SnapshotSource source,
      PathStateCatchUpQueue catchUpQueue, PathStateLayerLimits layerLimits) throws IOException {
    PathStateStoreManifest admittedManifest = Objects.requireNonNull(manifest, "manifest");
    SnapshotSource admittedSource = Objects.requireNonNull(source, "source");
    PathStateLayerLimits admittedLimits = Objects.requireNonNull(layerLimits, "layerLimits");
    SnapshotIdentity identity = Objects.requireNonNull(admittedSource.identity(), "identity");
    byte[] sourceIdentityDigest = SnapshotIdentity.copy32(
        admittedSource.sourceIdentityDigest(), "sourceIdentityDigest");
    descriptor.requireExactDatabases(admittedSource.databases());
    admittedSource.verifyIdentity(identity);

    PathStateCurrentStore currentStore = new PathStateCurrentStore(admittedManifest);
    if (currentStore.isInitialized()) {
      throw new IOException("path-state rebuild requires an uninitialized current store");
    }
    if (catchUpQueue != null) {
      catchUpQueue.admitSnapshot(identity);
    }

    RebuildResult rebuildResult;
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(admittedManifest)) {
      PathStateRoot root = stores.createRoot();
      PathStateRebuildCheckpoint checkpoint = stores.getRebuildCheckpoint();
      Map<Integer, StoreResult> completedStores = new TreeMap<>();
      if (checkpoint != null) {
        root.restoreRebuildParticipants(checkpoint);
        for (StoreResult completed : checkpoint.getCompletedStores()) {
          completedStores.put(completed.getStoreId(), completed);
        }
        logger.info("Path-state rebuild checkpoint restored: completedStores={}, "
                + "remainingStores={}, reuseScope=completed-store, "
                + "inProgressStoreResume=from-beginning",
            completedStores.size(), descriptor.getStores().size() - completedStores.size());
      } else {
        logger.info("Path-state rebuild starting without checkpoint: stores={}, "
                + "reuseScope=completed-store, inProgressStoreResume=from-beginning",
            descriptor.getStores().size());
      }
      if (checkpoint != null && !identity.sameAs(checkpoint.getIdentity())) {
        throw new IOException("path-state rebuild checkpoint snapshot identity mismatch");
      }
      if (checkpoint != null && !Arrays.equals(admittedManifest.getIdentityDigest(),
          checkpoint.getManifestDigest())) {
        throw new IOException("path-state rebuild checkpoint manifest identity mismatch");
      }
      if (checkpoint != null && !Arrays.equals(sourceIdentityDigest,
          checkpoint.getSourceIdentityDigest())) {
        throw new IOException("path-state rebuild checkpoint source identity mismatch");
      }
      try (StoreBuilders builders = new StoreBuilders(admittedManifest, sourceIdentityDigest,
          stores)) {
        buildStoresInParallel(admittedManifest, admittedSource, identity, sourceIdentityDigest,
            root, stores, builders, completedStores);
      }
      List<StoreResult> storeResults = new ArrayList<>(completedStores.values());
      long totalEntries = 0;
      for (StoreResult completed : storeResults) {
        totalEntries = Math.addExact(totalEntries, completed.getEntryCount());
      }

      admittedSource.verifyIdentity(identity);
      byte[] stateRoot = root.rootHash();
      byte[] sourceDigest = sourceDigest(identity, sourceIdentityDigest, storeResults, stateRoot);
      PathStateRootMetadata metadata = PathStateRootMetadata.base(identity.getBlockNumber(),
          identity.getBlockHash(), identity.getParentHash(), identity.getTimestamp(),
          identity.getPhase(), admittedManifest.getIdentityDigest(), stateRoot, sourceDigest);
      PathStateBasePublication publication = new PathStateBasePublication(admittedManifest);
      PathStateRootMetadata published = catchUpQueue == null
          ? publication.publish(stores, metadata)
          : catchUpQueue.publishBase(identity, metadata,
              () -> publication.publish(stores, metadata));
      rebuildResult = new RebuildResult(published, storeResults, totalEntries, sourceDigest);
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state rebuild entry count overflow", overflow);
    }
    if (catchUpQueue != null) {
      catchUpQueue.drain(admittedManifest, admittedLimits);
    }
    return rebuildResult;
  }

  private void buildStoresInParallel(PathStateStoreManifest manifest, SnapshotSource source,
      SnapshotIdentity identity, byte[] sourceIdentityDigest, PathStateRoot root,
      PathStateNodeStoreSet stores, StoreBuilders builders,
      Map<Integer, StoreResult> completedStores) throws IOException {
    Object checkpointLock = new Object();
    ExecutorService largeExecutor = Executors.newFixedThreadPool(LARGE_STORE_WORKERS,
        rebuildThreadFactory("large"));
    ExecutorService smallExecutor = Executors.newFixedThreadPool(SMALL_STORE_WORKERS,
        rebuildThreadFactory("small"));
    List<Future<?>> futures = new ArrayList<>();
    Future<?> accountFuture = null;
    StoreIdentity accountAsset = null;
    try {
      for (StoreIdentity store : descriptor.getStores()) {
        if (completedStores.containsKey(store.getStoreId())) {
          StoreResult completed = completedStores.get(store.getStoreId());
          logger.info("Path-state rebuild Store reused: storeId={}, dbName={}, entries={}",
              store.getStoreId(), store.getDbName(), completed.getEntryCount());
          continue;
        }
        if ("account-asset".equals(store.getDbName())) {
          accountAsset = store;
          continue;
        }
        ExecutorService executor = LARGE_STORES.contains(store.getDbName())
            ? largeExecutor : smallExecutor;
        Future<?> future = submitStore(executor, null, store, manifest, source, identity,
            sourceIdentityDigest, root, stores, builders, completedStores, checkpointLock);
        futures.add(future);
        if ("account".equals(store.getDbName())) {
          accountFuture = future;
        }
      }
      if (accountAsset != null) {
        futures.add(submitStore(largeExecutor, accountFuture, accountAsset, manifest, source,
            identity, sourceIdentityDigest, root, stores, builders, completedStores,
            checkpointLock));
      }
      Throwable failure = null;
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          failure = appendFailure(failure,
              new IOException("path-state rebuild interrupted", interrupted));
        } catch (ExecutionException failed) {
          Throwable cause = failed.getCause();
          failure = appendFailure(failure, cause instanceof IOException
              || cause instanceof RuntimeException ? cause
              : new IOException("path-state Store rebuild failed", cause));
        }
      }
      if (failure != null) {
        if (failure instanceof IOException) {
          throw (IOException) failure;
        }
        throw (RuntimeException) failure;
      }
    } finally {
      largeExecutor.shutdownNow();
      smallExecutor.shutdownNow();
    }
  }

  private Future<?> submitStore(ExecutorService executor, Future<?> dependency,
      StoreIdentity store, PathStateStoreManifest manifest, SnapshotSource source,
      SnapshotIdentity identity, byte[] sourceIdentityDigest, PathStateRoot root,
      PathStateNodeStoreSet stores, StoreBuilders builders,
      Map<Integer, StoreResult> completedStores,
      Object checkpointLock) {
    return executor.submit(() -> {
      try {
        awaitDependency(dependency);
        StoreAccumulator accumulator = new StoreAccumulator(store, identity.getPhase(), root,
            builders);
        if (!"account-asset".equals(store.getDbName())) {
          builders.reset(store.getDbName());
        }
        if ("account".equals(store.getDbName())) {
          builders.reset("account-asset");
        }
        logger.info("Path-state rebuild Store started: storeId={}, dbName={}, tier={}",
            store.getStoreId(), store.getDbName(), storeTier(store));
        source.scan(store.getDbName(), accumulator::accept);
        StoreResult result = accumulator.finish();
        accumulator.logCompleted();
        if ("account".equals(store.getDbName())) {
          root.participantRoot("account-asset");
        }
        synchronized (checkpointLock) {
          completedStores.put(result.getStoreId(), result);
          PathStateRebuildCheckpoint next = new PathStateRebuildCheckpoint(
              manifest.getIdentityDigest(), sourceIdentityDigest, identity,
              new ArrayList<>(completedStores.values()));
          stores.checkpointStreamedRebuild(next);
          logger.info("Path-state rebuild Store checkpointed: storeId={}, dbName={}, entries={}, "
                  + "completedStores={}, remainingStores={}",
              result.getStoreId(), result.getDbName(), result.getEntryCount(),
              completedStores.size(), descriptor.getStores().size() - completedStores.size());
        }
        faultHook.afterStore(result);
        return null;
      } catch (IOException | RuntimeException failure) {
        logger.error("Path-state rebuild Store failed: storeId={}, dbName={}, tier={}",
            store.getStoreId(), store.getDbName(), storeTier(store), failure);
        throw failure;
      }
    });
  }

  private static void awaitDependency(Future<?> dependency) throws IOException {
    if (dependency == null) {
      return;
    }
    try {
      dependency.get();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("path-state dependent Store rebuild interrupted", interrupted);
    } catch (ExecutionException failed) {
      throw new IOException("path-state dependent Store rebuild failed", failed.getCause());
    }
  }

  private static ThreadFactory rebuildThreadFactory(String tier) {
    AtomicInteger sequence = new AtomicInteger();
    return task -> {
      Thread thread = new Thread(task,
          "path-state-rebuild-" + tier + "-" + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private static String storeTier(StoreIdentity store) {
    return LARGE_STORES.contains(store.getDbName()) ? "large" : "small";
  }

  private static Throwable appendFailure(Throwable previous, Throwable next) {
    if (previous == null) {
      return next;
    }
    previous.addSuppressed(next);
    return previous;
  }

  private byte[] sourceDigest(SnapshotIdentity identity, byte[] sourceIdentityDigest,
      List<StoreResult> stores, byte[] stateRoot) {
    Hasher hasher = domainHasher(SOURCE_DIGEST_DOMAIN);
    putLong(hasher, identity.getBlockNumber());
    putBytes(hasher, identity.getBlockHash());
    putBytes(hasher, identity.getParentHash());
    putLong(hasher, identity.getTimestamp());
    putInt(hasher, identity.getPhase().ordinal());
    putBytes(hasher, sourceIdentityDigest);
    putInt(hasher, stores.size());
    for (StoreResult store : stores) {
      putInt(hasher, store.getStoreId());
      putString(hasher, store.getDbName());
      putLong(hasher, store.getEntryCount());
      putBytes(hasher, store.getInputDigest());
      putBytes(hasher, store.getStoreRoot());
    }
    putBytes(hasher, stateRoot);
    return hasher.hash().asBytes();
  }

  private final class StoreAccumulator {

    private final StoreIdentity store;
    private final P66Phase phase;
    private final PathStateRoot root;
    private final StoreBuilders builders;
    private final Hasher inputDigest;
    private final long startedNanos = System.nanoTime();
    private byte[] previousKey;
    private long entryCount;
    private long keyBytes;
    private long valueBytes;
    private long lastProgressLogNanos = startedNanos;

    private StoreAccumulator(StoreIdentity store, P66Phase phase, PathStateRoot root,
        StoreBuilders builders) {
      this.store = store;
      this.phase = phase;
      this.root = root;
      this.builders = builders;
      inputDigest = domainHasher(STORE_DIGEST_DOMAIN);
      putInt(inputDigest, store.getStoreId());
      putString(inputDigest, store.getDbName());
      putString(inputDigest, store.getComparatorId());
      putString(inputDigest, canonicalizer.requireFormat(store.getDbName()).getCodecId());
    }

    private void accept(byte[] physicalKey, byte[] rawValue) throws IOException {
      byte[] key = copy(physicalKey, "physicalKey");
      byte[] value = Arrays.copyOf(Objects.requireNonNull(rawValue, "rawValue"),
          rawValue.length);
      if (previousKey != null && compare(store, previousKey, key) >= 0) {
        throw new IllegalArgumentException(
            "path-state snapshot keys are not strictly increasing: " + store.getDbName());
      }
      validateAccountLayout(key, value);
      PathStateMutation mutation = canonicalizer.put(phase, store.getDbName(), key, value);
      builders.put(mutation);
      if ("account".equals(store.getDbName())) {
        List<PathStateMutation> projected = canonicalizer.projectSnapshotAccountAssets(
            phase, key, value);
        if (!projected.isEmpty()) {
          for (PathStateMutation asset : projected) {
            // A zero embedded balance projects to ABSENT; snapshot rebuilds spool PRESENT rows.
            if (!asset.isDelete()) {
              builders.put(asset);
            }
          }
        }
      }
      putBytes(inputDigest, key);
      putBytes(inputDigest, value);
      previousKey = key;
      entryCount = Math.addExact(entryCount, 1L);
      keyBytes = Math.addExact(keyBytes, key.length);
      valueBytes = Math.addExact(valueBytes, value.length);
      logProgressIfDue();
    }

    private void validateAccountLayout(byte[] key, byte[] value) {
      if ("account".equals(store.getDbName())) {
        canonicalizer.requireSnapshotAccountLayout(phase, key, value);
      }
    }

    private StoreResult finish() throws IOException {
      byte[] storeRoot = builders.build(store.getDbName());
      root.completeRebuildParticipant(store.getDbName(), storeRoot);
      return new StoreResult(store.getStoreId(), store.getDbName(), entryCount,
          inputDigest.hash().asBytes(), storeRoot);
    }

    private void logProgressIfDue() {
      if ((entryCount & PROGRESS_LOG_ROW_MASK) != 0) {
        return;
      }
      long now = System.nanoTime();
      if (now - lastProgressLogNanos < PROGRESS_LOG_INTERVAL_NANOS) {
        return;
      }
      lastProgressLogNanos = now;
      logProgress("scanning", now);
    }

    private void logCompleted() {
      logProgress("completed", System.nanoTime());
    }

    private void logProgress(String status, long now) {
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - startedNanos);
      long entriesPerSecond = elapsedMillis == 0 ? entryCount
          : (long) (entryCount * 1000.0d / elapsedMillis);
      logger.info("Path-state rebuild Store {}: storeId={}, dbName={}, tier={}, rows={}, "
              + "keyBytes={}, valueBytes={}, elapsedMs={}, rowsPerSecond={}",
          status, store.getStoreId(), store.getDbName(), storeTier(store), entryCount,
          keyBytes, valueBytes, elapsedMillis, entriesPerSecond);
    }
  }

  private final class StoreBuilders implements AutoCloseable {

    private final PathStateStoreManifest manifest;
    private final byte[] generation;
    private final PathStateNodeStoreSet stores;
    private final Map<String, BuilderHandle> opened = new LinkedHashMap<>();

    private StoreBuilders(PathStateStoreManifest manifest, byte[] generation,
        PathStateNodeStoreSet stores) {
      this.manifest = manifest;
      this.generation = Arrays.copyOf(generation, generation.length);
      this.stores = stores;
    }

    private synchronized void put(PathStateMutation mutation) throws IOException {
      PathStateMutation present = Objects.requireNonNull(mutation, "mutation");
      if (present.isDelete()) {
        throw new IOException("snapshot rebuild cannot spool a delete mutation");
      }
      StoreIdentity identity = descriptor.require(present.getDbName());
      byte[] secureKey = PathStateCommitmentCodec.storeLeafKey(identity.getStoreId(),
          present.getCanonicalKey());
      byte[] encodedValue = PathStateCommitmentCodec.presentLeafValue(
          present.getCanonicalValue());
      handle(identity).builder.put(secureKey, encodedValue);
    }

    private synchronized byte[] build(String dbName) throws IOException {
      BuilderHandle handle = handle(descriptor.require(dbName));
      byte[] root = handle.builder.build();
      handle.writer.close();
      return root;
    }

    private synchronized void reset(String dbName) throws IOException {
      handle(descriptor.require(dbName)).builder.reset();
    }

    private BuilderHandle handle(StoreIdentity identity) throws IOException {
      BuilderHandle existing = opened.get(identity.getDbName());
      if (existing != null) {
        return existing;
      }
      Path spool = manifest.getBaseDirectory().resolve("rebuild-spool")
          .resolve(Integer.toString(identity.getStoreId()));
      PathStateNodeStoreSet.RebuildWriter writer = stores.openRebuildWriter(
          identity.getStoreId());
      try {
        PathStateStoreTrieBuilder builder = new PathStateStoreTrieBuilder(spool,
            manifest.getEngine(), generation, writer::putNode, writer::putLeaf);
        BuilderHandle created = new BuilderHandle(builder, writer);
        opened.put(identity.getDbName(), created);
        return created;
      } catch (IOException | RuntimeException failure) {
        writer.close();
        throw failure;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      IOException failure = null;
      for (BuilderHandle handle : opened.values()) {
        try {
          handle.builder.close();
          handle.writer.close();
        } catch (IOException closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else {
            failure.addSuppressed(closeFailure);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static final class BuilderHandle {

    private final PathStateStoreTrieBuilder builder;
    private final PathStateNodeStoreSet.RebuildWriter writer;

    private BuilderHandle(PathStateStoreTrieBuilder builder,
        PathStateNodeStoreSet.RebuildWriter writer) {
      this.builder = builder;
      this.writer = writer;
    }
  }

  private static int compare(StoreIdentity store, byte[] left, byte[] right) {
    if (PathStateParticipantDescriptor.MARKET_PRICE_COMPARATOR.equals(
        store.getComparatorId())) {
      return MarketUtils.comparePriceKey(left, right);
    }
    for (int index = 0; index < Math.min(left.length, right.length); index++) {
      int compared = Integer.compare(left[index] & 0xff, right[index] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static Hasher domainHasher(String domain) {
    Hasher hasher = Hashing.sha256().newHasher();
    putString(hasher, domain);
    return hasher;
  }

  private static void putString(Hasher hasher, String value) {
    putBytes(hasher, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void putBytes(Hasher hasher, byte[] value) {
    byte[] bytes = Objects.requireNonNull(value, "value");
    putInt(hasher, bytes.length);
    hasher.putBytes(bytes);
  }

  private static void putInt(Hasher hasher, int value) {
    hasher.putBytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
  }

  private static void putLong(Hasher hasher, long value) {
    hasher.putBytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
  }

  private static byte[] copy(byte[] value, String name) {
    return Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
  }

  /** Caller-owned exact-27 native snapshot boundary. */
  public interface SnapshotSource {

    SnapshotIdentity identity();

    Collection<String> databases();

    /** Stable identity of the exact physical Store generations held by this snapshot. */
    byte[] sourceIdentityDigest();

    void scan(String dbName, EntryConsumer consumer) throws IOException;

    void verifyIdentity(SnapshotIdentity expected) throws IOException;
  }

  /** One physical PRESENT row read from a pinned Store snapshot. */
  @FunctionalInterface
  public interface EntryConsumer {

    void accept(byte[] physicalKey, byte[] rawValue) throws IOException;
  }

  @FunctionalInterface
  interface FaultHook {

    void afterStore(StoreResult store) throws IOException;
  }

  /** Immutable canonical block boundary shared by every Store snapshot in one rebuild. */
  public static final class SnapshotIdentity {

    private final long blockNumber;
    private final byte[] blockHash;
    private final byte[] parentHash;
    private final long timestamp;
    private final P66Phase phase;

    public SnapshotIdentity(long blockNumber, byte[] blockHash, byte[] parentHash,
        long timestamp, P66Phase phase) {
      if (blockNumber < 0) {
        throw new IllegalArgumentException("blockNumber must not be negative");
      }
      this.blockNumber = blockNumber;
      this.blockHash = copy32(blockHash, "blockHash");
      this.parentHash = copy32(parentHash, "parentHash");
      this.timestamp = timestamp;
      this.phase = Objects.requireNonNull(phase, "phase");
    }

    public long getBlockNumber() {
      return blockNumber;
    }

    public byte[] getBlockHash() {
      return Arrays.copyOf(blockHash, blockHash.length);
    }

    public byte[] getParentHash() {
      return Arrays.copyOf(parentHash, parentHash.length);
    }

    public long getTimestamp() {
      return timestamp;
    }

    public P66Phase getPhase() {
      return phase;
    }

    public boolean sameAs(SnapshotIdentity other) {
      return other != null && blockNumber == other.blockNumber && timestamp == other.timestamp
          && phase == other.phase && Arrays.equals(blockHash, other.blockHash)
          && Arrays.equals(parentHash, other.parentHash);
    }

    private static byte[] copy32(byte[] value, String name) {
      byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
      if (copy.length != PathStateRootMetadata.DIGEST_LENGTH) {
        throw new IllegalArgumentException(name + " must be exactly 32 bytes");
      }
      return copy;
    }
  }

  /** Immutable per-Store evidence emitted after one complete snapshot scan. */
  public static final class StoreResult {

    private final int storeId;
    private final String dbName;
    private final long entryCount;
    private final byte[] inputDigest;
    private final byte[] storeRoot;

    private StoreResult(int storeId, String dbName, long entryCount, byte[] inputDigest,
        byte[] storeRoot) {
      if (storeId <= 0 || entryCount < 0) {
        throw new IllegalArgumentException("rebuild Store result identity is invalid");
      }
      this.storeId = storeId;
      this.dbName = Objects.requireNonNull(dbName, "dbName");
      this.entryCount = entryCount;
      this.inputDigest = SnapshotIdentity.copy32(inputDigest, "inputDigest");
      this.storeRoot = SnapshotIdentity.copy32(storeRoot, "storeRoot");
    }

    static StoreResult restore(int storeId, String dbName, long entryCount, byte[] inputDigest,
        byte[] storeRoot) {
      return new StoreResult(storeId, dbName, entryCount, inputDigest, storeRoot);
    }

    public int getStoreId() {
      return storeId;
    }

    public String getDbName() {
      return dbName;
    }

    public long getEntryCount() {
      return entryCount;
    }

    public byte[] getInputDigest() {
      return Arrays.copyOf(inputDigest, inputDigest.length);
    }

    public byte[] getStoreRoot() {
      return Arrays.copyOf(storeRoot, storeRoot.length);
    }
  }

  /** Immutable publication result for BASE(P0). */
  public static final class RebuildResult {

    private final PathStateRootMetadata metadata;
    private final List<StoreResult> stores;
    private final long totalEntries;
    private final byte[] sourceDigest;
    private final Map<String, StoreResult> storesByName;

    private RebuildResult(PathStateRootMetadata metadata, List<StoreResult> stores,
        long totalEntries, byte[] sourceDigest) {
      this.metadata = metadata;
      this.stores = Collections.unmodifiableList(new ArrayList<>(stores));
      this.totalEntries = totalEntries;
      this.sourceDigest = Arrays.copyOf(sourceDigest, sourceDigest.length);
      LinkedHashMap<String, StoreResult> indexed = new LinkedHashMap<>();
      for (StoreResult store : stores) {
        indexed.put(store.getDbName(), store);
      }
      storesByName = Collections.unmodifiableMap(indexed);
    }

    public PathStateRootMetadata getMetadata() {
      return metadata;
    }

    public List<StoreResult> getStores() {
      return stores;
    }

    public StoreResult requireStore(String dbName) {
      StoreResult result = storesByName.get(Objects.requireNonNull(dbName, "dbName"));
      if (result == null) {
        throw new IllegalArgumentException("unknown rebuild Store: " + dbName);
      }
      return result;
    }

    public long getTotalEntries() {
      return totalEntries;
    }

    public byte[] getSourceDigest() {
      return Arrays.copyOf(sourceDigest, sourceDigest.length);
    }
  }
}
