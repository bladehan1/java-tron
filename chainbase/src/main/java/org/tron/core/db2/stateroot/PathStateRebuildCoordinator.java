package org.tron.core.db2.stateroot;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.capsule.utils.MarketUtils;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateParticipantDescriptor.StoreIdentity;

/** Builds and atomically publishes the first current path-state root from one admitted snapshot. */
public final class PathStateRebuildCoordinator {

  private static final String STORE_DIGEST_DOMAIN = "path-state-rebuild-store/v1";
  private static final String SOURCE_DIGEST_DOMAIN = "path-state-rebuild-source/v1";

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
      List<StoreResult> storeResults = checkpoint == null ? new ArrayList<>()
          : new ArrayList<>(checkpoint.getCompletedStores());
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
      long totalEntries = 0;
      for (StoreResult completed : storeResults) {
        totalEntries = Math.addExact(totalEntries, completed.getEntryCount());
      }
      for (int index = storeResults.size(); index < descriptor.getStores().size(); index++) {
        StoreIdentity store = descriptor.getStores().get(index);
        StoreAccumulator accumulator = new StoreAccumulator(store, identity.getPhase(), root,
            admittedSource);
        admittedSource.scan(store.getDbName(), accumulator::accept);
        StoreResult result = accumulator.finish();
        storeResults.add(result);
        totalEntries = Math.addExact(totalEntries, result.getEntryCount());
        checkpoint = new PathStateRebuildCheckpoint(admittedManifest.getIdentityDigest(),
            sourceIdentityDigest, identity, storeResults, root.rootHash());
        stores.checkpointRebuild(checkpoint);
        faultHook.afterStore(result);
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
    private final SnapshotSource source;
    private final Hasher inputDigest;
    private byte[] previousKey;
    private long entryCount;

    private StoreAccumulator(StoreIdentity store, P66Phase phase, PathStateRoot root,
        SnapshotSource source) {
      this.store = store;
      this.phase = phase;
      this.root = root;
      this.source = source;
      inputDigest = domainHasher(STORE_DIGEST_DOMAIN);
      putInt(inputDigest, store.getStoreId());
      putString(inputDigest, store.getDbName());
      putString(inputDigest, store.getComparatorId());
      putString(inputDigest, canonicalizer.requireFormat(store.getDbName()).getCodecId());
    }

    private void accept(byte[] physicalKey, byte[] rawValue) throws IOException {
      byte[] key = copyNonEmpty(physicalKey, "physicalKey");
      byte[] value = Arrays.copyOf(Objects.requireNonNull(rawValue, "rawValue"),
          rawValue.length);
      if (previousKey != null && compare(store, previousKey, key) >= 0) {
        throw new IllegalArgumentException(
            "path-state snapshot keys are not strictly increasing: " + store.getDbName());
      }
      validateAccountAssetLayout(key, value);
      PathStateMutation mutation = canonicalizer.put(phase, store.getDbName(), key, value);
      root.apply(Collections.singletonList(mutation));
      putBytes(inputDigest, key);
      putBytes(inputDigest, value);
      previousKey = key;
      entryCount = Math.addExact(entryCount, 1L);
    }

    private void validateAccountAssetLayout(byte[] key, byte[] value) throws IOException {
      if ("account".equals(store.getDbName())) {
        canonicalizer.requireSnapshotAccountLayout(phase, key, value);
        return;
      }
      if (!"account-asset".equals(store.getDbName())) {
        return;
      }
      byte[] accountKey = canonicalizer.accountAddressFromAssetKey(phase, key);
      byte[] accountValue = source.get("account", accountKey);
      if (accountValue == null) {
        throw new IOException("path-state AccountAsset row has no owning Account");
      }
      canonicalizer.requireSnapshotAccountLayout(phase, accountKey, accountValue);
    }

    private StoreResult finish() {
      return new StoreResult(store.getStoreId(), store.getDbName(), entryCount,
          inputDigest.hash().asBytes(), root.participantRoot(store.getDbName()));
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

  private static byte[] copyNonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  /** Caller-owned exact-27 native snapshot boundary. */
  public interface SnapshotSource {

    SnapshotIdentity identity();

    Collection<String> databases();

    /** Stable identity of the exact physical Store generations held by this snapshot. */
    byte[] sourceIdentityDigest();

    /** Returns one value from the same pinned snapshot, or {@code null} when physically absent. */
    byte[] get(String dbName, byte[] physicalKey) throws IOException;

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
