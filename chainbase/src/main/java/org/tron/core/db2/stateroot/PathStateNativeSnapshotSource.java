package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.capsule.utils.MarketUtils;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.EntryConsumer;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;

/** Caller-owned exact-27 native snapshot lease used only by current-state rebuild. */
public final class PathStateNativeSnapshotSource
    implements PathStateRebuildCoordinator.SnapshotSource, Closeable {

  private final PathStateParticipantDescriptor descriptor;
  private final SnapshotIdentity identity;
  private final Map<String, SnapshotCapableStore> stores;
  private final Map<String, String> sourceIdentities;
  private final Map<String, StoreSnapshot> snapshots;
  private final int pageSize;
  private final int marketEntryLimit;
  private boolean closed;

  private PathStateNativeSnapshotSource(PathStateParticipantDescriptor descriptor,
      SnapshotIdentity identity, Map<String, SnapshotCapableStore> stores,
      Map<String, String> sourceIdentities, Map<String, StoreSnapshot> snapshots,
      int pageSize, int marketEntryLimit) {
    this.descriptor = descriptor;
    this.identity = identity;
    this.stores = Collections.unmodifiableMap(new LinkedHashMap<>(stores));
    this.sourceIdentities = Collections.unmodifiableMap(new LinkedHashMap<>(sourceIdentities));
    this.snapshots = Collections.unmodifiableMap(new LinkedHashMap<>(snapshots));
    this.pageSize = pageSize;
    this.marketEntryLimit = marketEntryLimit;
  }

  /**
   * Resolves and pins every participant while holding the canonical apply/flush barrier.
   * Supplemental Stores are accepted only for exact participants absent from SnapshotManager.
   */
  public static PathStateNativeSnapshotSource acquire(SnapshotManager manager,
      Map<String, SnapshotCapableStore> supplementalStores, IdentityReader identityReader,
      int pageSize, int marketEntryLimit) throws IOException {
    Objects.requireNonNull(manager, "manager");
    Objects.requireNonNull(supplementalStores, "supplementalStores");
    Objects.requireNonNull(identityReader, "identityReader");
    if (pageSize <= 0 || marketEntryLimit <= 0) {
      throw new IllegalArgumentException("path-state snapshot scan limits must be positive");
    }
    PathStateNativeSnapshotSource[] acquired = new PathStateNativeSnapshotSource[1];
    manager.withArchiveStateBarrier(() -> acquired[0] = acquireInsideBarrier(manager,
        supplementalStores, identityReader, pageSize, marketEntryLimit));
    return acquired[0];
  }

  private static PathStateNativeSnapshotSource acquireInsideBarrier(SnapshotManager manager,
      Map<String, SnapshotCapableStore> supplementalStores, IdentityReader identityReader,
      int pageSize, int marketEntryLimit) throws IOException {
    PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
    LinkedHashMap<String, SnapshotCapableStore> stores = resolveStores(manager, supplementalStores,
        descriptor);
    SnapshotIdentity before = Objects.requireNonNull(identityReader.read(), "snapshot identity");
    LinkedHashMap<String, String> identities = new LinkedHashMap<>();
    LinkedHashMap<String, StoreSnapshot> snapshots = new LinkedHashMap<>();
    try {
      for (PathStateParticipantDescriptor.StoreIdentity participant : descriptor.getStores()) {
        String dbName = participant.getDbName();
        SnapshotCapableStore store = stores.get(dbName);
        String sourceIdentity = requireSourceIdentity(dbName, store.getSourceIdentity());
        StoreSnapshot snapshot = Objects.requireNonNull(
            store.pin(before.getBlockNumber(), before.getBlockHash()), "pinned Store snapshot");
        identities.put(dbName, sourceIdentity);
        snapshots.put(dbName, snapshot);
        validateSnapshot(dbName, sourceIdentity, before, snapshot);
      }
      SnapshotIdentity after = Objects.requireNonNull(identityReader.read(), "snapshot identity");
      if (!before.sameAs(after)) {
        throw new IOException("path-state canonical identity drifted during snapshot acquisition");
      }
      return new PathStateNativeSnapshotSource(descriptor, before, stores, identities, snapshots,
          pageSize, marketEntryLimit);
    } catch (IOException | RuntimeException failure) {
      closeAfterFailure(snapshots, failure);
      throw failure;
    }
  }

  private static LinkedHashMap<String, SnapshotCapableStore> resolveStores(SnapshotManager manager,
      Map<String, SnapshotCapableStore> supplementalStores,
      PathStateParticipantDescriptor descriptor) throws IOException {
    LinkedHashMap<String, SnapshotCapableStore> found = new LinkedHashMap<>();
    for (Chainbase database : new ArrayList<>(manager.getDbs())) {
      String dbName = database.getDbName();
      try {
        descriptor.require(dbName);
      } catch (IllegalArgumentException outsideScope) {
        continue;
      }
      if (found.containsKey(dbName)) {
        throw new IOException("duplicate path-state SnapshotManager Store: " + dbName);
      }
      Snapshot head = database.getHead();
      if (!Snapshot.isRoot(head)) {
        throw new IOException("path-state native snapshot requires a flushed Store: " + dbName);
      }
      Snapshot root = head.getRoot();
      DB<byte[], byte[]> engine = ((SnapshotRoot) root).getDb();
      if (!(engine instanceof SnapshotCapableStore)
          || !dbName.equals(engine.getDbName())) {
        throw new IOException("path-state Store lacks a native snapshot engine: " + dbName);
      }
      found.put(dbName, (SnapshotCapableStore) engine);
    }
    for (Map.Entry<String, SnapshotCapableStore> entry : supplementalStores.entrySet()) {
      String dbName = entry.getKey();
      SnapshotCapableStore store = Objects.requireNonNull(entry.getValue(), "supplemental Store");
      descriptor.require(dbName);
      if (!dbName.equals(store.getDbName()) || found.putIfAbsent(dbName, store) != null) {
        throw new IOException("duplicate or mismatched supplemental path-state Store: " + dbName);
      }
    }
    descriptor.requireExactDatabases(found.keySet());
    LinkedHashMap<String, SnapshotCapableStore> ordered = new LinkedHashMap<>();
    for (PathStateParticipantDescriptor.StoreIdentity participant : descriptor.getStores()) {
      ordered.put(participant.getDbName(), found.get(participant.getDbName()));
    }
    return ordered;
  }

  @Override
  public synchronized SnapshotIdentity identity() {
    ensureOpen();
    return identity;
  }

  @Override
  public Collection<String> databases() {
    return stores.keySet();
  }

  @Override
  public synchronized byte[] get(String dbName, byte[] physicalKey) throws IOException {
    ensureOpen();
    descriptor.require(dbName);
    StoreSnapshot snapshot = snapshots.get(dbName);
    if (snapshot == null) {
      throw new IOException("database is outside pinned path-state snapshot: " + dbName);
    }
    byte[] value = snapshot.get(copy(physicalKey, "physicalKey"));
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  @Override
  public synchronized void scan(String dbName, EntryConsumer consumer) throws IOException {
    ensureOpen();
    Objects.requireNonNull(consumer, "consumer");
    PathStateParticipantDescriptor.StoreIdentity participant = descriptor.require(dbName);
    StoreSnapshot snapshot = snapshots.get(dbName);
    if (snapshot == null) {
      throw new IOException("database is outside pinned path-state snapshot: " + dbName);
    }
    if (PathStateParticipantDescriptor.MARKET_PRICE_COMPARATOR.equals(
        participant.getComparatorId())) {
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
      scanLexical(snapshot, (key, value) -> {
        if (entries.size() >= marketEntryLimit) {
          throw new IOException("path-state market snapshot entry limit exceeded");
        }
        entries.add(new java.util.AbstractMap.SimpleImmutableEntry<>(key, value));
      });
      entries.sort((left, right) -> MarketUtils.comparePriceKey(left.getKey(), right.getKey()));
      for (Map.Entry<byte[], byte[]> entry : entries) {
        consumer.accept(entry.getKey(), entry.getValue());
      }
      return;
    }
    scanLexical(snapshot, consumer);
  }

  private void scanLexical(StoreSnapshot snapshot, EntryConsumer consumer) throws IOException {
    byte[] lower = new byte[0];
    byte[] previous = null;
    while (true) {
      List<Map.Entry<byte[], byte[]>> page;
      try {
        page = snapshot.range(lower, null, pageSize);
      } catch (UnsupportedOperationException unsupported) {
        throw new IOException("pinned Store does not support range scan: "
            + snapshot.getDbName(), unsupported);
      }
      if (page == null || page.size() > pageSize) {
        throw new IOException("pinned Store returned an invalid range page: "
            + snapshot.getDbName());
      }
      for (Map.Entry<byte[], byte[]> entry : page) {
        byte[] key = copy(entry.getKey(), "snapshot key");
        byte[] value = copy(entry.getValue(), "snapshot value");
        if (previous != null && compareUnsigned(previous, key) >= 0) {
          throw new IOException("pinned Store range is not strictly lexical: "
              + snapshot.getDbName());
        }
        consumer.accept(key, value);
        previous = key;
      }
      if (page.size() < pageSize) {
        return;
      }
      if (previous == null) {
        throw new IOException("pinned Store returned a full empty range page: "
            + snapshot.getDbName());
      }
      lower = Arrays.copyOf(previous, previous.length + 1);
    }
  }

  @Override
  public synchronized void verifyIdentity(SnapshotIdentity expected) throws IOException {
    ensureOpen();
    if (!identity.sameAs(expected)) {
      throw new IOException("path-state snapshot block identity mismatch");
    }
    for (Map.Entry<String, StoreSnapshot> entry : snapshots.entrySet()) {
      String dbName = entry.getKey();
      String expectedSource = sourceIdentities.get(dbName);
      if (!expectedSource.equals(stores.get(dbName).getSourceIdentity())) {
        throw new IOException("path-state Store source was replaced: " + dbName);
      }
      validateSnapshot(dbName, expectedSource, identity, entry.getValue());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    List<StoreSnapshot> reverse = new ArrayList<>(snapshots.values());
    Collections.reverse(reverse);
    for (StoreSnapshot snapshot : reverse) {
      try {
        snapshot.close();
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

  private static void validateSnapshot(String dbName, String sourceIdentity,
      SnapshotIdentity identity, StoreSnapshot snapshot) throws IOException {
    if (!dbName.equals(snapshot.getDbName())
        || !sourceIdentity.equals(snapshot.getSourceIdentity())
        || snapshot.getBlockNumber() != identity.getBlockNumber()
        || !Arrays.equals(snapshot.getBlockHash(), identity.getBlockHash())) {
      throw new IOException("pinned path-state Store identity mismatch: " + dbName);
    }
  }

  private static String requireSourceIdentity(String dbName, String sourceIdentity)
      throws IOException {
    if (sourceIdentity == null || sourceIdentity.isEmpty()) {
      throw new IOException("path-state Store source identity is invalid: " + dbName);
    }
    return sourceIdentity;
  }

  private static void closeAfterFailure(Map<String, StoreSnapshot> snapshots,
      Exception failure) {
    List<StoreSnapshot> reverse = new ArrayList<>(snapshots.values());
    Collections.reverse(reverse);
    for (StoreSnapshot snapshot : reverse) {
      try {
        snapshot.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static byte[] copy(byte[] value, String name) throws IOException {
    if (value == null) {
      throw new IOException(name + " must not be null");
    }
    return Arrays.copyOf(value, value.length);
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

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("path-state native snapshot source is closed");
    }
  }

  @FunctionalInterface
  public interface IdentityReader {
    SnapshotIdentity read() throws IOException;
  }
}
