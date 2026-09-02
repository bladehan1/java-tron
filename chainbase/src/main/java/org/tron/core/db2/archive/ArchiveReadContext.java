package org.tron.core.db2.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

/** Request-owned bindings from every versioned Store to one pinned archive snapshot. */
public final class ArchiveReadContext implements Closeable {

  private final ArchiveReadSnapshot snapshot;
  private final Closeable owner;
  private final Map<String, StoreAdapter<?>> adapters;
  private final HistoricalAccountAssetBalanceResolver accountAssetResolver =
      new HistoricalAccountAssetBalanceResolver();
  private final HistoricalAccountAssetPrefixResolver accountAssetPrefixResolver =
      new HistoricalAccountAssetPrefixResolver();
  private boolean closed;

  private ArchiveReadContext(ArchiveReadSnapshot snapshot,
      Collection<StoreAdapter<?>> adapters, Closeable owner) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.adapters = validateAdapters(adapters);
  }

  /** Takes ownership of {@code snapshot}, including when adapter validation fails. */
  public static ArchiveReadContext open(ArchiveReadSnapshot snapshot,
      Collection<StoreAdapter<?>> adapters) throws IOException {
    try {
      return new ArchiveReadContext(snapshot, adapters, snapshot);
    } catch (RuntimeException failure) {
      closeAfterFailedOpen(snapshot, failure);
      throw failure;
    }
  }

  /** Takes ownership of {@code lease} so closing this context also releases gate accounting. */
  public static ArchiveReadContext open(ArchiveRuntimeQueryGate.Lease lease,
      Collection<StoreAdapter<?>> adapters) throws IOException {
    Objects.requireNonNull(lease, "lease");
    try {
      return new ArchiveReadContext(lease.getSnapshot(), adapters, lease);
    } catch (RuntimeException failure) {
      closeAfterFailedOpen(lease, failure);
      throw failure;
    }
  }

  public synchronized <T> HistoricalStore<T> store(StoreAdapter<T> adapter) {
    ensureOpen();
    Objects.requireNonNull(adapter, "adapter");
    if (adapters.get(adapter.getDbName()) != adapter) {
      throw new IllegalArgumentException(
          "Store adapter does not belong to this archive read context: " + adapter.getDbName());
    }
    return new HistoricalStore<>(snapshot, adapter);
  }

  public Set<String> getAdapterDbNames() {
    return Collections.unmodifiableSet(new LinkedHashSet<>(adapters.keySet()));
  }

  /** Internal exact physical read; public RPC layers must use typed resolvers instead. */
  public synchronized OldValue getExact(String dbName, byte[] physicalRawKey)
      throws IOException {
    ensureOpen();
    if (!adapters.containsKey(Objects.requireNonNull(dbName, "dbName"))) {
      throw new IllegalArgumentException(
          "Store adapter does not belong to this archive read context: " + dbName);
    }
    return snapshot.get(dbName, Objects.requireNonNull(physicalRawKey, "physicalRawKey"));
  }

  public long getTargetBlock() {
    return snapshot.getTargetBlock();
  }

  public long getPinnedBlock() {
    return snapshot.getPinnedBlock();
  }

  public synchronized void requirePinnedIdentity() {
    ensureOpen();
    snapshot.requirePinnedIdentity();
  }

  /** Resolves exact Account bytes and one P66-aware token balance from this request snapshot. */
  public synchronized HistoricalAccountAssetBalanceResolver.Result resolveAccountAsset(
      byte[] address, String tokenId) throws IOException {
    ensureOpen();
    return accountAssetResolver.resolve(snapshot, address, tokenId);
  }

  /** Resolves all token balances for exactly one Account under explicit query budgets. */
  public synchronized HistoricalAccountAssetPrefixResolver.Result resolveAccountAssets(
      byte[] address, HistoricalAccountAssetPrefixResolver.Limits limits) throws IOException {
    ensureOpen();
    return accountAssetPrefixResolver.resolve(snapshot, address, limits);
  }

  /** Resolves one logical contract slot using contract metadata from this same pinned context. */
  public synchronized Optional<byte[]> getStorage(byte[] contractAddress, byte[] logicalSlot)
      throws IOException {
    ensureOpen();
    Objects.requireNonNull(contractAddress, "contractAddress");
    Objects.requireNonNull(logicalSlot, "logicalSlot");
    OldValue contractValue = snapshot.get("contract", contractAddress);
    if (!contractValue.isPresent()) {
      throw new ArchivePersistenceException(
          "Historical Contract metadata is absent for logical storage lookup");
    }
    SmartContract contract;
    try {
      contract = SmartContract.parseFrom(contractValue.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new ArchivePersistenceException(
          "Historical Contract metadata cannot be decoded", e);
    }
    byte[] physicalKey = StorageRowKeyCodec.physicalKey(contractAddress, logicalSlot,
        contract.getVersion(), contract.getTrxHash().toByteArray());
    OldValue storageValue = snapshot.get("storage-row", physicalKey);
    return storageValue.isPresent()
        ? Optional.of(storageValue.getValue()) : Optional.empty();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      owner.close();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Archive read context is closed");
    }
  }

  private static Map<String, StoreAdapter<?>> validateAdapters(
      Collection<StoreAdapter<?>> definitions) {
    Objects.requireNonNull(definitions, "adapters");
    Map<String, StoreAdapter<?>> indexed = new LinkedHashMap<>();
    for (StoreAdapter<?> adapter : definitions) {
      Objects.requireNonNull(adapter, "adapter");
      if (indexed.put(adapter.getDbName(), adapter) != null) {
        throw new IllegalArgumentException("Duplicate historical Store adapter: "
            + adapter.getDbName());
      }
    }
    Set<String> expected = ArchiveStoreScope.getStateDatabases();
    if (!indexed.keySet().equals(expected)) {
      Set<String> missing = new LinkedHashSet<>(expected);
      missing.removeAll(indexed.keySet());
      Set<String> unexpected = new LinkedHashSet<>(indexed.keySet());
      unexpected.removeAll(expected);
      throw new IllegalArgumentException("Historical Store adapter set mismatch; missing="
          + missing + ", unexpected=" + unexpected);
    }
    return Collections.unmodifiableMap(indexed);
  }

  private static void closeAfterFailedOpen(Closeable owner,
      RuntimeException failure) throws IOException {
    if (owner == null) {
      return;
    }
    try {
      owner.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  /** Immutable adapter definition; one definition must exist for every versioned physical Store. */
  public static final class StoreAdapter<T> {
    private final String dbName;
    private final ValueDecoder<T> decoder;

    private StoreAdapter(String dbName, ValueDecoder<T> decoder) {
      this.dbName = Objects.requireNonNull(dbName, "dbName");
      this.decoder = Objects.requireNonNull(decoder, "decoder");
      if (!ArchiveStoreScope.isStateDatabase(dbName)) {
        throw new IllegalArgumentException("Not a versioned archive state database: " + dbName);
      }
    }

    public static <T> StoreAdapter<T> define(String dbName, ValueDecoder<T> decoder) {
      return new StoreAdapter<>(dbName, decoder);
    }

    public String getDbName() {
      return dbName;
    }
  }

  /** Read-only point view for one exact physical Store keyspace. */
  public static final class HistoricalStore<T> {
    private final ArchiveReadSnapshot snapshot;
    private final StoreAdapter<T> adapter;

    private HistoricalStore(ArchiveReadSnapshot snapshot, StoreAdapter<T> adapter) {
      this.snapshot = snapshot;
      this.adapter = adapter;
    }

    public Optional<T> get(byte[] physicalRawKey) throws IOException {
      OldValue value = snapshot.get(adapter.dbName, physicalRawKey);
      if (!value.isPresent()) {
        return Optional.empty();
      }
      T decoded = adapter.decoder.decode(value.getValue());
      if (decoded == null) {
        throw new IllegalStateException(
            "Historical Store adapter returned null: " + adapter.dbName);
      }
      return Optional.of(decoded);
    }

    public boolean has(byte[] physicalRawKey) throws IOException {
      return snapshot.get(adapter.dbName, physicalRawKey).isPresent();
    }
  }

  @FunctionalInterface
  public interface ValueDecoder<T> {
    T decode(byte[] value);
  }
}
