package org.tron.core.db2.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.tron.core.db2.archive.ArchiveReadContext.StoreAdapter;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.protos.Protocol.Account;
import org.tron.protos.contract.SmartContractOuterClass.ContractState;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

/** One request-owned exact-27 typed view of a canonical historical block-final state. */
public final class HistoricalQuerySession implements AutoCloseable {

  public static final class Limits {

    private final long maxReads;
    private final long maxBytes;
    private final long timeoutMillis;

    public Limits(long maxReads, long maxBytes, long timeoutMillis) {
      if (maxReads <= 0 || maxBytes <= 0 || timeoutMillis <= 0) {
        throw new IllegalArgumentException("Historical query limits must be positive");
      }
      this.maxReads = maxReads;
      this.maxBytes = maxBytes;
      this.timeoutMillis = timeoutMillis;
    }

    public static Limits defaults() {
      return new Limits(100_000L, 64L * 1024L * 1024L, 10_000L);
    }
  }

  private static final Map<String, StoreAdapter<byte[]>> RAW_ADAPTERS = rawAdapters();
  private static final List<StoreAdapter<?>> EXACT_ADAPTERS = exactAdapters();

  private final ArchiveReadContext context;
  private final byte[] targetBlockHash;
  private final Limits limits;
  private final long deadlineNanos;
  private long reads;
  private long bytes;
  private boolean closed;

  private HistoricalQuerySession(ArchiveReadContext context, byte[] targetBlockHash,
      Limits limits) {
    this.context = Objects.requireNonNull(context, "context");
    this.targetBlockHash = copyHash(targetBlockHash);
    this.limits = Objects.requireNonNull(limits, "limits");
    this.deadlineNanos = System.nanoTime()
        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis);
    context.requirePinnedIdentity();
  }

  /** Takes ownership of {@code lease}, including if session construction fails. */
  public static HistoricalQuerySession open(ArchiveRuntimeQueryGate.Lease lease,
      byte[] targetBlockHash) throws IOException {
    return open(lease, targetBlockHash, Limits.defaults());
  }

  /** Takes ownership of a common-checkpoint point snapshot. */
  public static HistoricalQuerySession open(StateArchiveCheckpointReadSnapshot snapshot,
      byte[] targetBlockHash) throws IOException {
    return open(snapshot, targetBlockHash, Limits.defaults());
  }

  /** Takes ownership of a common-checkpoint point snapshot. */
  public static HistoricalQuerySession open(StateArchiveCheckpointReadSnapshot snapshot,
      byte[] targetBlockHash, Limits limits) throws IOException {
    ArchiveReadContext context = ArchiveReadContext.open(snapshot, EXACT_ADAPTERS);
    try {
      return new HistoricalQuerySession(context, targetBlockHash, limits);
    } catch (RuntimeException failure) {
      try {
        context.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  /** Takes ownership of {@code lease}, including if session construction fails. */
  public static HistoricalQuerySession open(ArchiveRuntimeQueryGate.Lease lease,
      byte[] targetBlockHash, Limits limits) throws IOException {
    ArchiveReadContext context = ArchiveReadContext.open(lease, EXACT_ADAPTERS);
    try {
      return new HistoricalQuerySession(context, targetBlockHash, limits);
    } catch (RuntimeException failure) {
      try {
        context.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  public long getTargetBlock() {
    ensureOpen();
    return context.getTargetBlock();
  }

  public long getPinnedBlock() {
    ensureOpen();
    return context.getPinnedBlock();
  }

  public byte[] getTargetBlockHash() {
    ensureOpen();
    return Arrays.copyOf(targetBlockHash, targetBlockHash.length);
  }

  public Optional<Account> getAccount(byte[] address) {
    byte[] key = copyKey(address, "address");
    Optional<byte[]> encoded = getExact("account", key);
    if (!encoded.isPresent()) {
      return Optional.empty();
    }
    Account account = decodeAccount(encoded.get());
    if (!account.getAddress().equals(com.google.protobuf.ByteString.copyFrom(key))) {
      throw new ArchivePersistenceException(
          "Historical Account address does not match the physical key");
    }
    return Optional.of(account);
  }

  public Optional<SmartContract> getContract(byte[] address) {
    Optional<byte[]> encoded = getExact("contract", copyKey(address, "address"));
    return encoded.map(HistoricalQuerySession::decodeContract);
  }

  public Optional<byte[]> getCode(byte[] address) {
    return getExact("code", copyKey(address, "address"));
  }

  public Optional<ContractState> getContractState(byte[] address) {
    Optional<byte[]> encoded = getExact("contract-state", copyKey(address, "address"));
    return encoded.map(HistoricalQuerySession::decodeContractState);
  }

  /** Contract metadata and storage-row lookup are resolved by the same pinned context. */
  public Optional<byte[]> getStorage(byte[] contractAddress, byte[] logicalSlot) {
    ensureOpen();
    byte[] address = copyKey(contractAddress, "contractAddress");
    byte[] slot = copyKey(logicalSlot, "logicalSlot");
    SmartContract contract = getContract(address).orElseThrow(() ->
        new ArchivePersistenceException(
            "Historical Contract is required to derive storage-row key"));
    byte[] transactionHash = contract.getTrxHash().isEmpty()
        ? null : contract.getTrxHash().toByteArray();
    byte[] physicalKey = StorageRowKeyCodec.physicalKey(address, slot,
        contract.getVersion(), transactionHash);
    return getExact("storage-row", physicalKey);
  }

  public Optional<byte[]> getExact(String dbName, byte[] physicalRawKey) {
    ensureOpen();
    requireReadBudget();
    try {
      OldValue value = context.getExact(dbName, copyKey(physicalRawKey, "physicalRawKey"));
      if (value.isPresent()) {
        accountBytes(value.getValue().length);
      }
      return value.isPresent() ? Optional.of(value.getValue()) : Optional.empty();
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to read historical Store " + dbName,
          failure);
    }
  }

  public void requirePinnedIdentity() {
    ensureOpen();
    context.requirePinnedIdentity();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      context.close();
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to close historical query session", failure);
    }
  }

  private synchronized void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Historical query session is closed");
    }
  }

  private synchronized void requireReadBudget() {
    if (System.nanoTime() - deadlineNanos > 0) {
      throw new HistoricalQueryBudgetException("Historical query deadline exceeded");
    }
    if (reads >= limits.maxReads) {
      throw new HistoricalQueryBudgetException("Historical query read budget exceeded");
    }
    reads++;
  }

  private synchronized void accountBytes(long additionalBytes) {
    if (additionalBytes > limits.maxBytes - bytes) {
      throw new HistoricalQueryBudgetException("Historical query byte budget exceeded");
    }
    bytes += additionalBytes;
  }

  private static Map<String, StoreAdapter<byte[]>> rawAdapters() {
    Map<String, StoreAdapter<byte[]>> adapters = new LinkedHashMap<>();
    for (String dbName : ArchiveStoreScope.getStateDatabases()) {
      adapters.put(dbName, StoreAdapter.define(dbName, HistoricalQuerySession::copyValue));
    }
    return Collections.unmodifiableMap(adapters);
  }

  private static List<StoreAdapter<?>> exactAdapters() {
    return Collections.unmodifiableList(new ArrayList<>(RAW_ADAPTERS.values()));
  }

  private static Account decodeAccount(byte[] value) {
    try {
      return Account.parseFrom(value);
    } catch (InvalidProtocolBufferException failure) {
      throw new ArchivePersistenceException("Historical Account cannot be decoded", failure);
    }
  }

  private static SmartContract decodeContract(byte[] value) {
    try {
      return SmartContract.parseFrom(value);
    } catch (InvalidProtocolBufferException failure) {
      throw new ArchivePersistenceException("Historical Contract cannot be decoded", failure);
    }
  }

  private static ContractState decodeContractState(byte[] value) {
    try {
      return ContractState.parseFrom(value);
    } catch (InvalidProtocolBufferException failure) {
      throw new ArchivePersistenceException(
          "Historical ContractState cannot be decoded", failure);
    }
  }

  private static byte[] copyHash(byte[] hash) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException("targetBlockHash must be exactly 32 bytes");
    }
    return Arrays.copyOf(hash, hash.length);
  }

  private static byte[] copyKey(byte[] value, String name) {
    return Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
  }

  private static byte[] copyValue(byte[] value) {
    return Arrays.copyOf(value, value.length);
  }
}
