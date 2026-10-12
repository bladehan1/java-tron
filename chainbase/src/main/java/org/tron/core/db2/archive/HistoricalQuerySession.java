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

    public long getTimeoutMillis() {
      return timeoutMillis;
    }
  }

  private static final Map<String, StoreAdapter<byte[]>> RAW_ADAPTERS = rawAdapters();
  private static final List<StoreAdapter<?>> EXACT_ADAPTERS = exactAdapters();

  private final ArchiveReadContext context;
  private final byte[] targetBlockHash;
  private final Limits limits;
  private final HistoricalQueryControl control;
  private final Runnable releaseAdmission;
  private long reads;
  private long bytes;
  private boolean closed;

  private HistoricalQuerySession(ArchiveReadContext context, byte[] targetBlockHash,
      Limits limits, HistoricalQueryControl control, Runnable releaseAdmission) {
    this.context = Objects.requireNonNull(context, "context");
    this.targetBlockHash = copyHash(targetBlockHash);
    this.limits = Objects.requireNonNull(limits, "limits");
    this.control = Objects.requireNonNull(control, "control");
    this.releaseAdmission = Objects.requireNonNull(releaseAdmission, "releaseAdmission");
    control.checkActive();
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
    return open(snapshot, targetBlockHash, limits,
        new HistoricalQueryControl(limits.timeoutMillis), () -> { });
  }

  /** Request admission remains owned by the caller until this method returns successfully. */
  public static HistoricalQuerySession open(StateArchiveCheckpointReadSnapshot snapshot,
      byte[] targetBlockHash, Limits limits, HistoricalQueryControl control,
      Runnable releaseAdmission) throws IOException {
    ArchiveReadContext context = ArchiveReadContext.open(snapshot, EXACT_ADAPTERS);
    try {
      return new HistoricalQuerySession(context, targetBlockHash, limits, control,
          releaseAdmission);
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
    return open(lease, targetBlockHash, limits,
        new HistoricalQueryControl(limits.timeoutMillis), () -> { });
  }

  public static HistoricalQuerySession open(ArchiveRuntimeQueryGate.Lease lease,
      byte[] targetBlockHash, Limits limits, HistoricalQueryControl control,
      Runnable releaseAdmission) throws IOException {
    ArchiveReadContext context = ArchiveReadContext.open(lease, EXACT_ADAPTERS);
    try {
      return new HistoricalQuerySession(context, targetBlockHash, limits, control,
          releaseAdmission);
    } catch (RuntimeException failure) {
      try {
        context.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  static HistoricalQuerySession openPoint(ArchivePointSnapshot snapshot, byte[] targetBlockHash,
      Limits limits, HistoricalQueryControl control, Runnable releaseAdmission) throws IOException {
    ArchiveReadContext context = ArchiveReadContext.openPoint(snapshot, EXACT_ADAPTERS);
    try {
      return new HistoricalQuerySession(context, targetBlockHash, limits, control, releaseAdmission);
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
    try {
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

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  public Optional<SmartContract> getContract(byte[] address) {
    try {
      Optional<byte[]> encoded = getExact("contract", copyKey(address, "address"));
      return encoded.map(HistoricalQuerySession::decodeContract);

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  public Optional<byte[]> getCode(byte[] address) {
    try {
      return getExact("code", copyKey(address, "address"));

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  public Optional<ContractState> getContractState(byte[] address) {
    try {
      Optional<byte[]> encoded = getExact("contract-state", copyKey(address, "address"));
      return encoded.map(HistoricalQuerySession::decodeContractState);

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  /** Contract metadata and storage-row lookup are resolved by the same request context. */
  public Optional<byte[]> getStorage(byte[] contractAddress, byte[] logicalSlot) {
    try {
      ensureOpen();
      byte[] address = copyKey(contractAddress, "contractAddress");
      byte[] slot = copyKey(logicalSlot, "logicalSlot");
      Optional<SmartContract> metadata = getContract(address);
      if (!metadata.isPresent()) {
        return Optional.empty();
      }
      SmartContract contract = metadata.get();
      byte[] transactionHash = contract.getTrxHash().toByteArray();
      if (dynamicLong("ALLOW_OPTIMIZE_TVM_STORAGE", 0L) == 1L) {
        Optional<byte[]> optimized = getExact("storage-row",
            StorageRowKeyCodec.optimizedKey(address, slot, transactionHash));
        // PRESENT zero is authoritative; only ABSENT falls back to the legacy row.
        if (optimized.isPresent()) {
          return optimized;
        }
      }
      return getExact("storage-row", StorageRowKeyCodec.physicalKey(address, slot,
          contract.getVersion(), transactionHash));
    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  public Optional<byte[]> getExact(String dbName, byte[] physicalRawKey) {
    try {
      ensureOpen();
      requireReadBudget();
      try {
        OldValue value = context.getExact(dbName, copyKey(physicalRawKey, "physicalRawKey"));
        control.checkActive();
        if (value.isPresent()) {
          accountBytes(value.getValue().length);
        }
        return value.isPresent() ? Optional.of(value.getValue()) : Optional.empty();
      } catch (IOException failure) {
        throw new HistoricalQueryException(HistoricalQueryException.Reason.DATA_ACCESS,
            "Failed to read historical Store " + dbName, failure);
      }

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  public void requirePinnedIdentity() {
    try {
      ensureOpen();
      context.requirePinnedIdentity();

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  /** Also called for pure-compute VM execution, which may perform no Store reads. */
  public void checkActive() {
    try {
      ensureOpen();
      control.checkActive();

    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  /** Evaluate fork activation using the same rule as latest, with inputs pinned at S(T). */
  public boolean passesFork(org.tron.core.config.Parameter.ForkBlockVersionEnum version) {
    try {
      ensureOpen();
      return org.tron.common.utils.ForkController.evaluate(version.getValue(),
          new org.tron.common.utils.ForkStateSource() {
            @Override
            public long latestBlockHeaderTimestamp() {
              return dynamicLong("latest_block_header_timestamp", null);
            }

            @Override
            public long maintenanceTimeInterval() {
              return dynamicLong("MAINTENANCE_TIME_INTERVAL", null);
            }

            @Override
            public byte[] statsByVersion(int forkVersion) {
              return getExact("properties", ("FORK_VERSION_" + forkVersion)
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8)).orElse(null);
            }

            @Override
            public boolean disableJavaLangMath() {
              return dynamicLong("CONSENSUS_LOGIC_OPTIMIZATION", 0L) == 1L;
            }

            @Override
            public long latestBlockHeaderNumber() {
              return dynamicLong("latest_block_header_number", null);
            }
          });
    } catch (RuntimeException failure) {
      throw markTerminal(failure);
    }
  }

  private long dynamicLong(String key, Long defaultValue) {
    Optional<byte[]> value = getExact("properties",
        key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    if (!value.isPresent()) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw new ArchivePersistenceException("Missing historical fork input: " + key);
    }
    if (value.get().length != Long.BYTES) {
      throw new ArchivePersistenceException("Invalid historical property: " + key);
    }
    return com.google.common.primitives.Longs.fromByteArray(value.get());
  }

  public RuntimeException markTerminal(RuntimeException failure) {
    return control.markTerminal(failure);
  }

  public void cancel() {
    control.cancel();
  }

  @Override
  public synchronized void close() {
    control.requireOwner();
    if (closed) {
      return;
    }
    closed = true;
    try {
      context.close();
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to close historical query session", failure);
    } finally {
      releaseAdmission.run();
    }
  }

  private synchronized void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Historical query session is closed");
    }
    control.checkActive();
  }

  private synchronized void requireReadBudget() {
    control.checkActive();
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
