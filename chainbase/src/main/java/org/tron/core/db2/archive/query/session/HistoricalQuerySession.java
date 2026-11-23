package org.tron.core.db2.archive.query.session;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.contract.AccountAssetStorage;
import org.tron.core.db2.archive.contract.HistoricalQueryException;
import org.tron.core.db2.archive.history.codec.P66AccountAssetCodec;
import org.tron.core.db2.archive.query.ArchiveRuntimeQueryGate;
import org.tron.core.db2.archive.query.HistoricalQueryBudgetException;
import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;
import org.tron.core.db2.archive.query.HistoricalQueryControl;
import org.tron.core.db2.archive.query.account.HistoricalAccountAssetBalanceResolver;
import org.tron.core.db2.archive.query.session.ArchiveReadContext.StoreAdapter;
import org.tron.core.db2.archive.query.snapshot.ArchivePointSnapshot;
import org.tron.core.db2.archive.query.snapshot.StateArchiveCheckpointReadSnapshot;
import org.tron.core.store.DynamicPropertiesStore;
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
  private static final P66AccountAssetCodec ASSET_CODEC = new P66AccountAssetCodec();
  private static final HistoricalAccountAssetBalanceResolver ACCOUNT_ASSET_RESOLVER =
      new HistoricalAccountAssetBalanceResolver();

  private final ArchiveReadContext context;
  private final byte[] targetBlockHash;
  private final Limits limits;
  private final HistoricalQueryControl control;
  private final Runnable releaseAdmission;
  private long reads;
  private long bytes;
  private boolean closed;
  private HistoricalForkView forkView;
  private boolean executionBound;

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

  /**
   * Opens a session over any point snapshot. Package-private: production code opens sessions only
   * through the common-checkpoint snapshot or a gate lease; tests use this to bind request-owned
   * fakes.
   */
  static HistoricalQuerySession openPoint(ArchivePointSnapshot snapshot, byte[] targetBlockHash,
      Limits limits, HistoricalQueryControl control, Runnable releaseAdmission)
      throws IOException {
    ArchiveReadContext context = ArchiveReadContext.openPoint(snapshot, EXACT_ADAPTERS);
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

  public long getTargetBlock() {
    return guarded(() -> {
      ensureOpen();
      return context.getTargetBlock();
    });
  }

  public long getPinnedBlock() {
    return guarded(() -> {
      ensureOpen();
      return context.getPinnedBlock();
    });
  }

  public byte[] getTargetBlockHash() {
    return guarded(() -> {
      ensureOpen();
      return Arrays.copyOf(targetBlockHash, targetBlockHash.length);
    });
  }

  public Optional<Account> getAccount(byte[] address) {
    return guarded(() -> {
      byte[] key = copyKey(address, "address");
      Optional<byte[]> encoded = readExact("account", key);
      if (!encoded.isPresent()) {
        return Optional.<Account>empty();
      }
      Account account = decodeAccount(encoded.get());
      if (!account.getAddress().equals(com.google.protobuf.ByteString.copyFrom(key))) {
        throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
            "Historical Account address does not match the physical key");
      }
      return Optional.of(account);
    });
  }

  public Optional<SmartContract> getContract(byte[] address) {
    return guarded(() -> readExact("contract", copyKey(address, "address"))
        .map(HistoricalQuerySession::decodeContract));
  }

  public Optional<byte[]> getCode(byte[] address) {
    return guarded(() -> readExact("code", copyKey(address, "address")));
  }

  public Optional<ContractState> getContractState(byte[] address) {
    return guarded(() -> readExact("contract-state", copyKey(address, "address"))
        .map(HistoricalQuerySession::decodeContractState));
  }

  /**
   * Point read of one logical contract slot at S(T): contract metadata and its 32-byte storage
   * row are read through this session, using the same key mapping as develop. An absent contract
   * yields {@link Optional#empty()} (the RPC answers 32 zero bytes).
   */
  public Optional<byte[]> storage(byte[] contractAddress, byte[] logicalSlot) {
    return guarded(() -> {
      byte[] address = copyKey(contractAddress, "contractAddress");
      byte[] slot = copyKey(logicalSlot, "logicalSlot");
      Optional<SmartContract> contract = getContract(address);
      if (!contract.isPresent()) {
        return Optional.<byte[]>empty();
      }
      return resolveStorageSlot(address, slot, contract.get());
    });
  }

  /**
   * Contract metadata and storage-row lookup are resolved by the same request context. Unlike
   * {@link #storage}, a missing contract is a failure. Uses the same develop key mapping.
   */
  public Optional<byte[]> getStorage(byte[] contractAddress, byte[] logicalSlot) {
    return guarded(() -> {
      byte[] address = copyKey(contractAddress, "contractAddress");
      byte[] slot = copyKey(logicalSlot, "logicalSlot");
      SmartContract contract = getContract(address).orElseThrow(() ->
          new ArchivePersistenceException(
              "Historical Contract is required to derive storage-row key"));
      return resolveStorageSlot(address, slot, contract);
    });
  }

  /**
   * VM row loader: one exact storage-row read. Only the 32-byte develop key format is
   * understood; any other width fails closed as {@code CAPABILITY(UNSUPPORTED_STORAGE_KEY_FORMAT)}.
   */
  public Optional<byte[]> storageRow(byte[] physicalKey) {
    return guarded(() -> {
      byte[] key = copyKey(physicalKey, "physicalKey");
      requireStorageRowKey(key);
      return readExact("storage-row", key);
    });
  }

  /**
   * One TRC10 balance at S(T) with upstream {@code AssetUtil.importAsset} semantics (chapter 05
   * section 3.11). Always charges exactly three point reads ({@code properties},
   * {@code account}, {@code account-asset}); an absent Account answers 0. Integrity violations
   * fail closed as {@code CORRUPT}.
   */
  public long accountAssetBalance(byte[] address, String tokenId) {
    return guarded(() -> {
      byte[] owner = copyKey(address, "address");
      byte[] directKey;
      try {
        directKey = ASSET_CODEC.assetPhysicalKey(owner, tokenId);
      } catch (ArchivePersistenceException invalid) {
        throw new HistoricalQueryCapabilityException(
            HistoricalQueryCapabilityException.UNSUPPORTED,
            "Unsupported historical TRC10 identity: " + invalid.getMessage());
      }
      OldValue property = OldValue.fromNullable(readExact("properties",
          AccountAssetStorage.proposal66PhysicalKey()).orElse(null));
      OldValue account = OldValue.fromNullable(readExact("account", owner).orElse(null));
      OldValue direct = OldValue.fromNullable(readExact("account-asset", directKey)
          .orElse(null));
      if (!property.isPresent()) {
        // A required (orElseThrow) property absent at S(T): section 3.10.1.
        throw new HistoricalQueryCapabilityException(
            HistoricalQueryCapabilityException.UNSUPPORTED_STATE,
            "Dynamic property is missing at the historical block: ALLOW_ASSET_OPTIMIZATION");
      }
      HistoricalAccountAssetBalanceResolver.Result result;
      try {
        result = ACCOUNT_ASSET_RESOLVER.resolveValues(context.getTargetBlock(), owner, tokenId,
            directKey, property, account, direct);
      } catch (HistoricalQueryException typed) {
        throw typed;
      } catch (ArchivePersistenceException invalid) {
        throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
            invalid.getMessage(), invalid);
      }
      return result.isAccountPresent() ? result.getBalance() : 0L;
    });
  }

  /** Raw dynamic-property read at S(T); ABSENT is {@link Optional#empty()}. */
  public Optional<byte[]> dynamicProperty(byte[] key) {
    return guarded(() -> readExact("properties", copyKey(key, "key")));
  }

  /**
   * Typed long dynamic property at S(T) (chapter 05 section 3.10.1): PRESENT uses the stored value;
   * ABSENT uses the typed getter's default when one exists; otherwise fails closed as
   * {@code CAPABILITY(UNSUPPORTED_STATE)}. Never falls back to latest.
   */
  public long dynamicPropertyLong(String key) {
    return guarded(() -> {
      Optional<byte[]> value = readExact("properties",
          Objects.requireNonNull(key, "key").getBytes(StandardCharsets.UTF_8));
      if (value.isPresent()) {
        return ByteArray.toLong(value.get());
      }
      Long defaultValue = DynamicPropertiesStore.getLongPropertyDefault(key);
      if (defaultValue == null) {
        throw new HistoricalQueryCapabilityException(
            HistoricalQueryCapabilityException.UNSUPPORTED_STATE,
            "Dynamic property is missing at the historical block: " + key);
      }
      return defaultValue;
    });
  }

  /** Fork-activation decisions evaluated at S(T), cached for this request. */
  public boolean passesFork(org.tron.core.config.Parameter.ForkBlockVersionEnum version) {
    return forks().pass(version);
  }

  public org.tron.common.utils.ForkView forks() {
    return guarded(() -> {
      ensureOpen();
      if (forkView == null) {
        forkView = new HistoricalForkView(this);
      }
      return forkView;
    });
  }

  public Optional<byte[]> getExact(String dbName, byte[] physicalRawKey) {
    return guarded(() -> readExact(dbName, physicalRawKey));
  }

  public void requirePinnedIdentity() {
    guarded(() -> {
      ensureOpen();
      context.requirePinnedIdentity();
      return null;
    });
  }

  /** Also called for pure-compute VM execution, which may perform no Store reads. */
  public void checkActive() {
    guarded(() -> {
      ensureOpen();
      control.checkActive();
      return null;
    });
  }

  /**
   * Records {@code failure} as this request's TERMINAL cause (first cause wins) and returns it.
   * Every later session method and {@link #checkActive()} rethrows the recorded cause.
   */
  public RuntimeException markTerminal(RuntimeException failure) {
    return control.markTerminal(failure);
  }

  public boolean isTerminal() {
    return control.isTerminal();
  }

  public Optional<RuntimeException> terminalFailure() {
    return control.terminalFailure();
  }

  /** A session drives at most one VM execution; a second binding is a programming error. */
  public synchronized void bindExecution() {
    ensureOpen();
    if (executionBound) {
      throw new IllegalStateException("Historical query session is already bound to an execution");
    }
    executionBound = true;
  }

  /** Number of logical point reads charged to this request so far. */
  public synchronized long getReadCount() {
    return reads;
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

  private Optional<byte[]> readExact(String dbName, byte[] physicalRawKey) {
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
  }

  private Optional<byte[]> resolveStorageSlot(byte[] address, byte[] slot,
      SmartContract contract) {
    byte[] transactionHash = contract.getTrxHash().isEmpty()
        ? null : contract.getTrxHash().toByteArray();
    byte[] addressHash = StorageRowKeyCodec.addressHash(address, transactionHash);
    return readExact("storage-row", StorageRowKeyCodec.physicalKeyFromAddressHash(addressHash,
        slot, contract.getVersion()));
  }

  static void requireStorageRowKey(byte[] key) {
    if (key.length != StorageRowKeyCodec.KEY_BYTES) {
      throw new HistoricalQueryCapabilityException(
          HistoricalQueryCapabilityException.UNSUPPORTED_STORAGE_KEY_FORMAT,
          "Historical storage-row key must be 32 bytes, got " + key.length);
    }
  }

  /**
   * Chapter 05 section 3.7 P1: every failure escaping a session method is recorded as the
   * request's TERMINAL cause before it is rethrown, so a VM frame that swallows it cannot let the
   * request finish with a REVERT-shaped or success-shaped answer.
   */
  private <T> T guarded(java.util.function.Supplier<T> body) {
    try {
      return body.get();
    } catch (RuntimeException failure) {
      throw control.markTerminal(failure);
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
      throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
          "Historical Account cannot be decoded", failure);
    }
  }

  private static SmartContract decodeContract(byte[] value) {
    try {
      return SmartContract.parseFrom(value);
    } catch (InvalidProtocolBufferException failure) {
      throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
          "Historical Contract cannot be decoded", failure);
    }
  }

  private static ContractState decodeContractState(byte[] value) {
    try {
      return ContractState.parseFrom(value);
    } catch (InvalidProtocolBufferException failure) {
      throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
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
