package org.tron.core.db2.archive.execution;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.query.HistoricalQueryControl;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.db2.archive.query.session.HistoricalSessionTestSupport;
import org.tron.core.db2.archive.query.snapshot.ArchivePointSnapshot;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

/**
 * Request-owned in-memory S(T) for historical query and execution tests: a session fake behind
 * the real {@link HistoricalQuerySession} (budget, TERMINAL marking, typed reads). Every point
 * read is counted per (store, key) and can be failed or observed.
 */
final class HistoricalTestState implements ArchivePointSnapshot {

  static final long TARGET_BLOCK = 10L;

  private final Map<String, Map<String, byte[]>> rows = new HashMap<>();
  private final Map<String, Integer> reads = new HashMap<>();
  private final List<String> readLog = new ArrayList<>();
  private final Map<String, Supplier<RuntimeException>> runtimeFailures = new HashMap<>();
  private final Map<String, IOException> ioFailures = new HashMap<>();
  private BiConsumer<String, byte[]> onRead = (db, key) -> { };
  private Long anyProperty;
  boolean closed;

  static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[19] = 0x77;
    address[20] = (byte) suffix;
    return address;
  }

  static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String id(String db, byte[] key) {
    return db + ":" + Hex.toHexString(key);
  }

  HistoricalTestState put(String db, byte[] key, byte[] value) {
    rows.computeIfAbsent(db, ignored -> new HashMap<>())
        .put(Hex.toHexString(key), Arrays.copyOf(value, value.length));
    return this;
  }

  HistoricalTestState remove(String db, byte[] key) {
    rows.computeIfAbsent(db, ignored -> new HashMap<>()).remove(Hex.toHexString(key));
    return this;
  }

  HistoricalTestState property(String key, long value) {
    return put("properties", bytes(key), ByteArray.fromLong(value));
  }

  HistoricalTestState removeProperty(String key) {
    return remove("properties", bytes(key));
  }

  HistoricalTestState failWith(String db, byte[] key, Supplier<RuntimeException> failure) {
    runtimeFailures.put(id(db, key), failure);
    return this;
  }

  HistoricalTestState failWithIo(String db, byte[] key, IOException failure) {
    ioFailures.put(id(db, key), failure);
    return this;
  }

  /** Every ABSENT property key answers {@code value} (field-alignment probes). */
  HistoricalTestState anyProperty(long value) {
    this.anyProperty = value;
    return this;
  }

  HistoricalTestState onRead(BiConsumer<String, byte[]> hook) {
    this.onRead = hook;
    return this;
  }

  int reads(String db, byte[] key) {
    return reads.getOrDefault(id(db, key), 0);
  }

  int readsOf(String db) {
    int total = 0;
    for (Map.Entry<String, Integer> entry : reads.entrySet()) {
      if (entry.getKey().startsWith(db + ":")) {
        total += entry.getValue();
      }
    }
    return total;
  }

  List<String> readLog() {
    return new ArrayList<>(readLog);
  }

  /** The VM switches and fork-evaluation inputs a historical execution needs at S(T). */
  HistoricalTestState vmDefaults() {
    String[] enabled = {
        "ALLOW_MULTI_SIGN", "ALLOW_TVM_TRANSFER_TRC10", "ALLOW_TVM_CONSTANTINOPLE",
        "ALLOW_TVM_SOLIDITY_059", "ALLOW_SHIELDED_TRC20_TRANSACTION",
        "ALLOW_TVM_ISTANBUL", "ALLOW_TVM_FREEZE", "ALLOW_TVM_VOTE", "ALLOW_TVM_LONDON",
        "ALLOW_TVM_COMPATIBLE_EVM", "ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX",
        "ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID", "ALLOW_TVM_SHANGHAI",
        "ALLOW_ENERGY_ADJUSTMENT", "ALLOW_STRICT_MATH", "ALLOW_TVM_CANCUN",
        "CONSENSUS_LOGIC_OPTIMIZATION", "ALLOW_TVM_BLOB", "ALLOW_CREATION_OF_CONTRACTS",
        "ALLOW_ASSET_OPTIMIZATION"
    };
    for (String key : enabled) {
      property(key, 1L);
    }
    String[] disabled = {
        "ALLOW_DYNAMIC_ENERGY", "ALLOW_TVM_SELFDESTRUCT_RESTRICTION", "ALLOW_TVM_OSAKA",
        "ALLOW_HARDEN_RESOURCE_CALCULATION"
    };
    for (String key : disabled) {
      property(key, 0L);
    }
    property("latest_block_header_number", TARGET_BLOCK);
    property("latest_block_header_timestamp", 1_000_000L);
    property("MAINTENANCE_TIME_INTERVAL", 21_600_000L);
    property("UNFREEZE_DELAY_DAYS", 14L);
    property("DYNAMIC_ENERGY_THRESHOLD", 0L);
    property("DYNAMIC_ENERGY_INCREASE_FACTOR", 0L);
    property("DYNAMIC_ENERGY_MAX_FACTOR", 0L);
    property("ENERGY_FEE", 420L);
    property("MAX_FEE_LIMIT", 15_000_000_000L);
    property("MAX_CPU_TIME_OF_ONE_TX", 80L);
    property("CURRENT_CYCLE_NUMBER", 7L);
    return this;
  }

  HistoricalTestState account(byte[] address, long balance) {
    return put("account", address, Account.newBuilder()
        .setAddress(ByteString.copyFrom(address)).setType(AccountType.Normal)
        .setAssetOptimized(true).setBalance(balance).build().toByteArray());
  }

  HistoricalTestState account(Account account) {
    return put("account", account.getAddress().toByteArray(), account.toByteArray());
  }

  /** A deployed contract at S(T): Account, SmartContract (version 0, no trx hash) and code. */
  HistoricalTestState contract(byte[] address, byte[] code) {
    return contract(address, code, SmartContract.newBuilder()
        .setContractAddress(ByteString.copyFrom(address))
        .setOriginAddress(ByteString.copyFrom(address))
        .setConsumeUserResourcePercent(100).setVersion(0).build());
  }

  HistoricalTestState contract(byte[] address, byte[] code, SmartContract contract) {
    put("account", address, Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Contract).setAssetOptimized(true).build().toByteArray());
    put("contract", address, contract.toByteArray());
    return put("code", address, code);
  }

  HistoricalQuerySession open() throws IOException {
    return open(HistoricalQuerySession.Limits.defaults());
  }

  HistoricalQuerySession open(HistoricalQuerySession.Limits limits) throws IOException {
    return HistoricalSessionTestSupport.openPoint(this, hash((int) TARGET_BLOCK), limits,
        new HistoricalQueryControl(limits.getTimeoutMillis()), () -> { });
  }

  @Override
  public OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
    String id = id(dbName, physicalRawKey);
    reads.merge(id, 1, Integer::sum);
    readLog.add(id);
    onRead.accept(dbName, physicalRawKey);
    IOException io = ioFailures.get(id);
    if (io != null) {
      throw io;
    }
    Supplier<RuntimeException> failure = runtimeFailures.get(id);
    if (failure != null) {
      throw failure.get();
    }
    Map<String, byte[]> table = rows.get(dbName);
    byte[] value = table == null ? null : table.get(Hex.toHexString(physicalRawKey));
    if (value == null && anyProperty != null && "properties".equals(dbName)) {
      value = ByteArray.fromLong(anyProperty);
    }
    return value == null ? OldValue.absent() : OldValue.present(Arrays.copyOf(value,
        value.length));
  }

  @Override
  public long getTargetBlock() {
    return TARGET_BLOCK;
  }

  @Override
  public long getPinnedBlock() {
    return TARGET_BLOCK + 2;
  }

  @Override
  public byte[] getPinnedHash() {
    return hash((int) TARGET_BLOCK + 2);
  }

  @Override
  public void requirePinnedIdentity() {
  }

  @Override
  public void close() {
    closed = true;
  }
}
