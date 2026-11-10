package org.tron.core.archive.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;

/** Exact historical state reads backed only by one request-owned Archive session. */
final class HistoricalArchiveStateSource implements RepositoryStateSource {

  private final HistoricalQuerySession session;
  /**
   * Request-local cache of TRC10 balances resolved at S(T), shared by the root and every child
   * frame: a balance is imported (and charged to the read budget) at most once per request.
   */
  private final java.util.Map<String, Long> accountAssetBalances = new java.util.HashMap<>();
  private final java.util.Set<String> absentProperties = new java.util.HashSet<>();

  HistoricalArchiveStateSource(HistoricalQuerySession session) {
    this.session = java.util.Objects.requireNonNull(session, "session");
    session.requirePinnedIdentity();
  }

  @Override
  public AccountCapsule getAccount(byte[] address) {
    return session.getAccount(address).map(AccountCapsule::new).orElse(null);
  }

  /**
   * ABSENT is cached for the request so repeated reads neither hit the session nor charge the
   * read budget again. A key with a typed-getter default is a legitimate ABSENT and is logged at
   * DEBUG and counted; any other ABSENT key is logged at WARN, rate-limited (section 3.10.5).
   */
  @Override
  public BytesCapsule getDynamicProperty(byte[] key) {
    String name = new String(key, java.nio.charset.StandardCharsets.UTF_8);
    if (absentProperties.contains(name)) {
      return null;
    }
    BytesCapsule value = session.dynamicProperty(key).map(BytesCapsule::new).orElse(null);
    if (value == null) {
      absentProperties.add(name);
      HistoricalPropertyLog.absent(name);
    }
    return value;
  }

  @Override
  public ContractCapsule getContract(byte[] address) {
    return session.getContract(address).map(ContractCapsule::new).orElse(null);
  }

  @Override
  public ContractStateCapsule getContractState(byte[] address) {
    return session.getContractState(address).map(ContractStateCapsule::new).orElse(null);
  }

  @Override
  public byte[] getCode(byte[] address) {
    return session.getCode(address).orElse(null);
  }

  /** VM row loader for the 32-byte storage-row format supported by develop. */
  @Override
  public StorageRowCapsule getStorageRow(byte[] physicalKey) {
    return session.storageRow(physicalKey).map(StorageRowCapsule::new).orElse(null);
  }

  @Override
  public long getAccountAssetBalance(byte[] address, String tokenId) {
    String key = org.bouncycastle.util.encoders.Hex.toHexString(address) + "/" + tokenId;
    Long cached = accountAssetBalances.get(key);
    if (cached == null) {
      cached = session.accountAssetBalance(address, tokenId);
      accountAssetBalances.put(key, cached);
    }
    return cached;
  }

  @Override
  public void checkActive() {
    session.checkActive();
  }

  /** Fork state is evaluated at S(T) through the session's ForkView (section 3.9.3). */
  @Override
  public boolean passesFork(ForkBlockVersionEnum version) {
    return session.forks().pass(version);
  }

  @Override
  public RuntimeException markFailure(RuntimeException failure) {
    return session.markTerminal(failure);
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }
}
