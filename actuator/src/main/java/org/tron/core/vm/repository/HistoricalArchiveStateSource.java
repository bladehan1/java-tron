package org.tron.core.vm.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.db2.archive.HistoricalQuerySession;

/** Exact historical state reads backed only by one request-owned Archive session. */
final class HistoricalArchiveStateSource implements RepositoryStateSource {

  private final HistoricalQuerySession session;

  HistoricalArchiveStateSource(HistoricalQuerySession session) {
    this.session = java.util.Objects.requireNonNull(session, "session");
    session.requirePinnedIdentity();
  }

  @Override
  public AccountCapsule getAccount(byte[] address) {
    return session.getAccount(address).map(AccountCapsule::new).orElse(null);
  }

  @Override
  public BytesCapsule getDynamicProperty(byte[] key) {
    return session.getExact("properties", key).map(BytesCapsule::new).orElse(null);
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

  @Override
  public StorageRowCapsule getStorageRow(byte[] physicalKey) {
    return session.getExact("storage-row", physicalKey)
        .map(StorageRowCapsule::new).orElse(null);
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }
}
