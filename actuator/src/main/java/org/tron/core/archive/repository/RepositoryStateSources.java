package org.tron.core.archive.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.store.StoreFactory;

/** Read boundary used by Repository overlays; source implementations remain package private. */
public final class RepositoryStateSources {

  private final RepositoryStateSource source;

  private RepositoryStateSources(RepositoryStateSource source) {
    this.source = source;
  }

  public static RepositoryStateSources current(StoreFactory storeFactory) {
    return new RepositoryStateSources(new CurrentStoreStateSource(storeFactory));
  }

  public static RepositoryStateSources historical(HistoricalQuerySession session) {
    return new RepositoryStateSources(new HistoricalArchiveStateSource(session));
  }

  public AccountCapsule getAccount(byte[] address) {
    return source.getAccount(address);
  }

  public BytesCapsule getDynamicProperty(byte[] key) {
    return source.getDynamicProperty(key);
  }

  public ContractCapsule getContract(byte[] address) {
    return source.getContract(address);
  }

  public ContractStateCapsule getContractState(byte[] address) {
    return source.getContractState(address);
  }

  public byte[] getCode(byte[] address) {
    return source.getCode(address);
  }

  public StorageRowCapsule getStorageRow(byte[] physicalKey) {
    return source.getStorageRow(physicalKey);
  }

  public boolean passesFork(ForkBlockVersionEnum version) {
    return source.passesFork(version);
  }

  public long getAccountAssetBalance(byte[] address, String tokenId) {
    return source.getAccountAssetBalance(address, tokenId);
  }

  public void checkActive() {
    source.checkActive();
  }

  public RuntimeException markFailure(RuntimeException failure) {
    return source.markFailure(failure);
  }

  public boolean isReadOnly() {
    return source.isReadOnly();
  }
}
