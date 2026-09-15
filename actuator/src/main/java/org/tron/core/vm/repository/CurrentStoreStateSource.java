package org.tron.core.vm.repository;

import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.CodeCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.store.StoreFactory;

/** Existing current Chainbase stores adapted to RepositoryStateSource. */
final class CurrentStoreStateSource implements RepositoryStateSource {

  private final ChainBaseManager manager;

  CurrentStoreStateSource(StoreFactory storeFactory) {
    this.manager = storeFactory.getChainBaseManager();
  }

  @Override
  public AccountCapsule getAccount(byte[] address) {
    return manager.getAccountStore().get(address);
  }

  @Override
  public BytesCapsule getDynamicProperty(byte[] key) {
    try {
      return manager.getDynamicPropertiesStore().get(key);
    } catch (BadItemException | ItemNotFoundException ignored) {
      return null;
    }
  }

  @Override
  public ContractCapsule getContract(byte[] address) {
    return manager.getContractStore().get(address);
  }

  @Override
  public ContractStateCapsule getContractState(byte[] address) {
    return manager.getContractStateStore().get(address);
  }

  @Override
  public byte[] getCode(byte[] address) {
    CodeCapsule code = manager.getCodeStore().get(address);
    return code == null ? null : code.getData();
  }

  @Override
  public StorageRowCapsule getStorageRow(byte[] physicalKey) {
    return manager.getStorageRowStore().get(physicalKey);
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }
}
