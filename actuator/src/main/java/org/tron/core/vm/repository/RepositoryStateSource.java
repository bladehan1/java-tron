package org.tron.core.vm.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;

/** Root-miss state reads for a Repository overlay. */
interface RepositoryStateSource {

  AccountCapsule getAccount(byte[] address);

  BytesCapsule getDynamicProperty(byte[] key);

  ContractCapsule getContract(byte[] address);

  ContractStateCapsule getContractState(byte[] address);

  byte[] getCode(byte[] address);

  StorageRowCapsule getStorageRow(byte[] physicalKey);

  boolean isReadOnly();
}
