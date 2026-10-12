package org.tron.core.vm.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;

/** Root-miss state reads for a Repository overlay. */
interface RepositoryStateSource {

  AccountCapsule getAccount(byte[] address);

  BytesCapsule getDynamicProperty(byte[] key);

  ContractCapsule getContract(byte[] address);

  ContractStateCapsule getContractState(byte[] address);

  byte[] getCode(byte[] address);

  StorageRowCapsule getStorageRow(byte[] physicalKey);

  /** Whether the given fork version has passed at this source's state. */
  boolean passesFork(ForkBlockVersionEnum version);

  default RuntimeException markFailure(RuntimeException failure) {
    return failure;
  }

  default void checkActive() {
  }

  boolean isReadOnly();
}
