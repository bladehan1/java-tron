package org.tron.core.archive.repository;

import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.vm.repository.Repository;

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

  /**
   * One TRC10 balance at this source's state with upstream {@code AssetUtil.importAsset}
   * semantics (chapter 05 section 3.11). Only historical sources answer it; current execution
   * keeps the upstream AccountCapsule path.
   */
  default long getAccountAssetBalance(byte[] address, String tokenId) {
    throw new IllegalStateException("AccountAsset balance is resolved by AccountCapsule here");
  }

  default void checkActive() {
  }

  /** Records a historical-class failure as TERMINAL (historical) and returns it. */
  default RuntimeException markFailure(RuntimeException failure) {
    return failure;
  }

  boolean isReadOnly();
}
