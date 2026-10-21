package org.tron.core.vm.utils;

import org.tron.common.utils.ForkController;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.config.Parameter;
import org.tron.core.db2.archive.query.HistoricalExecutionScope;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.vm.VMUtils;
import org.tron.core.vm.program.Program.OutOfTimeException;
import org.tron.core.vm.repository.Repository;
import org.tron.protos.Protocol;

public class MUtil {


  private MUtil() {
  }

  public static void transfer(Repository deposit, byte[] fromAddress, byte[] toAddress, long amount)
      throws ContractValidateException {
    if (0 == amount) {
      return;
    }
    VMUtils.validateForSmartContract(deposit, fromAddress, toAddress, amount);
    deposit.addBalance(toAddress, amount);
    deposit.addBalance(fromAddress, -amount);
  }

  public static void transferAllToken(Repository deposit, byte[] fromAddress, byte[] toAddress) {
    AccountCapsule fromAccountCap = deposit.getAccount(fromAddress);
    Protocol.Account.Builder fromBuilder = fromAccountCap.getInstance().toBuilder();
    AccountCapsule toAccountCap = deposit.getAccount(toAddress);
    toAccountCap.importAllAsset();
    Protocol.Account.Builder toBuilder = toAccountCap.getInstance().toBuilder();
    fromAccountCap.getAssetMapV2().forEach((tokenId, amount) -> {
      toBuilder.putAssetV2(tokenId, toBuilder.getAssetV2Map().getOrDefault(tokenId, 0L) + amount);
      fromBuilder.putAssetV2(tokenId, 0L);
    });

    deposit.putAccountValue(fromAddress, new AccountCapsule(fromBuilder.build()));
    deposit.putAccountValue(toAddress, new AccountCapsule(toBuilder.build()));
  }

  public static void transferToken(Repository deposit, byte[] fromAddress, byte[] toAddress,
      String tokenId, long amount)
      throws ContractValidateException {
    if (0 == amount) {
      return;
    }
    VMUtils.validateForSmartContract(deposit, fromAddress, toAddress, tokenId.getBytes(), amount);
    deposit.addTokenBalance(toAddress, tokenId.getBytes(), amount);
    deposit.addTokenBalance(fromAddress, tokenId.getBytes(), -amount);
  }

  public static boolean isNullOrEmpty(String str) {
    return (str == null) || str.isEmpty();
  }

  public static boolean isNotNullOrEmpty(String str) {
    return !isNullOrEmpty(str);
  }

  /**
   * Fork gate for the static CPU-time checks. Inside a historical execution the decision is
   * {@code ForkView@S(T)} from the request's scope; latest execution keeps ForkController
   * (chapter 05 section 3.9.3).
   */
  private static boolean passFork(Parameter.ForkBlockVersionEnum version) {
    HistoricalExecutionScope scope = HistoricalExecutionScope.current();
    return scope != null ? scope.forks().pass(version) : ForkController.instance().pass(version);
  }

  public static void checkCPUTimeForContractHashFields() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_2_2)) {
      throw new OutOfTimeException("CPU timeout for contract hash fields");
    }
  }

  public static void checkCPUTime() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_7_1)) {
      throw new OutOfTimeException("CPU timeout for 0x0a executing");
    }
  }

  public static void checkCPUTimeForCreate2() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_1_1)) {
      throw new OutOfTimeException("CPU timeout for create2 executing");
    }
  }

  public static void checkCPUTimeForModExp() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_1_1)) {
      throw new OutOfTimeException("CPU timeout for modExp executing");
    }
  }

  public static void checkCPUTimeForFreezeV2AfterSelfDestruct() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_2_2)) {
      throw new OutOfTimeException("CPU timeout for FreezeBalanceV2 after SELFDESTRUCT");
    }
  }

  public static void checkCPUTimeForSelfDestructedBeneficiary() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_2_2)) {
      throw new OutOfTimeException(
          "CPU timeout for SELFDESTRUCT with selfdestructed beneficiary");
    }
  }

  public static void checkCPUTimeForInvalidDelegatedV2Balance() {
    if (passFork(Parameter.ForkBlockVersionEnum.VERSION_4_8_2_2)) {
      throw new OutOfTimeException("CPU timeout for invalid delegated V2 balance");
    }
  }
}
