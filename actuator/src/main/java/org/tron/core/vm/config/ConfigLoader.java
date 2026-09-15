package org.tron.core.vm.config;

import lombok.extern.slf4j.Slf4j;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.StoreFactory;
import org.tron.core.vm.HistoricalCapabilityException;
import org.tron.core.vm.repository.Repository;

@Slf4j(topic = "VMConfigLoader")
public class ConfigLoader {

  //only for unit test
  public static boolean disable = false;

  // isolate=true: a constant call bound to a non-HEAD (solidity/PBFT) snapshot installs its
  // snapshot into a thread-local view instead of the process-wide global, so it cannot pollute
  // the flags the block-processing path reads concurrently.
  public static void load(StoreFactory storeFactory, boolean isolate) {
    if (!disable) {
      DynamicPropertiesStore ds = storeFactory.getChainBaseManager().getDynamicPropertiesStore();
      VMConfig.setVmTrace(CommonParameter.getInstance().isVmTrace());
      if (ds != null) {
        VMConfig.Snapshot snapshot = new VMConfig.Snapshot();
        snapshot.energyLimitHardFork = ds.getLatestBlockHeaderNumber()
            >= CommonParameter.getInstance().getBlockNumForEnergyLimit();
        snapshot.allowMultiSign = ds.getAllowMultiSign() == 1;
        snapshot.allowTvmTransferTrc10 = ds.getAllowTvmTransferTrc10() == 1;
        snapshot.allowTvmConstantinople = ds.getAllowTvmConstantinople() == 1;
        snapshot.allowTvmSolidity059 = ds.getAllowTvmSolidity059() == 1;
        snapshot.allowShieldedTRC20Transaction = ds.getAllowShieldedTRC20Transaction() == 1;
        snapshot.allowTvmIstanbul = ds.getAllowTvmIstanbul() == 1;
        snapshot.allowTvmFreeze = ds.getAllowTvmFreeze() == 1;
        snapshot.allowTvmVote = ds.getAllowTvmVote() == 1;
        snapshot.allowTvmLondon = ds.getAllowTvmLondon() == 1;
        snapshot.allowTvmCompatibleEvm = ds.getAllowTvmCompatibleEvm() == 1;
        snapshot.allowHigherLimitForMaxCpuTimeOfOneTx =
            ds.getAllowHigherLimitForMaxCpuTimeOfOneTx() == 1;
        snapshot.allowTvmFreezeV2 = ds.supportUnfreezeDelay();
        snapshot.allowOptimizedReturnValueOfChainId = ds.getAllowOptimizedReturnValueOfChainId() == 1;
        snapshot.allowDynamicEnergy = ds.getAllowDynamicEnergy() == 1;
        snapshot.dynamicEnergyThreshold = ds.getDynamicEnergyThreshold();
        snapshot.dynamicEnergyIncreaseFactor = ds.getDynamicEnergyIncreaseFactor();
        snapshot.dynamicEnergyMaxFactor = ds.getDynamicEnergyMaxFactor();
        snapshot.allowTvmShanghai = ds.getAllowTvmShangHai() == 1;
        snapshot.allowEnergyAdjustment = ds.getAllowEnergyAdjustment() == 1;
        snapshot.allowStrictMath = ds.getAllowStrictMath() == 1;
        snapshot.allowTvmCancun = ds.getAllowTvmCancun() == 1;
        snapshot.disableJavaLangMath = ds.getConsensusLogicOptimization() == 1;
        snapshot.allowTvmBlob = ds.getAllowTvmBlob() == 1;
        snapshot.allowTvmSelfdestructRestriction = ds.getAllowTvmSelfdestructRestriction() == 1;
        snapshot.allowTvmOsaka = ds.getAllowTvmOsaka() == 1;
        snapshot.allowHardenResourceCalculation = ds.getAllowHardenResourceCalculation() == 1;
        if (isolate) {
          VMConfig.setLocalSnapshot(snapshot);
        } else {
          VMConfig.initVmHardFork(snapshot.energyLimitHardFork);
          VMConfig.setGlobalSnapshot(snapshot);
        }
      }
    }
  }

  /** Loads an isolated VM view exclusively from a Repository's request-owned state source. */
  public static void load(Repository repository) {
    if (disable) {
      throw new HistoricalCapabilityException(
          "Historical VM config loading cannot be disabled");
    }
    VMConfig.setVmTrace(CommonParameter.getInstance().isVmTrace());
    VMConfig.Snapshot snapshot = new VMConfig.Snapshot();
    snapshot.energyLimitHardFork = property(repository, "latest_block_header_number")
        >= CommonParameter.getInstance().getBlockNumForEnergyLimit();
    snapshot.allowMultiSign = enabled(repository, "ALLOW_MULTI_SIGN");
    snapshot.allowTvmTransferTrc10 = enabled(repository, "ALLOW_TVM_TRANSFER_TRC10");
    snapshot.allowTvmConstantinople = enabled(repository, "ALLOW_TVM_CONSTANTINOPLE");
    snapshot.allowTvmSolidity059 = enabled(repository, "ALLOW_TVM_SOLIDITY_059");
    snapshot.allowShieldedTRC20Transaction =
        enabled(repository, "ALLOW_SHIELDED_TRC20_TRANSACTION");
    snapshot.allowTvmIstanbul = enabled(repository, "ALLOW_TVM_ISTANBUL");
    snapshot.allowTvmFreeze = enabled(repository, "ALLOW_TVM_FREEZE");
    snapshot.allowTvmVote = enabled(repository, "ALLOW_TVM_VOTE");
    snapshot.allowTvmLondon = enabled(repository, "ALLOW_TVM_LONDON");
    snapshot.allowTvmCompatibleEvm = enabled(repository, "ALLOW_TVM_COMPATIBLE_EVM");
    snapshot.allowHigherLimitForMaxCpuTimeOfOneTx =
        enabled(repository, "ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX");
    snapshot.allowTvmFreezeV2 = property(repository, "UNFREEZE_DELAY_DAYS") > 0;
    snapshot.allowOptimizedReturnValueOfChainId =
        enabled(repository, "ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID");
    snapshot.allowDynamicEnergy = enabled(repository, "ALLOW_DYNAMIC_ENERGY");
    snapshot.dynamicEnergyThreshold = property(repository, "DYNAMIC_ENERGY_THRESHOLD");
    snapshot.dynamicEnergyIncreaseFactor = property(repository, "DYNAMIC_ENERGY_INCREASE_FACTOR");
    snapshot.dynamicEnergyMaxFactor = property(repository, "DYNAMIC_ENERGY_MAX_FACTOR");
    snapshot.allowTvmShanghai = enabled(repository, "ALLOW_TVM_SHANGHAI");
    snapshot.allowEnergyAdjustment = enabled(repository, "ALLOW_ENERGY_ADJUSTMENT");
    snapshot.allowStrictMath = enabled(repository, "ALLOW_STRICT_MATH");
    snapshot.allowTvmCancun = enabled(repository, "ALLOW_TVM_CANCUN");
    snapshot.disableJavaLangMath = enabled(repository, "CONSENSUS_LOGIC_OPTIMIZATION");
    snapshot.allowTvmBlob = enabled(repository, "ALLOW_TVM_BLOB");
    snapshot.allowTvmSelfdestructRestriction =
        enabled(repository, "ALLOW_TVM_SELFDESTRUCT_RESTRICTION");
    snapshot.allowTvmOsaka = enabled(repository, "ALLOW_TVM_OSAKA");
    snapshot.allowHardenResourceCalculation =
        enabled(repository, "ALLOW_HARDEN_RESOURCE_CALCULATION");
    VMConfig.setLocalSnapshot(snapshot);
  }

  public static long property(Repository repository, String key) {
    return repository.getDynamicPropertyLong(key);
  }

  private static boolean enabled(Repository repository, String key) {
    return property(repository, key) == 1L;
  }
}
