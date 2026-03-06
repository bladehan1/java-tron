package org.tron.core.db;

import static org.tron.common.math.Maths.max;
import static org.tron.common.math.Maths.min;
import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;
import static org.tron.core.config.Parameter.ChainConstant.TRX_PRECISION;
import static org.tron.protos.contract.Common.ResourceCode.ENERGY;


import lombok.extern.slf4j.Slf4j;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.Parameter.AdaptiveResourceLimitConstants;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.store.AccountStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.Protocol.Account.AccountResource;

@Slf4j(topic = "DB")
public class EnergyProcessor extends ResourceProcessor {

  public EnergyProcessor(DynamicPropertiesStore dynamicPropertiesStore, AccountStore accountStore) {
    super(dynamicPropertiesStore, accountStore);
  }

  public static long getHeadSlot(DynamicPropertiesStore dynamicPropertiesStore) {
    return (dynamicPropertiesStore.getLatestBlockHeaderTimestamp() -
        Long.parseLong(CommonParameter.getInstance()
            .getGenesisBlock().getTimestamp()))
        / BLOCK_PRODUCED_INTERVAL;
  }

  public void updateUsage(AccountCapsule accountCapsule) {
    long now = getHeadSlot();
    updateUsage(accountCapsule, now);
  }

  private void updateUsage(AccountCapsule accountCapsule, long now) {
    AccountResource accountResource = accountCapsule.getAccountResource();

    long oldEnergyUsage = accountResource.getEnergyUsage();
    long latestConsumeTime = accountResource.getLatestConsumeTimeForEnergy();

    accountCapsule.setEnergyUsage(increase(accountCapsule, ENERGY,
            oldEnergyUsage, 0, latestConsumeTime, now));
  }

  public void updateTotalEnergyAverageUsage() {
    long now = getHeadSlot();
    long blockEnergyUsage = dynamicPropertiesStore.getBlockEnergyUsage();
    long totalEnergyAverageUsage = dynamicPropertiesStore
        .getTotalEnergyAverageUsage();
    long totalEnergyAverageTime = dynamicPropertiesStore.getTotalEnergyAverageTime();

    long newPublicEnergyAverageUsage = increase(totalEnergyAverageUsage, blockEnergyUsage,
        totalEnergyAverageTime, now, averageWindowSize);

    dynamicPropertiesStore.saveTotalEnergyAverageUsage(newPublicEnergyAverageUsage);
    dynamicPropertiesStore.saveTotalEnergyAverageTime(now);
  }

  public void updateAdaptiveTotalEnergyLimit() {
    long totalEnergyAverageUsage = dynamicPropertiesStore
        .getTotalEnergyAverageUsage();
    long targetTotalEnergyLimit = dynamicPropertiesStore.getTotalEnergyTargetLimit();
    long totalEnergyCurrentLimit = dynamicPropertiesStore
        .getTotalEnergyCurrentLimit();
    long totalEnergyLimit = dynamicPropertiesStore.getTotalEnergyLimit();

    long result;
    if (totalEnergyAverageUsage > targetTotalEnergyLimit) {
      result = totalEnergyCurrentLimit * AdaptiveResourceLimitConstants.CONTRACT_RATE_NUMERATOR
          / AdaptiveResourceLimitConstants.CONTRACT_RATE_DENOMINATOR;
      // logger.info(totalEnergyAverageUsage + ">" + targetTotalEnergyLimit + "\n" + result);
    } else {
      result = totalEnergyCurrentLimit * AdaptiveResourceLimitConstants.EXPAND_RATE_NUMERATOR
          / AdaptiveResourceLimitConstants.EXPAND_RATE_DENOMINATOR;
      // logger.info(totalEnergyAverageUsage + "<" + targetTotalEnergyLimit + "\n" + result);
    }
    result = min(max(result, totalEnergyLimit, this.disableJavaLangMath()),
        totalEnergyLimit * dynamicPropertiesStore.getAdaptiveResourceLimitMultiplier(),
        this.disableJavaLangMath());

    dynamicPropertiesStore.saveTotalEnergyCurrentLimit(result);
    logger.debug("Adjust totalEnergyCurrentLimit, old: {}, new: {}.",
        totalEnergyCurrentLimit, result);
  }

  @Override
  public void consume(TransactionCapsule trx,
      TransactionTrace trace)
      throws ContractValidateException, AccountResourceInsufficientException {
    throw new RuntimeException("Not support");
  }

  public boolean useEnergy(AccountCapsule accountCapsule, long energy, long now) {

    long energyUsage = accountCapsule.getEnergyUsage();

    long latestConsumeTime = accountCapsule.getAccountResource().getLatestConsumeTimeForEnergy();
    long energyLimit = calculateGlobalEnergyLimit(accountCapsule);
    long newEnergyUsage;
    if (!dynamicPropertiesStore.supportUnfreezeDelay()) {
      newEnergyUsage = increase(energyUsage, 0, latestConsumeTime, now);
    } else {
      // only participate in the calculation as a temporary variable, without disk flushing
      newEnergyUsage = recovery(accountCapsule, ENERGY, energyUsage,
          latestConsumeTime, now);
    }

    if (energy > (energyLimit - newEnergyUsage)
        && dynamicPropertiesStore.getAllowTvmFreeze() == 0
        && !dynamicPropertiesStore.supportUnfreezeDelay()) {
      return false;
    }

    long latestOperationTime = dynamicPropertiesStore.getLatestBlockHeaderTimestamp();
    if (!dynamicPropertiesStore.supportUnfreezeDelay()) {
      newEnergyUsage = increase(newEnergyUsage, energy, now, now);
    } else {
      // Participate in calculation and flush disk persistence
      newEnergyUsage = increase(accountCapsule, ENERGY, energyUsage, energy,
          latestConsumeTime, now);
    }

    accountCapsule.setEnergyUsage(newEnergyUsage);
    accountCapsule.setLatestOperationTime(latestOperationTime);
    accountCapsule.setLatestConsumeTimeForEnergy(now);

    accountStore.put(accountCapsule.createDbKey(), accountCapsule);

    if (dynamicPropertiesStore.getAllowAdaptiveEnergy() == 1) {
      long blockEnergyUsage = dynamicPropertiesStore.getBlockEnergyUsage() + energy;
      dynamicPropertiesStore.saveBlockEnergyUsage(blockEnergyUsage);
    }

    return true;
  }

  public long calculateGlobalEnergyLimit(AccountCapsule accountCapsule) {
    long frozeBalance = accountCapsule.getAllFrozenBalanceForEnergy();
    if (dynamicPropertiesStore.supportUnfreezeDelay()) {
      return calculateGlobalEnergyLimitV2(frozeBalance);
    }

    if (frozeBalance < TRX_PRECISION) {
      return 0;
    }

    long energyWeight = frozeBalance / TRX_PRECISION;
    long totalEnergyLimit = dynamicPropertiesStore.getTotalEnergyCurrentLimit();
    long totalEnergyWeight = dynamicPropertiesStore.getTotalEnergyWeight();
    if (dynamicPropertiesStore.allowNewReward() && totalEnergyWeight <= 0) {
      return 0;
    } else {
      assert totalEnergyWeight > 0;
    }
    return (long) (energyWeight * ((double) totalEnergyLimit / totalEnergyWeight));
  }

  public long calculateGlobalEnergyLimitV2(long frozeBalance) {
    double energyWeight = (double) frozeBalance / TRX_PRECISION;
    long totalEnergyLimit = dynamicPropertiesStore.getTotalEnergyCurrentLimit();
    long totalEnergyWeight = dynamicPropertiesStore.getTotalEnergyWeight();
    if (totalEnergyWeight == 0) {
      return 0;
    }
    return (long) (energyWeight * ((double) totalEnergyLimit / totalEnergyWeight));
  }


  public long getAccountLeftEnergyFromFreeze(AccountCapsule accountCapsule) {
    long now = getHeadSlot();
    long energyUsage = accountCapsule.getEnergyUsage();
    long latestConsumeTime = accountCapsule.getAccountResource().getLatestConsumeTimeForEnergy();
    long energyLimit = calculateGlobalEnergyLimit(accountCapsule);

    long newEnergyUsage = recovery(accountCapsule, ENERGY, energyUsage, latestConsumeTime, now);

    return max(energyLimit - newEnergyUsage, 0, this.disableJavaLangMath()); // us
  }

  public boolean useFreeEnergy(AccountCapsule accountCapsule, long energy, long now) {
    long freeEnergyLimit = dynamicPropertiesStore.getFreeEnergyLimit();
    if (freeEnergyLimit <= 0L) {
      return false;
    }

    long freeEnergyUsage = accountCapsule.getFreeEnergyUsage();
    long latestConsumeFreeEnergyTime = accountCapsule.getLatestConsumeFreeEnergyTime();
    long newFreeEnergyUsage = increase(freeEnergyUsage, 0, latestConsumeFreeEnergyTime, now);

    if (energy > (freeEnergyLimit - newFreeEnergyUsage)) {
      logger.debug("Free energy usage is running out."
              + " energy: {}, freeEnergyLimit: {}, newFreeEnergyUsage: {}.",
          energy, freeEnergyLimit, newFreeEnergyUsage);
      return false;
    }
      // TODO confirm energy public limit
//    long publicNetLimit = dynamicPropertiesStore.getPublicNetLimit();
//    long publicNetUsage = dynamicPropertiesStore.getPublicNetUsage();
//    long publicNetTime = dynamicPropertiesStore.getPublicNetTime();
//
//    long newPublicNetUsage = increase(publicNetUsage, 0, publicNetTime, now);
//
//    if (energy > (publicNetLimit - newPublicNetUsage)) {
//      logger.debug("Free public net usage is running out."
//              + " Bytes: {}, publicNetLimit: {}, newPublicNetUsage: {}.",
//          energy, publicNetLimit, newPublicNetUsage);
//      return false;
//    }

    latestConsumeFreeEnergyTime = now;
    // TODO confirm now is headerBlockTime
    long latestOperationTime = now;
//    publicNetTime = now;
    newFreeEnergyUsage = increase(newFreeEnergyUsage, energy, latestConsumeFreeEnergyTime, now);
//    newPublicNetUsage = increase(newPublicNetUsage, energy, publicNetTime, now);
    accountCapsule.setFreeEnergyUsage(newFreeEnergyUsage);
    accountCapsule.setLatestConsumeFreeEnergyTime(latestConsumeFreeEnergyTime);
    accountCapsule.setLatestOperationTime(latestOperationTime);

//    dynamicPropertiesStore.savePublicNetUsage(newPublicNetUsage);
//    dynamicPropertiesStore.savePublicNetTime(publicNetTime);
    accountStore.put(accountCapsule.createDbKey(), accountCapsule);
    return true;
  }

  private long recovery(AccountCapsule accountCapsule, long energy, long now) {

    return 0;
  }

  private long getHeadSlot() {
    return getHeadSlot(dynamicPropertiesStore);
  }


}


