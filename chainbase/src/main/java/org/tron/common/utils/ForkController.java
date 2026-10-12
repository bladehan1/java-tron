package org.tron.common.utils;

import static org.tron.common.math.Maths.ceil;
import static org.tron.common.utils.StringUtil.encode58Check;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionConsts;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.store.DynamicPropertiesStore;

@Slf4j(topic = "utils")
public class ForkController {

  private static final byte VERSION_DOWNGRADE = (byte) 0;
  private static final byte VERSION_UPGRADE = (byte) 1;
  private static final byte[] check;

  static {
    check = new byte[1024];
    Arrays.fill(check, VERSION_UPGRADE);
  }

  @Getter
  private ChainBaseManager manager;

  public void init(ChainBaseManager manager) {
    this.manager = manager;
    DynamicPropertiesStore store = manager.getDynamicPropertiesStore();
    int latestVersion = store.getLatestVersion();
    if (latestVersion == 0) {
      for (ForkBlockVersionEnum version : ForkBlockVersionEnum.values()) {
        int v = version.getValue();
        if (pass(v) && latestVersion < v) {
          latestVersion = v;
        }
      }
      store.saveLatestVersion(latestVersion);
    }
  }

  public boolean pass(ForkBlockVersionEnum forkBlockVersionEnum) {
    return pass(forkBlockVersionEnum.getValue());
  }

  public synchronized boolean pass(int version) {
    if (manager == null) {
      throw new IllegalStateException("not inited");
    }
    return evaluate(version, latestSource);
  }

  /**
   * The fork-activation rule as a pure function of its dynamic-property inputs (chapter 05
   * section 3.9.3). {@link #pass(int)} evaluates it against the latest store; historical
   * execution evaluates the same algorithm against S(T).
   */
  public static boolean evaluate(int version, ForkStateSource source) {
    Objects.requireNonNull(source, "source");
    if (version > ForkBlockVersionEnum.VERSION_4_0.getValue()) {
      return passNew(version, source);
    } else {
      return passOld(version, source);
    }
  }

  private static boolean passOld(int version, ForkStateSource source) {
    if (version == ForkBlockVersionConsts.ENERGY_LIMIT) {
      return checkForEnergyLimit(source);
    }

    byte[] stats = source.statsByVersion(version);
    return check(stats);
  }

  private static boolean passNew(int version, ForkStateSource source) {
    ForkBlockVersionEnum versionEnum = ForkBlockVersionEnum.getForkBlockVersionEnum(version);
    if (versionEnum == null) {
      logger.warn("Not exist block version: {}.", version);
      return false;
    }
    long latestBlockTime = source.latestBlockHeaderTimestamp();
    long maintenanceTimeInterval = source.maintenanceTimeInterval();
    long hardForkTime = ((versionEnum.getHardForkTime() - 1) / maintenanceTimeInterval + 1)
        * maintenanceTimeInterval;
    if (latestBlockTime < hardForkTime) {
      return false;
    }
    byte[] stats = source.statsByVersion(version);
    if (stats == null || stats.length == 0) {
      return false;
    }
    int count = 0;
    for (int i = 0; i < stats.length; i++) {
      if (check[i] == stats[i]) {
        ++count;
      }
    }
    return count >= ceil((double) versionEnum.getHardForkRate() * stats.length / 100,
        source.disableJavaLangMath());
  }


  // when block.version = 5,
  // it make block use new energy to handle transaction when block number >= 4727890L.
  // version !=5, skip this.
  private static boolean checkForEnergyLimit(ForkStateSource source) {
    long blockNum = source.latestBlockHeaderNumber();
    return blockNum >= CommonParameter.getInstance().getBlockNumForEnergyLimit();
  }

  private static boolean check(byte[] stats) {
    if (stats == null || stats.length == 0) {
      return false;
    }

    for (int i = 0; i < stats.length; i++) {
      if (check[i] != stats[i]) {
        return false;
      }
    }

    return true;
  }

  /** Latest inputs, read through the manager's current DynamicPropertiesStore on every access. */
  private final ForkStateSource latestSource = new ForkStateSource() {
    @Override
    public long latestBlockHeaderTimestamp() {
      return manager.getDynamicPropertiesStore().getLatestBlockHeaderTimestamp();
    }

    @Override
    public long maintenanceTimeInterval() {
      return manager.getDynamicPropertiesStore().getMaintenanceTimeInterval();
    }

    @Override
    public byte[] statsByVersion(int version) {
      return manager.getDynamicPropertiesStore().statsByVersion(version);
    }

    @Override
    public boolean disableJavaLangMath() {
      return manager.getDynamicPropertiesStore().disableJavaLangMath();
    }

    @Override
    public long latestBlockHeaderNumber() {
      return manager.getDynamicPropertiesStore().getLatestBlockHeaderNumber();
    }
  };

  private void downgrade(int version, int slot) {
    for (ForkBlockVersionEnum versionEnum : ForkBlockVersionEnum.values()) {
      int versionValue = versionEnum.getValue();
      if (versionValue > version && !pass(versionValue)) {
        byte[] stats = manager.getDynamicPropertiesStore().statsByVersion(versionValue);
        if (Objects.nonNull(stats)) {
          stats[slot] = VERSION_DOWNGRADE;
          manager.getDynamicPropertiesStore().statsByVersion(versionValue, stats);
        }
      }
    }
  }

  private void upgrade(int version, int slotSize) {
    for (ForkBlockVersionEnum versionEnum : ForkBlockVersionEnum.values()) {
      int versionValue = versionEnum.getValue();
      if (versionValue < version && !pass(versionValue)) {
        byte[] stats = manager.getDynamicPropertiesStore().statsByVersion(versionValue);
        if (stats == null || stats.length == 0) {
          stats = new byte[slotSize];
        }
        Arrays.fill(stats, VERSION_UPGRADE);
        manager.getDynamicPropertiesStore().statsByVersion(versionValue, stats);
      }
    }
  }

  public synchronized void update(BlockCapsule blockCapsule) {
    List<ByteString> witnesses = manager.getWitnessScheduleStore().getActiveWitnesses();
    ByteString witness = blockCapsule.getWitnessAddress();
    int slot = witnesses.indexOf(witness);
    if (slot < 0) {
      return;
    }

    int version = blockCapsule.getInstance().getBlockHeader().getRawData().getVersion();
    if (version < ForkBlockVersionConsts.ENERGY_LIMIT) {
      return;
    }

    if (manager.getDynamicPropertiesStore().getLatestVersion() >= version) {
      return;
    }

    downgrade(version, slot);

    byte[] stats = manager.getDynamicPropertiesStore().statsByVersion(version);
    if (Objects.isNull(stats) || stats.length != witnesses.size()) {
      stats = new byte[witnesses.size()];
    }

    if (pass(version)) {
      upgrade(version, stats.length);
      manager.getDynamicPropertiesStore().saveLatestVersion(version);
      return;
    }

    stats[slot] = VERSION_UPGRADE;
    manager.getDynamicPropertiesStore().statsByVersion(version, stats);
    logger.info(
        "Update hard fork: {}, witness size: {}, solt: {}, witness: {}, version: {}.",
        Streams.zip(witnesses.stream(), Stream.of(ArrayUtils.toObject(stats)), Maps::immutableEntry)
            .map(e -> Maps
                .immutableEntry(encode58Check(e.getKey().toByteArray()), e.getValue()))
            .map(e -> Maps
                .immutableEntry(StringUtils.substring(e.getKey(), e.getKey().length() - 4),
                    e.getValue()))
            .collect(Collectors.toList()),
        witnesses.size(),
        slot,
        encode58Check(witness.toByteArray()),
        version);
  }

  public synchronized void reset() {
    int size = manager.getWitnessScheduleStore().getActiveWitnesses().size();
    for (ForkBlockVersionEnum versionEnum : ForkBlockVersionEnum.values()) {
      int versionValue = versionEnum.getValue();
      byte[] stats = manager.getDynamicPropertiesStore().statsByVersion(versionValue);
      if (Objects.nonNull(stats) && !pass(versionValue)) {
        stats = new byte[size];
        manager.getDynamicPropertiesStore().statsByVersion(versionValue, stats);
      }
    }
  }

  public static ForkController instance() {
    return ForkControllerEnum.INSTANCE.getInstance();
  }

  private enum ForkControllerEnum {
    INSTANCE;

    private ForkController instance;

    ForkControllerEnum() {
      instance = new ForkController();
    }

    private ForkController getInstance() {
      return instance;
    }
  }

}
