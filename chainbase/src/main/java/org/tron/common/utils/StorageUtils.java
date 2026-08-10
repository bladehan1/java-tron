package org.tron.common.utils;

import static org.tron.common.parameter.CommonParameter.ENERGY_LIMIT_HARD_FORK;
import static org.tron.core.db.common.DbSourceInter.LEVELDB;

import java.io.File;
import org.apache.commons.lang3.StringUtils;
import org.iq80.leveldb.Options;
import org.slf4j.LoggerFactory;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;
import org.tron.core.Constant;


public class StorageUtils {

  private static final org.slf4j.Logger levelDbLogger = LoggerFactory.getLogger(LEVELDB);

  public static boolean getEnergyLimitHardFork() {
    return ENERGY_LIMIT_HARD_FORK;
  }

  public static String getOutputDirectoryByDbName(String dbName) {
    String path = getPathByDbName(dbName);
    if (!StringUtils.isBlank(path)) {
      return path;
    }
    return getOutputDirectory();
  }

  public static String getPathByDbName(String dbName) {
    if (hasProperty(dbName)) {
      return getProperty(dbName).getPath();
    }
    return null;
  }

  private static boolean hasProperty(String dbName) {
    if (CommonParameter.getInstance().getStorage()
        .getPropertyMap() != null) {
      return CommonParameter.getInstance().getStorage()
          .getPropertyMap().containsKey(dbName);
    }
    return false;
  }

  private static Property getProperty(String dbName) {
    return CommonParameter.getInstance().getStorage()
        .getPropertyMap().get(dbName);
  }

  public static String getOutputDirectory() {
    if (!"".equals(CommonParameter.getInstance().getOutputDirectory())
        && !CommonParameter.getInstance().getOutputDirectory().endsWith(File.separator)) {
      return CommonParameter.getInstance().getOutputDirectory() + File.separator;
    }
    return CommonParameter.getInstance().getOutputDirectory();
  }

  public static Options getOptionsByDbName(String dbName) {
    Options options;
    if (hasProperty(dbName)) {
      options = getProperty(dbName).getDbOptions();
    } else {
      options = CommonParameter.getInstance().getStorage().newDefaultDbOptions(dbName);
    }
    if (Constant.MARKET_PAIR_PRICE_TO_ORDER.equals(dbName)) {
      options.comparator(new MarketOrderPriceComparatorForLevelDB());
    }
    options.logger(message -> {
      levelDbLogger.info("{} {}", dbName, message);
      if (!Metrics.databaseEnabled()) {
        return;
      }
      if (message.startsWith("Recovering")) {
        Metrics.counterInc(MetricKeys.Counter.DB_EVENT, 1, LEVELDB, dbName, "recover");
      } else if (message.startsWith("Compacting")) {
        Metrics.counterInc(MetricKeys.Counter.DB_EVENT, 1, LEVELDB, dbName, "compact");
      } else if (message.startsWith("Delete")) {
        Metrics.counterInc(MetricKeys.Counter.DB_EVENT, 1, LEVELDB, dbName, "delete");
      } else if (message.startsWith("Generated")) {
        Metrics.counterInc(MetricKeys.Counter.DB_EVENT, 1, LEVELDB, dbName, "create");
      }
    });
    return options;
  }
}
