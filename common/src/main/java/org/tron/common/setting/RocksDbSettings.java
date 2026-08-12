package org.tron.common.setting;

import static org.tron.core.Constant.ROCKSDB;

import java.util.Arrays;
import java.util.Locale;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.LoggerFactory;
import org.tron.common.utils.MarketOrderPriceComparatorForRocksDB;
import org.tron.core.Constant;
import org.tron.core.config.args.StorageConfig.DbSettingsConfig;

@Slf4j
public class RocksDbSettings {

  private static RocksDbSettings rocksDbSettings;

  @Getter
  private String benchmarkProfile;
  @Getter
  private String benchmarkMode;
  @Getter
  private boolean useLegacyOptions;
  @Getter
  private boolean legacySharedBlockCache;
  @Getter
  private int levelNumber;
  @Getter
  private int maxOpenFiles;
  @Getter
  private int compactThreads;
  @Getter
  private long blockSize;
  @Getter
  private long blockCacheSize;
  @Getter
  private boolean cacheIndexAndFilterBlocks;
  @Getter
  private boolean pinL0FilterAndIndexBlocksInCache;
  @Getter
  private int bloomFilterBitsPerKey;
  @Getter
  private boolean wholeKeyFiltering;
  @Getter
  private int blockRestartInterval;
  @Getter
  private long writeBufferSize;
  @Getter
  private int maxWriteBufferNumber;
  @Getter
  private int minWriteBufferNumberToMerge;
  @Getter
  private int maxBackgroundFlushes;
  @Getter
  private long maxBytesForLevelBase;
  @Getter
  private double maxBytesForLevelMultiplier;
  @Getter
  private boolean levelCompactionDynamicLevelBytes;
  @Getter
  private int level0FileNumCompactionTrigger;
  @Getter
  private int level0SlowdownWritesTrigger;
  @Getter
  private int level0StopWritesTrigger;
  @Getter
  private long targetFileSizeBase;
  @Getter
  private int targetFileSizeMultiplier;
  @Getter
  private boolean enableStatistics;
  @Getter
  private CompressionType compressionType;

  static {
    RocksDB.loadLibrary();
  }

  private static LRUCache cache;
  private static long cacheSize;

  private static final String[] CI_ENVIRONMENT_VARIABLES = {
      "CI",
      "JENKINS_URL",
      "TRAVIS",
      "CIRCLECI",
      "GITHUB_ACTIONS",
      "GITLAB_CI"
  };

  private static final org.slf4j.Logger rocksDbLogger = LoggerFactory.getLogger(ROCKSDB);

  private RocksDbSettings() {

  }

  public static RocksDbSettings getDefaultSettings() {
    RocksDbSettings defaultSettings = new RocksDbSettings();
    return defaultSettings.withBenchmarkProfile("default").withBenchmarkMode("E1")
        .withUseLegacyOptions(true)
        .withLegacySharedBlockCache(false)
        .withLevelNumber(7).withBlockSize(64).withBlockCacheSize(1024)
        .withCacheIndexAndFilterBlocks(true).withPinL0FilterAndIndexBlocksInCache(true)
        .withBloomFilterBitsPerKey(10).withWholeKeyFiltering(true)
        .withBlockRestartInterval(16).withWriteBufferSize(64)
        .withMaxWriteBufferNumber(2).withMinWriteBufferNumberToMerge(1)
        .withMaxBackgroundFlushes(1).withCompactThreads(32)
        .withTargetFileSizeBase(256).withMaxBytesForLevelMultiplier(10)
        .withTargetFileSizeMultiplier(1).withMaxBytesForLevelBase(256)
        .withLevelCompactionDynamicLevelBytes(true).withLevel0FileNumCompactionTrigger(2)
        .withLevel0SlowdownWritesTrigger(20).withLevel0StopWritesTrigger(36)
        .withMaxOpenFiles(5000).withCompressionType("SNAPPY_COMPRESSION")
        .withEnableStatistics(false);
  }

  public static RocksDbSettings getSettings() {
    return rocksDbSettings == null ? getDefaultSettings() : rocksDbSettings;
  }

  public static RocksDbSettings initCustomSettings(DbSettingsConfig settings) {
    rocksDbSettings = new RocksDbSettings()
        .withBenchmarkProfile(settings.getBenchmarkProfile())
        .withBenchmarkMode(settings.getBenchmarkMode())
        .withUseLegacyOptions(settings.isUseLegacyOptions())
        .withLegacySharedBlockCache(settings.isLegacySharedBlockCache())
        .withMaxOpenFiles(settings.getMaxOpenFiles())
        .withEnableStatistics(false)
        .withLevelNumber(settings.getLevelNumber())
        .withCompactThreads(settings.getCompactThreads())
        .withBlockSize(settings.getBlocksize())
        .withBlockCacheSize(settings.getBlockCacheSize())
        .withCacheIndexAndFilterBlocks(settings.isCacheIndexAndFilterBlocks())
        .withPinL0FilterAndIndexBlocksInCache(
            settings.isPinL0FilterAndIndexBlocksInCache())
        .withBloomFilterBitsPerKey(settings.getBloomFilterBitsPerKey())
        .withWholeKeyFiltering(settings.isWholeKeyFiltering())
        .withBlockRestartInterval(settings.getBlockRestartInterval())
        .withWriteBufferSize(settings.getWriteBufferSize())
        .withMaxWriteBufferNumber(settings.getMaxWriteBufferNumber())
        .withMinWriteBufferNumberToMerge(settings.getMinWriteBufferNumberToMerge())
        .withMaxBackgroundFlushes(settings.getMaxBackgroundFlushes())
        .withMaxBytesForLevelBase(settings.getMaxBytesForLevelBase())
        .withMaxBytesForLevelMultiplier(settings.getMaxBytesForLevelMultiplier())
        .withLevelCompactionDynamicLevelBytes(settings.isLevelCompactionDynamicLevelBytes())
        .withLevel0FileNumCompactionTrigger(settings.getLevel0FileNumCompactionTrigger())
        .withLevel0SlowdownWritesTrigger(settings.getLevel0SlowdownWritesTrigger())
        .withLevel0StopWritesTrigger(settings.getLevel0StopWritesTrigger())
        .withTargetFileSizeBase(settings.getTargetFileSizeBase())
        .withTargetFileSizeMultiplier(settings.getTargetFileSizeMultiplier())
        .withCompressionType(settings.getCompressionType());
    return rocksDbSettings;
  }

  public static void loggingSettings() {
    logger.info("RocksDB benchmark profile: {}, mode: {}, settings: {}",
        rocksDbSettings.getBenchmarkProfile(), rocksDbSettings.getBenchmarkMode(),
        rocksDbSettings.describe());
  }

  public String describe() {
    return String.format(Locale.ROOT,
        "useLegacyOptions=%s,legacySharedBlockCache=%s,levels=%d,compactThreads=%d,"
            + "blockSize=%d,blockCacheSize=%d,"
            + "cacheIndexAndFilter=%s,pinL0=%s,bloomBits=%d,wholeKey=%s,restartInterval=%d,"
            + "writeBufferSize=%d,maxWriteBuffers=%d,minWriteBuffersToMerge=%d,flushThreads=%d,"
            + "maxBytesForLevelBase=%d,maxBytesMultiplier=%s,dynamicLevels=%s,l0Trigger=%d,"
            + "l0Slowdown=%d,l0Stop=%d,targetFileBase=%d,targetFileMultiplier=%d,"
            + "maxOpenFiles=%d,compression=%s",
        useLegacyOptions, legacySharedBlockCache, levelNumber, compactThreads,
        blockSize, blockCacheSize,
        cacheIndexAndFilterBlocks,
        pinL0FilterAndIndexBlocksInCache, bloomFilterBitsPerKey, wholeKeyFiltering,
        blockRestartInterval, writeBufferSize, maxWriteBufferNumber,
        minWriteBufferNumberToMerge, maxBackgroundFlushes, maxBytesForLevelBase,
        maxBytesForLevelMultiplier, levelCompactionDynamicLevelBytes,
        level0FileNumCompactionTrigger, level0SlowdownWritesTrigger,
        level0StopWritesTrigger, targetFileSizeBase, targetFileSizeMultiplier,
        maxOpenFiles, compressionType);
  }

  public RocksDbSettings withMaxOpenFiles(int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
    return this;
  }

  public RocksDbSettings withBenchmarkProfile(String benchmarkProfile) {
    this.benchmarkProfile = benchmarkProfile;
    return this;
  }

  public RocksDbSettings withBenchmarkMode(String benchmarkMode) {
    this.benchmarkMode = benchmarkMode;
    return this;
  }

  public RocksDbSettings withUseLegacyOptions(boolean useLegacyOptions) {
    this.useLegacyOptions = useLegacyOptions;
    return this;
  }

  public RocksDbSettings withLegacySharedBlockCache(boolean legacySharedBlockCache) {
    this.legacySharedBlockCache = legacySharedBlockCache;
    return this;
  }

  public RocksDbSettings withCompactThreads(int compactThreads) {
    this.compactThreads = compactThreads;
    return this;
  }

  public RocksDbSettings withBlockSize(long blockSize) {
    this.blockSize = blockSize * 1024;
    return this;
  }

  public RocksDbSettings withBlockCacheSize(long blockCacheSize) {
    this.blockCacheSize = blockCacheSize * 1024 * 1024;
    return this;
  }

  public RocksDbSettings withCacheIndexAndFilterBlocks(boolean enabled) {
    this.cacheIndexAndFilterBlocks = enabled;
    return this;
  }

  public RocksDbSettings withPinL0FilterAndIndexBlocksInCache(boolean enabled) {
    this.pinL0FilterAndIndexBlocksInCache = enabled;
    return this;
  }

  public RocksDbSettings withBloomFilterBitsPerKey(int bloomFilterBitsPerKey) {
    this.bloomFilterBitsPerKey = bloomFilterBitsPerKey;
    return this;
  }

  public RocksDbSettings withWholeKeyFiltering(boolean wholeKeyFiltering) {
    this.wholeKeyFiltering = wholeKeyFiltering;
    return this;
  }

  public RocksDbSettings withBlockRestartInterval(int blockRestartInterval) {
    this.blockRestartInterval = blockRestartInterval;
    return this;
  }

  public RocksDbSettings withWriteBufferSize(long writeBufferSize) {
    this.writeBufferSize = writeBufferSize * 1024 * 1024;
    return this;
  }

  public RocksDbSettings withMaxWriteBufferNumber(int maxWriteBufferNumber) {
    this.maxWriteBufferNumber = maxWriteBufferNumber;
    return this;
  }

  public RocksDbSettings withMinWriteBufferNumberToMerge(int value) {
    this.minWriteBufferNumberToMerge = value;
    return this;
  }

  public RocksDbSettings withMaxBackgroundFlushes(int maxBackgroundFlushes) {
    this.maxBackgroundFlushes = maxBackgroundFlushes;
    return this;
  }

  public RocksDbSettings withMaxBytesForLevelBase(long maxBytesForLevelBase) {
    this.maxBytesForLevelBase = maxBytesForLevelBase * 1024 * 1024;
    return this;
  }

  public RocksDbSettings withMaxBytesForLevelMultiplier(double maxBytesForLevelMultiplier) {
    this.maxBytesForLevelMultiplier = maxBytesForLevelMultiplier;
    return this;
  }

  public RocksDbSettings withLevelCompactionDynamicLevelBytes(boolean enabled) {
    this.levelCompactionDynamicLevelBytes = enabled;
    return this;
  }

  public RocksDbSettings withLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
    this.level0FileNumCompactionTrigger = level0FileNumCompactionTrigger;
    return this;
  }

  public RocksDbSettings withLevel0SlowdownWritesTrigger(int value) {
    this.level0SlowdownWritesTrigger = value;
    return this;
  }

  public RocksDbSettings withLevel0StopWritesTrigger(int value) {
    this.level0StopWritesTrigger = value;
    return this;
  }

  public RocksDbSettings withEnableStatistics(boolean enable) {
    this.enableStatistics = enable;
    return this;
  }

  public RocksDbSettings withLevelNumber(int levelNumber) {
    this.levelNumber = levelNumber;
    return this;
  }

  public RocksDbSettings withTargetFileSizeBase(long targetFileSizeBase) {
    this.targetFileSizeBase = targetFileSizeBase * 1024 * 1024;
    return this;
  }

  public RocksDbSettings withTargetFileSizeMultiplier(int targetFileSizeMultiplier) {
    this.targetFileSizeMultiplier = targetFileSizeMultiplier;
    return this;
  }

  public RocksDbSettings withCompressionType(String compressionType) {
    try {
      this.compressionType = CompressionType.valueOf(compressionType);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported RocksDB compressionType: "
          + compressionType, e);
    }
    return this;
  }

  private static synchronized LRUCache getCache(long requestedSize) {
    if (cache == null) {
      cache = new LRUCache(requestedSize);
      cacheSize = requestedSize;
    } else if (cacheSize != requestedSize) {
      throw new IllegalStateException("RocksDB blockCacheSize changed in the same JVM; "
          + "restart the process between benchmark profiles");
    }
    return cache;
  }

  /**
   * Creates a new RocksDB Options.
   *
   * <p><b>CRITICAL:</b> Must be closed after use to prevent native memory leaks.
   * Use try-with-resources.
   *
   * <pre>{@code
   * try (Options options = getOptionsByDbName(dbName)) {
   *     // do something
   * }
   * }</pre>
   *
   * @param dbName  db name
   * @return a new Options instance that must be closed
   */
  public static Options getOptionsByDbName(String dbName) {
    RocksDbSettings settings = getSettings();

    Options options = new Options();

    options.setLogger(new Logger(options) {
      @Override
      protected void log(InfoLogLevel infoLogLevel, String logMsg) {
        rocksDbLogger.info("{} {}", dbName, logMsg);
      }
    });
    // most of these options are suggested by https://github.com/facebook/rocksdb/wiki/Set-Up-Options

    // general options
    if (settings.isEnableStatistics()) {
      try (Statistics statistics = new Statistics()) {
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        options.setStatistics(statistics);
      }
      // Prometheus polls selected tickers; avoid a second periodic dump to the RocksDB log.
      options.setStatsDumpPeriodSec(0);
    }
    if (settings.isUseLegacyOptions()) {
      applyLegacyOptions(options, settings);
    } else {
      applyCustomOptions(options, settings);
    }
    if (Constant.MARKET_PAIR_PRICE_TO_ORDER.equals(dbName)) {
      ComparatorOptions comparatorOptions = new ComparatorOptions();
      options.setComparator(new MarketOrderPriceComparatorForRocksDB(comparatorOptions));
    }

    if (isRunningInCI() && "default".equals(settings.getBenchmarkProfile())) {
      options.optimizeForSmallDb();
      // Disable fallocate calls  to avoid issues with disk space
      options.setAllowFAllocate(false);
      // Set WAL size limits to avoid excessive disk
      options.setMaxTotalWalSize(2 * 1024 * 1024);
      // Set recycle log file
      options.setRecycleLogFileNum(1);
      // Enable creation of missing column families
      options.setCreateMissingColumnFamilies(true);
      // Set max background flushes to 1 to reduce resource usage
      options.setMaxBackgroundFlushes(1);
    }

    return options;
  }

  private static void applyLegacyOptions(Options options, RocksDbSettings settings) {
    options.setCreateIfMissing(true);
    options.setIncreaseParallelism(1);
    options.setLevelCompactionDynamicLevelBytes(true);
    options.setMaxOpenFiles(settings.getMaxOpenFiles());
    options.setNumLevels(settings.getLevelNumber());
    options.setMaxBytesForLevelMultiplier(settings.getMaxBytesForLevelMultiplier());
    options.setMaxBytesForLevelBase(settings.getMaxBytesForLevelBase());
    options.setMaxBackgroundCompactions(settings.getCompactThreads());
    options.setLevel0FileNumCompactionTrigger(settings.getLevel0FileNumCompactionTrigger());
    options.setTargetFileSizeMultiplier(settings.getTargetFileSizeMultiplier());
    options.setTargetFileSizeBase(settings.getTargetFileSizeBase());
    BlockBasedTableConfig tableCfg = new BlockBasedTableConfig();
    if (settings.isLegacySharedBlockCache()) {
      tableCfg.setBlockCache(RocksDbSettings.getCache(settings.getBlockCacheSize()));
    }
    options.setTableFormatConfig(tableCfg);
  }

  private static void applyCustomOptions(Options options, RocksDbSettings settings) {
    options.setCreateIfMissing(true);
    options.setIncreaseParallelism(1);
    options.setLevelCompactionDynamicLevelBytes(
        settings.isLevelCompactionDynamicLevelBytes());
    options.setMaxOpenFiles(settings.getMaxOpenFiles());
    options.setNumLevels(settings.getLevelNumber());
    options.setMaxBytesForLevelMultiplier(settings.getMaxBytesForLevelMultiplier());
    options.setMaxBytesForLevelBase(settings.getMaxBytesForLevelBase());
    options.setMaxBackgroundCompactions(settings.getCompactThreads());
    options.setMaxBackgroundFlushes(settings.getMaxBackgroundFlushes());
    options.setWriteBufferSize(settings.getWriteBufferSize());
    options.setMaxWriteBufferNumber(settings.getMaxWriteBufferNumber());
    options.setMinWriteBufferNumberToMerge(settings.getMinWriteBufferNumberToMerge());
    options.setLevel0FileNumCompactionTrigger(settings.getLevel0FileNumCompactionTrigger());
    options.setLevel0SlowdownWritesTrigger(settings.getLevel0SlowdownWritesTrigger());
    options.setLevel0StopWritesTrigger(settings.getLevel0StopWritesTrigger());
    options.setTargetFileSizeMultiplier(settings.getTargetFileSizeMultiplier());
    options.setTargetFileSizeBase(settings.getTargetFileSizeBase());
    options.setCompressionType(settings.getCompressionType());

    BlockBasedTableConfig tableCfg = new BlockBasedTableConfig();
    tableCfg.setBlockSize(settings.getBlockSize());
    tableCfg.setBlockRestartInterval(settings.getBlockRestartInterval());
    tableCfg.setWholeKeyFiltering(settings.isWholeKeyFiltering());
    if (settings.getBlockCacheSize() == 0) {
      tableCfg.setNoBlockCache(true);
    } else {
      tableCfg.setBlockCache(RocksDbSettings.getCache(settings.getBlockCacheSize()));
      tableCfg.setCacheIndexAndFilterBlocks(settings.isCacheIndexAndFilterBlocks());
      tableCfg.setPinL0FilterAndIndexBlocksInCache(
          settings.isPinL0FilterAndIndexBlocksInCache());
    }
    if (settings.getBloomFilterBitsPerKey() > 0) {
      tableCfg.setFilter(new BloomFilter(settings.getBloomFilterBitsPerKey(), false));
    }
    options.setTableFormatConfig(tableCfg);
  }

  private static boolean isRunningInCI() {
    return Arrays.stream(CI_ENVIRONMENT_VARIABLES).anyMatch(System.getenv()::containsKey);
  }
}
