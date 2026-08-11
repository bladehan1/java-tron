package org.tron.common.setting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.tron.core.config.args.StorageConfig.DbSettingsConfig;

public class RocksDbSettingsTest {

  @Test
  public void shouldApplyBenchmarkSettingsToNativeOptions() {
    DbSettingsConfig config = new DbSettingsConfig();
    config.setUseLegacyOptions(false);
    config.setBenchmarkProfile("native-options");
    config.setBenchmarkMode("E2");
    config.setBlocksize(32);
    config.setWriteBufferSize(96);
    config.setMaxWriteBufferNumber(4);
    config.setMinWriteBufferNumberToMerge(2);
    config.setMaxBackgroundFlushes(3);
    config.setLevel0FileNumCompactionTrigger(5);
    config.setLevel0SlowdownWritesTrigger(12);
    config.setLevel0StopWritesTrigger(18);
    config.setTargetFileSizeBase(128);
    config.setCompressionType("LZ4_COMPRESSION");

    RocksDbSettings settings = RocksDbSettings.initCustomSettings(config);
    try (Options options = RocksDbSettings.getOptionsByDbName("benchmark-test")) {
      assertEquals("native-options", settings.getBenchmarkProfile());
      assertEquals("E2", settings.getBenchmarkMode());
      assertEquals(96L * 1024 * 1024, options.writeBufferSize());
      assertEquals(4, options.maxWriteBufferNumber());
      assertEquals(2, options.minWriteBufferNumberToMerge());
      assertEquals(3, options.maxBackgroundFlushes());
      assertEquals(5, options.level0FileNumCompactionTrigger());
      assertEquals(12, options.level0SlowdownWritesTrigger());
      assertEquals(18, options.level0StopWritesTrigger());
      assertEquals(128L * 1024 * 1024, options.targetFileSizeBase());
      assertEquals(CompressionType.LZ4_COMPRESSION, options.compressionType());

      assertTrue(options.tableFormatConfig() instanceof BlockBasedTableConfig);
      BlockBasedTableConfig table = (BlockBasedTableConfig) options.tableFormatConfig();
      assertEquals(32L * 1024, table.blockSize());
      assertEquals(16, table.blockRestartInterval());
      assertTrue(table.cacheIndexAndFilterBlocks());
      assertTrue(table.pinL0FilterAndIndexBlocksInCache());
    } finally {
      RocksDbSettings.initCustomSettings(new DbSettingsConfig());
    }
  }

  @Test
  public void shouldPreserveLegacyNativeOptionsByDefault() {
    DbSettingsConfig config = new DbSettingsConfig();
    config.setBenchmarkProfile("legacy-options");
    config.setMaxBackgroundFlushes(3);
    config.setWriteBufferSize(96);
    config.setBlocksize(32);
    config.setCacheIndexAndFilterBlocks(true);
    config.setPinL0FilterAndIndexBlocksInCache(true);
    config.setBloomFilterBitsPerKey(10);
    config.setLevel0FileNumCompactionTrigger(5);
    config.setTargetFileSizeBase(128);
    config.setCompressionType("LZ4_COMPRESSION");

    RocksDbSettings settings = RocksDbSettings.initCustomSettings(config);
    try (Options options = RocksDbSettings.getOptionsByDbName("legacy-options");
        Options expected = new Options()) {
      expected.setIncreaseParallelism(1);
      expected.setMaxBackgroundCompactions(settings.getCompactThreads());
      assertTrue(settings.isUseLegacyOptions());
      assertEquals(expected.maxBackgroundFlushes(), options.maxBackgroundFlushes());
      assertEquals(expected.writeBufferSize(), options.writeBufferSize());
      assertEquals(expected.compressionType(), options.compressionType());
      assertEquals(5, options.level0FileNumCompactionTrigger());
      assertEquals(128L * 1024 * 1024, options.targetFileSizeBase());
      assertTrue(options.tableFormatConfig() instanceof BlockBasedTableConfig);
      BlockBasedTableConfig table = (BlockBasedTableConfig) options.tableFormatConfig();
      BlockBasedTableConfig defaultTable = new BlockBasedTableConfig();
      assertEquals(defaultTable.blockSize(), table.blockSize());
      assertEquals(defaultTable.blockRestartInterval(), table.blockRestartInterval());
      assertEquals(defaultTable.cacheIndexAndFilterBlocks(),
          table.cacheIndexAndFilterBlocks());
      assertEquals(defaultTable.pinL0FilterAndIndexBlocksInCache(),
          table.pinL0FilterAndIndexBlocksInCache());
    } finally {
      RocksDbSettings.initCustomSettings(new DbSettingsConfig());
    }
  }
}
