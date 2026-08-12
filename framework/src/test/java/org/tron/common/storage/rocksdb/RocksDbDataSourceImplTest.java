package org.tron.common.storage.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.tron.common.TestConstants.TEST_CONF;
import static org.tron.common.TestConstants.assumeLevelDbAvailable;

import io.prometheus.client.CollectorRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.FileUtil;
import org.tron.common.utils.PropUtil;
import org.tron.common.utils.StorageUtils;
import org.tron.core.config.args.Args;
import org.tron.core.exception.TronError;

/**
 * RocksDB-specific tests. Common DB tests are in {@link
 * org.tron.common.storage.DbDataSourceImplTest}.
 */
public class RocksDbDataSourceImplTest {

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private byte[] key1 = "00000001aa".getBytes();
  private byte[] value1 = "10000".getBytes();

  @AfterClass
  public static void destroy() {
    Args.clearParam();
  }

  @BeforeClass
  public static void initDb() throws IOException {
    Args.setParam(new String[]{"--output-directory",
        temporaryFolder.newFolder().toString()}, TEST_CONF);
    CommonParameter.getInstance().storage.setDbEngine("ROCKSDB");
  }

  @Test
  public void initDbTest() {
    makeExceptionDb("test_initDb");
    TronError thrown = assertThrows(TronError.class, () -> new RocksDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "test_initDb"));
    assertEquals(TronError.ErrCode.ROCKSDB_INIT, thrown.getErrCode());
  }

  @Test
  public void testCheckOrInitEngine() {
    String dir =
        Args.getInstance().getOutputDirectory() + Args.getInstance().getStorage().getDbDirectory();
    String enginePath = dir + File.separator + "test_engine" + File.separator + "engine.properties";
    FileUtil.createDirIfNotExists(dir + File.separator + "test_engine");
    FileUtil.createFileIfNotExists(enginePath);
    PropUtil.writeProperty(enginePath, "ENGINE", "ROCKSDB");
    Assert.assertEquals("ROCKSDB", PropUtil.readProperty(enginePath, "ENGINE"));

    RocksDbDataSourceImpl dataSource;
    dataSource = new RocksDbDataSourceImpl(dir, "test_engine");
    Assert.assertNotNull(dataSource.getDatabase());
    dataSource.closeDB();

    PropUtil.writeProperty(enginePath, "ENGINE", "LEVELDB");
    Assert.assertEquals("LEVELDB", PropUtil.readProperty(enginePath, "ENGINE"));

    try {
      new RocksDbDataSourceImpl(dir, "test_engine");
    } catch (TronError e) {
      Assert.assertEquals("Cannot open LEVELDB database with ROCKSDB engine.", e.getMessage());
    }
    PropUtil.writeProperty(enginePath, "ENGINE", "ROCKSDB");
  }

  @Test
  public void testRocksDbOpenLevelDb() {
    assumeLevelDbAvailable();
    String name = "test_openLevelDb";
    String output = Paths
        .get(StorageUtils.getOutputDirectoryByDbName(name), CommonParameter
            .getInstance().getStorage().getDbDirectory()).toString();
    LevelDbDataSourceImpl levelDb = new LevelDbDataSourceImpl(
        StorageUtils.getOutputDirectoryByDbName(name), name);
    levelDb.putData(key1, value1);
    levelDb.closeDB();
    expectedException.expectMessage("Cannot open LEVELDB database with ROCKSDB engine.");
    new RocksDbDataSourceImpl(output, name);
  }

  @Test
  public void testRocksDbOpenLevelDb2() {
    assumeLevelDbAvailable();
    String name = "test_openLevelDb2";
    String output = Paths
        .get(StorageUtils.getOutputDirectoryByDbName(name), CommonParameter
            .getInstance().getStorage().getDbDirectory()).toString();
    LevelDbDataSourceImpl levelDb = new LevelDbDataSourceImpl(
        StorageUtils.getOutputDirectoryByDbName(name), name);
    levelDb.putData(key1, value1);
    levelDb.closeDB();
    File engineFile = Paths.get(output, name, "engine.properties").toFile();
    if (engineFile.exists()) {
      engineFile.delete();
    }
    Assert.assertFalse(engineFile.exists());

    expectedException.expectMessage("Cannot open LEVELDB database with ROCKSDB engine.");
    new RocksDbDataSourceImpl(output, name);
  }

  @Test
  public void backupAndDelete() throws RocksDBException {
    RocksDbDataSourceImpl dataSource = new RocksDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "backupAndDelete");
    dataSource.putData(key1, value1);
    Path dir = Paths.get(Args.getInstance().getOutputDirectory(), "backup");
    String path = dir + File.separator;
    FileUtil.createDirIfNotExists(path);
    dataSource.backup(path);
    File backDB = Paths.get(dir.toString(), dataSource.getDBName()).toFile();
    Assert.assertTrue(backDB.exists());
    dataSource.deleteDbBakPath(path);
    Assert.assertFalse(backDB.exists());
    dataSource.closeDB();
  }

  @Test
  public void exportsRocksDbStatisticsWhenDatabaseMetricsEnabled() {
    CommonParameter parameter = CommonParameter.getInstance();
    boolean prometheusEnabled = parameter.isMetricsPrometheusEnable();
    boolean databaseMetricsEnabled = parameter.isMetricsPrometheusDatabaseEnable();
    boolean statisticsEnabled = RocksDbSettings.getSettings().isEnableStatistics();
    RocksDbDataSourceImpl dataSource = null;
    try {
      parameter.setMetricsPrometheusEnable(true);
      parameter.setMetricsPrometheusDatabaseEnable(true);
      RocksDbSettings.getSettings().withEnableStatistics(true);
      String database = "statistics-export";
      dataSource = new RocksDbDataSourceImpl(Args.getInstance().getOutputDirectory(), database);

      dataSource.putData(key1, value1);
      dataSource.getData(key1);
      dataSource.getData("missing-key".getBytes());
      dataSource.stat();

      Double writes = CollectorRegistry.defaultRegistry.getSampleValue(
          MetricKeys.Counter.DB_ROCKSDB_TICKER + "_total",
          new String[]{"type", "db", "ticker"},
          new String[]{"ROCKSDB", database, "number_keys_written"});
      Assert.assertNotNull(writes);
      Assert.assertTrue(writes >= 1);
      Assert.assertEquals(1.0, getOutcome(database, "hit"), 0.0);
      Assert.assertEquals(1.0, getOutcome(database, "miss"), 0.0);
    } finally {
      if (dataSource != null) {
        dataSource.closeDB();
      }
      RocksDbSettings.getSettings().withEnableStatistics(statisticsEnabled);
      parameter.setMetricsPrometheusDatabaseEnable(databaseMetricsEnabled);
      parameter.setMetricsPrometheusEnable(prometheusEnabled);
    }
  }

  private double getOutcome(String database, String outcome) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(
        MetricKeys.Counter.DB_GET + "_total",
        new String[]{"type", "db", "outcome"},
        new String[]{"ROCKSDB", database, outcome});
    return value == null ? 0.0 : value;
  }

  private void makeExceptionDb(String dbName) {
    RocksDbDataSourceImpl dataSource = new RocksDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "test_initDb");
    dataSource.closeDB();
    FileUtil.saveData(dataSource.getDbPath().toString() + "/CURRENT",
        "...", Boolean.FALSE);
  }
}
