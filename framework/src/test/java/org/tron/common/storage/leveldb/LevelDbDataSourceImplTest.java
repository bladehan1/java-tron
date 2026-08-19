/*
 * Copyright (c) [2016] [ <ether.camp> ]
 * This file is part of the ethereumJ library.
 *
 * The ethereumJ library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ethereumJ library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the ethereumJ library. If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.common.storage.leveldb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.slf4j.LoggerFactory;
import org.tron.common.TestConstants;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.common.utils.FileUtil;
import org.tron.common.utils.PropUtil;
import org.tron.common.utils.ReflectUtils;
import org.tron.common.utils.StorageUtils;
import org.tron.core.config.args.Args;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.exception.TronError;

/**
 * LevelDB-specific tests. Common DB tests are in {@link
 * org.tron.common.storage.DbDataSourceImplTest}.
 */
public class LevelDbDataSourceImplTest {

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private byte[] key1 = "00000001aa".getBytes();
  private byte[] value1 = "10000".getBytes();

  static {
    RocksDB.loadLibrary();
  }

  @AfterClass
  public static void destroy() {
    Args.clearParam();
  }

  @Before
  public void initDb() throws IOException {
    Args.setParam(new String[]{"--output-directory",
        temporaryFolder.newFolder().toString()}, TestConstants.TEST_CONF);
  }

  @Test
  public void initDbTest() {
    makeExceptionDb("test_initDb");
    TronError thrown = assertThrows(TronError.class, () -> new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "test_initDb"));
    assertEquals(TronError.ErrCode.LEVELDB_INIT, thrown.getErrCode());
  }

  @Test
  public void testCheckOrInitEngine() {
    String dir =
        Args.getInstance().getOutputDirectory() + Args.getInstance().getStorage().getDbDirectory();
    String enginePath = dir + File.separator + "test_engine" + File.separator + "engine.properties";
    FileUtil.createDirIfNotExists(dir + File.separator + "test_engine");
    FileUtil.createFileIfNotExists(enginePath);
    PropUtil.writeProperty(enginePath, "ENGINE", "LEVELDB");
    Assert.assertEquals("LEVELDB", PropUtil.readProperty(enginePath, "ENGINE"));

    LevelDbDataSourceImpl dataSource;
    dataSource = new LevelDbDataSourceImpl(dir, "test_engine");
    dataSource.closeDB();

    PropUtil.writeProperty(enginePath, "ENGINE", "ROCKSDB");
    Assert.assertEquals("ROCKSDB", PropUtil.readProperty(enginePath, "ENGINE"));
    try {
      new LevelDbDataSourceImpl(dir, "test_engine");
    } catch (TronError e) {
      Assert.assertEquals("Cannot open ROCKSDB database with LEVELDB engine.", e.getMessage());
    }
  }

  @Test
  public void testLevelDbOpenRocksDb() {
    String name = "test_openRocksDb";
    String output = java.nio.file.Paths
        .get(StorageUtils.getOutputDirectoryByDbName(name), CommonParameter
            .getInstance().getStorage().getDbDirectory()).toString();
    RocksDbDataSourceImpl rocksDb = new RocksDbDataSourceImpl(output, name);
    rocksDb.putData(key1, value1);
    rocksDb.closeDB();
    exception.expectMessage("Cannot open ROCKSDB database with LEVELDB engine.");
    new LevelDbDataSourceImpl(StorageUtils.getOutputDirectoryByDbName(name), name);
  }

  private void makeExceptionDb(String dbName) {
    LevelDbDataSourceImpl dataSource = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "test_initDb");
    dataSource.closeDB();
    FileUtil.saveData(dataSource.getDbPath().toString() + "/CURRENT",
        "...", Boolean.FALSE);
  }

  @Test
  public void slowOpen() throws IOException {
    Logger dbLogger = (Logger) LoggerFactory.getLogger("DB");
    ListAppender<ILoggingEvent> dbAppender = new ListAppender<>();
    dbAppender.start();
    dbLogger.addAppender(dbAppender);
    try {
      final File dbDir = temporaryFolder.newFolder();
      final Path dbPath = dbDir.toPath();
      final String watchdogDbName = "slow-open-db";

      LevelDbDataSourceImpl ds = new LevelDbDataSourceImpl();
      ReflectUtils.setFieldValue(ds, "dataBaseName", watchdogDbName);
      ReflectUtils.setFieldValue(ds, "parentPath", dbDir.getParent());
      long startNs = System.nanoTime() - TimeUnit.SECONDS.toNanos(61);
      ds.logSlowOpen(dbPath, startNs);

      List<ILoggingEvent> warns = dbAppender.list.stream()
          .filter(e -> e.getLevel() == Level.WARN)
          .collect(Collectors.toList());
      assertEquals("expected exactly one WARN event", 1, warns.size());
      ILoggingEvent warn = warns.get(0);
      assertNotNull("expected one WARN from the watchdog helper", warn);
      String rendered = warn.getFormattedMessage();
      assertTrue("WARN should include the Toolkit remediation hint: " + rendered,
          rendered.contains("Toolkit.jar db archive -d"));
      assertTrue("WARN should echo the db name: " + rendered,
          rendered.contains(watchdogDbName));
    } finally {
      dbAppender.stop();
      dbLogger.detachAppender(dbAppender);
    }
  }

  @Test
  public void fastOpen() {
    Logger dbLogger = (Logger) LoggerFactory.getLogger("DB");
    ListAppender<ILoggingEvent> dbAppender = new ListAppender<>();
    dbAppender.start();
    dbLogger.addAppender(dbAppender);
    try {
      String dir = Args.getInstance().getOutputDirectory()
          + Args.getInstance().getStorage().getDbDirectory();
      LevelDbDataSourceImpl ds = new LevelDbDataSourceImpl(dir, "test_fast_open");
      ds.closeDB();
      long warnCount = dbAppender.list.stream()
          .filter(e -> e.getLevel() == Level.WARN)
          .count();
      assertEquals("no WARN should fire for a fast open", 0, warnCount);
    } finally {
      dbAppender.stop();
      dbLogger.detachAppender(dbAppender);
    }
  }

  @Test
  public void nativeSnapshotKeepsOldValueAfterLiveWrite() throws Exception {
    LevelDbDataSourceImpl dataSource = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "nativeSnapshotKeepsOldValue");
    dataSource.putData(key1, value1);
    try (LevelDbDataSourceImpl.PinnedSnapshot snapshot = dataSource.pinSnapshot()) {
      dataSource.putData(key1, "replacement".getBytes());
      assertArrayEquals(value1, snapshot.get(key1));
      assertEquals(dataSource.getSnapshotSourceIdentity(), snapshot.getSourceIdentity());
    }
    dataSource.closeDB();
  }

  @Test
  public void levelDbWrapperExposesSnapshotCapability() throws Exception {
    LevelDbDataSourceImpl dataSource = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "wrapperSnapshotCapability");
    org.tron.core.db2.common.LevelDB wrapper = new org.tron.core.db2.common.LevelDB(dataSource);
    wrapper.put(key1, value1);
    assertTrue(wrapper instanceof SnapshotCapableStore);
    try (StoreSnapshot snapshot = wrapper.pin(1, new byte[32])) {
      wrapper.put(key1, "20000".getBytes());
      assertArrayEquals(value1, snapshot.get(key1));
      assertEquals(wrapper.getSourceIdentity(), snapshot.getSourceIdentity());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void closeWaitsForCrossThreadSnapshotRelease() throws Exception {
    LevelDbDataSourceImpl dataSource = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "closeWaitsForSnapshot");
    LevelDbDataSourceImpl.PinnedSnapshot snapshot = dataSource.pinSnapshot();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch closeStarted = new CountDownLatch(1);
    try {
      Future<?> close = executor.submit(() -> {
        closeStarted.countDown();
        dataSource.closeDB();
      });
      assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
      executor.submit(() -> {
        snapshot.close();
        return null;
      }).get(5, TimeUnit.SECONDS);
      close.get(5, TimeUnit.SECONDS);
    } finally {
      snapshot.close();
      executor.shutdownNow();
      dataSource.closeDB();
    }
  }

  @Test
  public void resetWaitsForPinAndChangesEngineSourceIdentity() throws Exception {
    LevelDbDataSourceImpl dataSource = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), "resetWaitsForSnapshot");
    String originalIdentity = dataSource.getSnapshotSourceIdentity();
    LevelDbDataSourceImpl.PinnedSnapshot snapshot = dataSource.pinSnapshot();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch resetStarted = new CountDownLatch(1);
    try {
      Future<?> reset = executor.submit(() -> {
        resetStarted.countDown();
        dataSource.resetDb();
      });
      assertTrue(resetStarted.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> reset.get(100, TimeUnit.MILLISECONDS));
      snapshot.close();
      reset.get(5, TimeUnit.SECONDS);
      assertNotEquals(originalIdentity, dataSource.getSnapshotSourceIdentity());
      assertThrows(IllegalStateException.class, () -> snapshot.get(key1));
    } finally {
      snapshot.close();
      executor.shutdownNow();
      dataSource.closeDB();
    }
  }

  @Test
  public void sourceIdentityPersistsAcrossProcessStyleReopen() {
    String name = "sourceIdentityPersistsAcrossReopen";
    LevelDbDataSourceImpl first = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), name);
    String identity = first.getSnapshotSourceIdentity();
    first.closeDB();

    LevelDbDataSourceImpl reopened = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), name);
    try {
      assertEquals(identity, reopened.getSnapshotSourceIdentity());
    } finally {
      reopened.closeDB();
    }
  }

  @Test
  public void corruptSourceIdentityFailsBeforeDatabaseReopen() throws IOException {
    String name = "corruptSourceIdentityFailsClosed";
    LevelDbDataSourceImpl first = new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), name);
    Path identityPath = first.getDbPath().resolve(".archive-engine.identity");
    first.closeDB();

    byte[] corrupted = Files.readAllBytes(identityPath);
    corrupted[corrupted.length - 1] ^= 1;
    Files.write(identityPath, corrupted);

    assertThrows(TronError.class, () -> new LevelDbDataSourceImpl(
        Args.getInstance().getOutputDirectory(), name));
    assertArrayEquals(corrupted, Files.readAllBytes(identityPath));
  }
}
