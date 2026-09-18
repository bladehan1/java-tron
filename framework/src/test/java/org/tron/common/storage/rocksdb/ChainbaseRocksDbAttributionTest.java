package org.tron.common.storage.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.prometheus.client.Collector.MetricFamilySamples;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.FlushOptions;
import org.tron.common.prometheus.ChainbaseRocksDbExports;
import org.tron.core.exception.TronError;

public class ChainbaseRocksDbAttributionTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private String previous;
  private final ChainbaseRocksDbExports exporter = new ChainbaseRocksDbExports();

  @Before
  public void enable() {
    previous = System.getProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY);
    System.setProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY, "true");
  }

  @After
  public void restore() {
    if (previous == null) {
      System.clearProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY);
    } else {
      System.setProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY, previous);
    }
  }

  private RocksDbDataSourceImpl open(String name) throws Exception {
    return new RocksDbDataSourceImpl(folder.newFolder().getAbsolutePath(), name);
  }

  private double value(String metric, String... labels) {
    return exporter.collect().stream().flatMap(f -> f.samples.stream())
        .filter(s -> s.name.equals(metric) && s.labelValues.equals(Arrays.asList(labels)))
        .mapToDouble(s -> s.value).findFirst().orElse(Double.NaN);
  }

  @Test
  public void recordsNativeReadsActualPropertiesAndResetEpoch() throws Exception {
    RocksDbDataSourceImpl db = open("account");
    try {
      byte[] key = {1};
      db.putData(key, new byte[]{2, 3});
      try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
        db.getDatabase().flush(flush);
      }
      db.getData(key);
      db.getData(key);
      assertTrue(value("tron_chainbase_rocksdb_read_count", "account", "bytes_read") > 0);
      assertEquals(db.getDatabase().getLongProperty("rocksdb.block-cache-capacity"),
          value("tron_chainbase_rocksdb_bytes", "account", "block-cache-capacity"), 0);
      double epoch = value("tron_chainbase_rocksdb_epoch", "account");
      db.resetDb();
      assertNotEquals(epoch, value("tron_chainbase_rocksdb_epoch", "account"), 0);
      assertEquals(0, value("tron_chainbase_rocksdb_read_count", "account", "bytes_read"), 0);
    } finally {
      db.closeDB();
    }
    assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_available", "account")));
  }

  @Test
  public void doesNotRegisterDisabledOrUnselectedStores() throws Exception {
    System.clearProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY);
    RocksDbDataSourceImpl disabled = open("account");
    try {
      assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_available", "account")));
    } finally {
      disabled.closeDB();
    }
    System.setProperty(ChainbaseRocksDbExports.ENABLE_PROPERTY, "true");
    RocksDbDataSourceImpl other = open("code");
    try {
      assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_available", "code")));
    } finally {
      other.closeDB();
    }
  }

  @Test
  public void duplicateNamesAreUnavailableUntilOneOwnerCloses() throws Exception {
    RocksDbDataSourceImpl first = open("account-asset");
    RocksDbDataSourceImpl second = null;
    try {
      second = open("account-asset");
      assertEquals(0, value("tron_chainbase_rocksdb_available", "account-asset"), 0);
      assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_epoch", "account-asset")));
      first.closeDB();
      assertEquals(1, value("tron_chainbase_rocksdb_available", "account-asset"), 0);
    } finally {
      first.closeDB();
      if (second != null) {
        second.closeDB();
      }
    }
  }

  @Test
  public void scrapeCanRaceResetAndClose() throws Exception {
    RocksDbDataSourceImpl db = open("storage-row");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> scrape = executor.submit(() -> {
        for (int i = 0; i < 500; i++) {
          exporter.collect();
        }
      });
      for (int i = 0; i < 3; i++) {
        db.resetDb();
      }
      db.closeDB();
      scrape.get(30, TimeUnit.SECONDS);
      assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_available", "storage-row")));
    } finally {
      db.closeDB();
      executor.shutdownNow();
    }
  }

  @Test
  public void failedOpenDoesNotPublishMetrics() throws Exception {
    Path parent = folder.newFolder().toPath();
    Path db = Files.createDirectory(parent.resolve("account"));
    Files.write(db.resolve("CURRENT"), "missing-manifest\n".getBytes(StandardCharsets.UTF_8));
    assertThrows(TronError.class, () -> new RocksDbDataSourceImpl(parent.toString(), "account"));
    assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_available", "account")));
  }

  @Test
  public void unavailableReaderDoesNotEmitZeroCountsAndFilterWorks() {
    try (ChainbaseRocksDbExports.Registration registration =
        ChainbaseRocksDbExports.register("account", () -> null)) {
      assertEquals(0, value("tron_chainbase_rocksdb_available", "account"), 0);
      assertTrue(Double.isNaN(value("tron_chainbase_rocksdb_read_count", "account", "bytes_read")));
      List<MetricFamilySamples> filtered = exporter.collect(
          name -> name.equals("tron_chainbase_rocksdb_available"));
      assertEquals(1, filtered.size());
      assertFalse(filtered.get(0).samples.isEmpty());
    }
  }
}
