package org.tron.common.storage.rocksdb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.tron.common.setting.RocksDbSettings;

public class ChainbaseAccountCacheCapacityTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private String previous;

  @Before
  public void saveProperty() {
    previous = System.getProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB);
    System.clearProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB);
  }

  @After
  public void restoreProperty() {
    if (previous == null) {
      System.clearProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB);
    } else {
      System.setProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB, previous);
    }
  }

  @Test
  public void actualCapacityIsAccountOnlyAndSurvivesReopen() throws Exception {
    String parent = folder.newFolder().getAbsolutePath();
    long defaultCapacity;
    RocksDbDataSourceImpl baseline = new RocksDbDataSourceImpl(parent, "account");
    try {
      defaultCapacity = baseline.getDatabase().getLongProperty("rocksdb.block-cache-capacity");
    } finally {
      baseline.closeDB();
    }
    for (String capacity : new String[]{"8", "64", "8"}) {
      System.setProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB, capacity);
      RocksDbDataSourceImpl db = new RocksDbDataSourceImpl(parent, "account");
      try {
        assertEquals(Long.parseLong(capacity) * 1024 * 1024,
            db.getDatabase().getLongProperty("rocksdb.block-cache-capacity"));
        db.putData(new byte[]{1}, new byte[]{2});
        assertArrayEquals(new byte[]{2}, db.getData(new byte[]{1}));
        db.resetDb();
        assertEquals(Long.parseLong(capacity) * 1024 * 1024,
            db.getDatabase().getLongProperty("rocksdb.block-cache-capacity"));
      } finally {
        db.closeDB();
      }
    }
    System.setProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB, "64");
    for (String name : new String[]{"account-asset", "storage-row", "code"}) {
      RocksDbDataSourceImpl db = new RocksDbDataSourceImpl(parent, name);
      try {
        assertEquals(defaultCapacity,
            db.getDatabase().getLongProperty("rocksdb.block-cache-capacity"));
      } finally {
        db.closeDB();
      }
    }
    System.clearProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB);
    RocksDbDataSourceImpl db = new RocksDbDataSourceImpl(parent, "account");
    try {
      assertEquals(defaultCapacity,
          db.getDatabase().getLongProperty("rocksdb.block-cache-capacity"));
    } finally {
      db.closeDB();
    }
  }

  @Test
  public void rejectsInvalidBudgetBeforeAllocatingOptions() {
    for (String capacity : new String[]{"", "0", "-1", "256", "invalid"}) {
      System.setProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB, capacity);
      assertThrows(IllegalArgumentException.class,
          () -> RocksDbSettings.getOptionsByDbName("account"));
    }
  }

  @Test
  public void fixedSstReuseImprovesInBothOrdersWithoutChangingValues() throws Exception {
    Path path = folder.newFolder().toPath().resolve("account");
    byte[][] values = new byte[8192][2048];
    Random random = new Random(20260913);
    try (Options options = RocksDbSettings.getOptionsByDbName("account");
         RocksDB db = RocksDB.open(options, path.toString());
         FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
      for (int i = 0; i < values.length; i++) {
        random.nextBytes(values[i]);
        db.put(key(i), values[i]);
      }
      db.flush(flush);
    }
    Map<String, String> frozenSst = sstHashes(path);
    assertTrue(!frozenSst.isEmpty());
    long[] misses = new long[4];
    int[] capacities = {8, 64, 64, 8};
    for (int run = 0; run < capacities.length; run++) {
      System.setProperty(RocksDbSettings.ACCOUNT_CACHE_BENCHMARK_MIB,
          Integer.toString(capacities[run]));
      try (Statistics statistics = new Statistics();
           Options options = RocksDbSettings.getOptionsByDbName("account")
               .setStatistics(statistics);
           RocksDB db = RocksDB.open(options, path.toString())) {
        for (int pass = 0; pass < 2; pass++) {
          long before = statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
          for (int i = 0; i < values.length; i++) {
            assertArrayEquals(values[i], db.get(key(i)));
          }
          long delta = statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS) - before;
          if (pass == 1) {
            misses[run] = delta;
          }
          System.out.println("account fixed-sst run=" + run + " capacityMiB="
              + capacities[run] + " pass=" + pass + " dataMiss=" + delta);
        }
        // Inspect the persisted native table options, not the mutable Java builder.
        Path latest;
        try (Stream<Path> files = Files.list(path)) {
          latest = files.filter(p -> p.getFileName().toString().startsWith("OPTIONS-"))
              .max(java.util.Comparator.comparing(p -> p.getFileName().toString())).get();
        }
        String persisted = Files.readString(latest);
        assertTrue(persisted.contains("block_size=4096"));
        assertTrue(persisted.contains("cache_index_and_filter_blocks=false"));
        assertTrue(persisted.contains("filter_policy=nullptr"));
      }
      assertEquals("all arms must read the same immutable SST bytes", frozenSst, sstHashes(path));
    }
    assertTrue("forward order must improve cache reuse", misses[0] > misses[1]);
    assertTrue("reverse order must improve cache reuse", misses[3] > misses[2]);
    assertEquals(0, misses[1]);
    assertEquals(0, misses[2]);
  }

  private static byte[] key(int i) {
    return ByteBuffer.allocate(4).putInt(i).array();
  }

  private static Map<String, String> sstHashes(Path directory) throws Exception {
    Map<String, String> result = new TreeMap<>();
    try (Stream<Path> files = Files.list(directory)) {
      for (Path file : files.filter(p -> p.toString().endsWith(".sst"))
          .collect(Collectors.toList())) {
        result.put(file.getFileName().toString(), Arrays.toString(
            MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
      }
    }
    return result;
  }
}
