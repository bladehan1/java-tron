package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateNativeGetPerfTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  private static void requireApi() {
    RocksDB.loadLibrary();
    try {
      RocksDB.class.getMethod("getPerfContext");
    } catch (NoSuchMethodException unavailable) {
      Assume.assumeNoException(unavailable);
    }
  }

  private static Object level(RocksDB database) throws Exception {
    return RocksDB.class.getMethod("getPerfLevel").invoke(database);
  }

  private static void level(RocksDB database, Object value) throws Exception {
    RocksDB.class.getMethod("setPerfLevel", value.getClass()).invoke(database, value);
  }

  @Test
  public void readsFixedSstAndRestoresThreadLevelWithoutResettingCounters() throws Exception {
    requireApi();
    Path path = temporary.newFolder().toPath();
    byte[] key = new byte[]{1};
    byte[] value = new byte[8192];
    Arrays.fill(value, (byte) 7);
    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB database = RocksDB.open(options, path.toString());
        FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
      database.put(key, value);
      database.flush(flush);
    }
    Path sst;
    try (Stream<Path> files = Files.list(path)) {
      sst = files.filter(file -> file.toString().endsWith(".sst")).findFirst().get();
    }
    byte[] sstHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(sst));
    try (Options options = new Options();
        RocksDB database = RocksDB.open(options, path.toString())) {
      Object previous = level(database);
      Object count = previous.getClass().getField("ENABLE_COUNT").get(null);
      try {
        level(database, count);
        Object context = RocksDB.class.getMethod("getPerfContext").invoke(database);
        Method readCount = context.getClass().getMethod("getBlockReadCount");
        long before = (Long) readCount.invoke(context);
        PathStateNativeGetPerf perf = new PathStateNativeGetPerf();
        for (int i = 0; i < 2049; i++) {
          assertArrayEquals(value, perf.get(database, key));
          assertEquals(count, level(database));
        }
        Map<String, Long> stats = perf.snapshot();
        assertEquals(Long.valueOf(2049), stats.get("calls"));
        assertEquals(Long.valueOf(3), stats.get("samples"));
        assertTrue(stats.get("sampled_block_read_count") > 0);
        assertTrue(stats.get("sampled_block_read_bytes") > 0);
        assertTrue(stats.get("sampled_cache_hit_count") > 0);
        assertTrue(stats.get("sampled_get_nanos") > 0);
        assertTrue(stats.get("sampled_probe_nanos") >= stats.get("sampled_get_nanos"));
        assertTrue((Long) readCount.invoke(context) >= before
            + stats.get("sampled_block_read_count"));
        ((AutoCloseable) context).close();
        PathStateNativeGetPerf failing = new PathStateNativeGetPerf();
        assertThrows(NullPointerException.class, () -> failing.get(database, null));
        assertEquals(count, level(database));
        assertEquals(Long.valueOf(1), failing.snapshot().get("failed_samples"));
        assertArrayEquals(sstHash,
            MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(sst)));
      } finally {
        level(database, previous);
      }
    }
  }

  @Test
  public void workersUseTheirOwnContextAndDoNotOverrideAnActiveObserver() throws Exception {
    requireApi();
    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB database = RocksDB.open(options, temporary.newFolder().getPath())) {
      Object previous = level(database);
      Object timed = previous.getClass().getField("ENABLE_TIME").get(null);
      Object count = previous.getClass().getField("ENABLE_COUNT").get(null);
      PathStateNativeGetPerf perf = new PathStateNativeGetPerf();
      ExecutorService workers = Executors.newFixedThreadPool(2);
      try {
        level(database, timed);
        assertNull(perf.get(database, new byte[]{1}));
        assertEquals(timed, level(database));
        assertEquals(Long.valueOf(1), perf.snapshot().get("skipped_active"));
        Future<?> first = workers.submit(() -> workerReads(database, perf, count));
        Future<?> second = workers.submit(() -> workerReads(database, perf, count));
        first.get();
        second.get();
        assertEquals(timed, level(database));
        assertEquals(Long.valueOf(2049), perf.snapshot().get("calls"));
        assertEquals(Long.valueOf(2), perf.snapshot().get("samples"));
      } finally {
        workers.shutdownNow();
        level(database, previous);
      }
    }
  }

  private static void workerReads(RocksDB database, PathStateNativeGetPerf perf, Object count) {
    try {
      Object before = level(database);
      try {
        level(database, count);
        for (int i = 0; i < 1024; i++) {
          assertNull(perf.get(database, new byte[]{1}));
          assertEquals(count, level(database));
        }
      } finally {
        level(database, before);
      }
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  @Test
  public void disabledByDefaultAndSeparateFromDatabaseTickers() throws Exception {
    requireApi();
    String previous = System.getProperty(PathStateNativeGetPerf.PROPERTY);
    try {
      System.clearProperty(PathStateNativeGetPerf.PROPERTY);
      Path path = temporary.newFolder().toPath();
      try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(path, Engine.ROCKSDB,
          "small", NativeDbConfig.small(), -1, true)) {
        assertNull(store.readPerfStatistics());
        assertNull(store.get(new byte[]{1}));
        assertEquals(11, store.readStatistics().size());
      }
      System.setProperty(PathStateNativeGetPerf.PROPERTY, "true");
      try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(path, Engine.ROCKSDB,
          "small", NativeDbConfig.small(), -1, true)) {
        assertNull(store.get(new byte[]{1}));
        assertEquals(Long.valueOf(1), store.readPerfStatistics().get("calls"));
        assertFalse(store.readStatistics().containsKey("samples"));
      }
    } finally {
      if (previous == null) {
        System.clearProperty(PathStateNativeGetPerf.PROPERTY);
      } else {
        System.setProperty(PathStateNativeGetPerf.PROPERTY, previous);
      }
    }
  }
}
