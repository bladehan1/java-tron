package org.tron.common.storage.rocksdb;

import io.prometheus.client.Counter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;
import org.tron.common.setting.RocksDbSettings;

/** Sampled RocksDB PerfContext collection that remains compatible with the legacy JNI. */
@Slf4j(topic = "DB")
final class RocksDbGetPerfContext {

  private static final String[] METRICS = {
      "sampled_get", "hit", "miss", "block_read_count", "block_read_bytes",
      "block_read_nanos", "index_block_read_count", "filter_block_read_count",
      "block_cache_hit_count", "from_memtable_count", "user_key_comparison_count",
      "read_bytes"
  };
  private static final PerfApi PERF_API = PerfApi.load();

  private final RocksDB database;
  private final int sampleOneIn;
  private final Counter.Child[] counters;

  private RocksDbGetPerfContext(RocksDB database, String databaseName, int sampleOneIn) {
    this.database = database;
    this.sampleOneIn = sampleOneIn;
    this.counters = new Counter.Child[METRICS.length];
    for (int i = 0; i < METRICS.length; i++) {
      counters[i] = Metrics.databaseCounterChild(MetricKeys.Counter.DB_GET_PERF,
          "ROCKSDB", databaseName, METRICS[i]);
    }
  }

  static RocksDbGetPerfContext create(RocksDB database, String databaseName) {
    RocksDbSettings settings = RocksDbSettings.getSettings();
    if (!Metrics.databaseEnabled() || !settings.shouldSamplePerfContext(databaseName)) {
      return null;
    }
    if (!PERF_API.supported) {
      logger.warn("RocksDB JNI does not expose PerfContext; skip sampling for {}", databaseName);
      return null;
    }
    return new RocksDbGetPerfContext(database, databaseName,
        settings.getPerfContextSampleOneIn());
  }

  byte[] get(byte[] key) throws RocksDBException {
    if (ThreadLocalRandom.current().nextInt(sampleOneIn) != 0) {
      return database.get(key);
    }

    Object context = null;
    boolean enabled = false;
    try {
      PERF_API.setPerfLevel.invoke(database, PERF_API.enableTime);
      enabled = true;
      context = PERF_API.getPerfContext.invoke(database);
      PERF_API.reset.invoke(context);
    } catch (InvocationTargetException e) {
      logger.warn("Unable to enable RocksDB PerfContext", e.getCause());
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.warn("Unable to enable RocksDB PerfContext", e);
    }
    if (context == null) {
      if (enabled) {
        disablePerfContext();
      }
      return database.get(key);
    }

    try {
      byte[] value = database.get(key);
      try {
        record(context, value != null);
      } catch (ReflectiveOperationException | RuntimeException e) {
        logger.warn("Unable to record RocksDB PerfContext", e);
      }
      return value;
    } finally {
      disablePerfContext();
      closePerfContext(context);
    }
  }

  private void record(Object context, boolean hit) throws ReflectiveOperationException {
    add(0, 1);
    add(hit ? 1 : 2, 1);
    add(3, PERF_API.blockReadCount.invoke(context));
    add(4, PERF_API.blockReadBytes.invoke(context));
    add(5, PERF_API.blockReadNanos.invoke(context));
    add(6, PERF_API.indexBlockReadCount.invoke(context));
    add(7, PERF_API.filterBlockReadCount.invoke(context));
    add(8, PERF_API.blockCacheHitCount.invoke(context));
    add(9, PERF_API.fromMemtableCount.invoke(context));
    add(10, PERF_API.userKeyComparisonCount.invoke(context));
    add(11, PERF_API.readBytes.invoke(context));
  }

  private void add(int index, Object value) {
    long amount = ((Number) value).longValue();
    if (amount > 0) {
      counters[index].inc(amount);
    }
  }

  private void disablePerfContext() {
    try {
      PERF_API.setPerfLevel.invoke(database, PERF_API.disable);
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.warn("Unable to disable RocksDB PerfContext", e);
    }
  }

  private void closePerfContext(Object context) {
    try {
      PERF_API.close.invoke(context);
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.warn("Unable to close RocksDB PerfContext", e);
    }
  }

  private static final class PerfApi {
    private final boolean supported;
    private Object enableTime;
    private Object disable;
    private Method setPerfLevel;
    private Method getPerfContext;
    private Method reset;
    private Method close;
    private Method blockReadCount;
    private Method blockReadBytes;
    private Method blockReadNanos;
    private Method indexBlockReadCount;
    private Method filterBlockReadCount;
    private Method blockCacheHitCount;
    private Method fromMemtableCount;
    private Method userKeyComparisonCount;
    private Method readBytes;

    private PerfApi(boolean supported) {
      this.supported = supported;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static PerfApi load() {
      try {
        Class<?> level = Class.forName("org.rocksdb.PerfLevel");
        Class<?> context = Class.forName("org.rocksdb.PerfContext");
        PerfApi api = new PerfApi(true);
        api.enableTime = Enum.valueOf((Class<? extends Enum>) level,
            "ENABLE_TIME_EXCEPT_FOR_MUTEX");
        api.disable = Enum.valueOf((Class<? extends Enum>) level, "DISABLE");
        api.setPerfLevel = RocksDB.class.getMethod("setPerfLevel", level);
        api.getPerfContext = RocksDB.class.getMethod("getPerfContext");
        api.reset = context.getMethod("reset");
        api.close = context.getMethod("close");
        api.blockReadCount = context.getMethod("getBlockReadCount");
        api.blockReadBytes = context.getMethod("getBlockReadByte");
        api.blockReadNanos = context.getMethod("getBlockReadTime");
        api.indexBlockReadCount = context.getMethod("getIndexBlockReadCount");
        api.filterBlockReadCount = context.getMethod("getFilterBlockReadCount");
        api.blockCacheHitCount = context.getMethod("getBlockCacheHitCount");
        api.fromMemtableCount = context.getMethod("getFromMemtableCount");
        api.userKeyComparisonCount = context.getMethod("getUserKeyComparisonCount");
        api.readBytes = context.getMethod("getReadBytes");
        return api;
      } catch (ReflectiveOperationException | LinkageError e) {
        return new PerfApi(false);
      }
    }
  }
}
