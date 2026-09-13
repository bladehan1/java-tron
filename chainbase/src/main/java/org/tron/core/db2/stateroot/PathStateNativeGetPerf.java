package org.tron.core.db2.stateroot;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Optional database-local aggregates of thread-local, one-in-1024 point-read samples. */
final class PathStateNativeGetPerf {
  static final int SAMPLE_EVERY = 1024;
  static final String PROPERTY = "tron.pathstate.nativeGetPerf";
  private final AtomicLong calls = new AtomicLong();
  private final Map<String, Long> totals = new LinkedHashMap<>();
  private final Api api;

  PathStateNativeGetPerf() {
    api = new Api();
    for (String name : new String[]{"samples", "skipped_active", "failed_samples",
        "sampled_get_nanos", "sampled_probe_nanos"}) {
      totals.put(name, 0L);
    }
    for (String name : api.getters.keySet()) {
      totals.put(name, 0L);
    }
  }

  byte[] get(RocksDB database, byte[] key) throws RocksDBException {
    if ((calls.getAndIncrement() & (SAMPLE_EVERY - 1)) != 0) {
      return database.get(key);
    }
    long probeStarted = System.nanoTime();
    Object previous = invoke(api.getLevel, database);
    // Do not borrow a thread already owned by another timing observer.
    String level = ((Enum<?>) previous).name();
    if (!"DISABLE".equals(level) && !"ENABLE_COUNT".equals(level)) {
      add("skipped_active", 1);
      return database.get(key);
    }
    Object context = invoke(api.getContext, database);
    Map<String, Long> before = api.snapshot(context);
    try {
      invoke(api.setLevel, database, api.timedLevel);
      long started = System.nanoTime();
      byte[] value = database.get(key);
      long elapsed = System.nanoTime() - started;
      Map<String, Long> after = api.snapshot(context);
      synchronized (this) {
        add("samples", 1);
        add("sampled_get_nanos", elapsed);
        after.forEach((name, count) -> add(name, count - before.get(name)));
      }
      return value;
    } catch (RocksDBException | RuntimeException failure) {
      add("failed_samples", 1);
      throw failure;
    } finally {
      // No reset: preserve the caller's cumulative context as well as its perf level.
      invoke(api.setLevel, database, previous);
      add("sampled_probe_nanos", System.nanoTime() - probeStarted);
      // The JNI context is borrowed TLS, not owned by the database or this collector.
      invoke(api.closeContext, context);
    }
  }

  synchronized Map<String, Long> snapshot() {
    Map<String, Long> result = new LinkedHashMap<>(totals);
    result.put("calls", calls.get());
    return result;
  }

  private synchronized void add(String name, long delta) {
    if (delta < 0) {
      throw new IllegalStateException("native get perf counter moved backwards: " + name);
    }
    totals.put(name, totals.get(name) + delta);
  }

  private static Object invoke(Method method, Object target, Object... arguments) {
    try {
      return method.invoke(target, arguments);
    } catch (IllegalAccessException | InvocationTargetException failure) {
      throw new IllegalStateException("cannot access RocksDB native get perf", failure);
    }
  }

  /** Resolve once, only when explicitly enabled; the default RocksDB 5.x build has no API. */
  private static final class Api {
    private final Method getLevel;
    private final Method setLevel;
    private final Method getContext;
    private final Method closeContext;
    private final Object timedLevel;
    private final Map<String, Method> getters = new LinkedHashMap<>();

    private Api() {
      try {
        getLevel = RocksDB.class.getMethod("getPerfLevel");
        Class<?> level = getLevel.getReturnType();
        setLevel = RocksDB.class.getMethod("setPerfLevel", level);
        timedLevel = level.getField("ENABLE_TIME_EXCEPT_FOR_MUTEX").get(null);
        getContext = RocksDB.class.getMethod("getPerfContext");
        Class<?> context = getContext.getReturnType();
        closeContext = context.getMethod("close");
        getter(context, "sampled_block_read_count", "getBlockReadCount");
        getter(context, "sampled_block_read_bytes", "getBlockReadByte");
        getter(context, "sampled_block_read_nanos", "getBlockReadTime");
        getter(context, "sampled_index_read_count", "getIndexBlockReadCount");
        getter(context, "sampled_filter_read_count", "getFilterBlockReadCount");
        getter(context, "sampled_cache_hit_count", "getBlockCacheHitCount");
        getter(context, "sampled_checksum_nanos", "getBlockChecksumTime");
        getter(context, "sampled_decompress_nanos", "getBlockDecompressTime");
        getter(context, "sampled_read_index_nanos", "getReadIndexBlockNanos");
        getter(context, "sampled_read_filter_nanos", "getReadFilterBlockNanos");
        getter(context, "sampled_find_table_nanos", "getFindTableNanos");
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException("native get perf requires the RocksDB 9.7.4 API", failure);
      }
    }

    private void getter(Class<?> context, String name, String method)
        throws NoSuchMethodException {
      getters.put(name, context.getMethod(method));
    }

    private Map<String, Long> snapshot(Object context) {
      Map<String, Long> result = new LinkedHashMap<>();
      getters.forEach((name, method) -> result.put(name, (Long) invoke(method, context)));
      return result;
    }
  }
}
