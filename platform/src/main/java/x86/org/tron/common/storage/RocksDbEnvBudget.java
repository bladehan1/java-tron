package org.tron.common.storage;

import org.rocksdb.Env;
import org.rocksdb.RocksDB;

/**
 * Process RocksDB background-thread budget, RocksJava 5.15.10 line (x86 default; design v2
 * ch08 §3.3.2).
 *
 * <p>This line has no only-increase API and no pool-size getter, and its pool ids are inverted
 * relative to their names: {@code Env.FLUSH_POOL} is the C++ LOW (compaction) pool and
 * {@code Env.COMPACTION_POOL} is the C++ HIGH (flush) pool. The adapter keeps a process
 * high-water mark ({@link RocksDbEnvHighWater}) and always sets exactly that mark, so it never
 * lowers a pool. Same API as the 9.7.4 adapter in {@code platform/src/main/java/{x86-high,arm}}.
 * </p>
 */
public final class RocksDbEnvBudget {

  public static final String JNI_LINE = "5.15.10";

  private static final RocksDbEnvHighWater HIGH_WATER = new RocksDbEnvHighWater();

  private RocksDbEnvBudget() {
  }

  public static boolean canReadPoolSizes() {
    return false;
  }

  /** Grows the C++ LOW (compaction) and HIGH (flush) pools to at least the given sizes. */
  public static void raise(int compactionThreads, int flushThreads) {
    RocksDB.loadLibrary();
    final Env env = Env.getDefault();
    // RocksDbEnvHighWater sends compaction with Env.FLUSH_POOL and flush with
    // Env.COMPACTION_POOL: on 5.15.10 the Java pool ids are inverted (env.cc:29-41).
    HIGH_WATER.raiseAndApply(compactionThreads, flushThreads, env::setBackgroundThreads);
  }

  /** Not readable on this line: always -1 (the high-water mark is reported separately). */
  public static int compactionPoolSize() {
    return -1;
  }

  /** Not readable on this line: always -1 (the high-water mark is reported separately). */
  public static int flushPoolSize() {
    return -1;
  }

  /** The last compaction (C++ LOW) value set by this adapter. */
  public static int compactionHighWater() {
    return HIGH_WATER.compactionHighWater();
  }

  /** The last flush (C++ HIGH) value set by this adapter. */
  public static int flushHighWater() {
    return HIGH_WATER.flushHighWater();
  }
}
