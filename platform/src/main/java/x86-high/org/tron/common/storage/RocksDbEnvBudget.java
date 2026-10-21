package org.tron.common.storage;

import org.rocksdb.Env;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.tron.common.math.MathWrapper;

/**
 * Process RocksDB background-thread budget, RocksJava 9.7.4 line (design v2 ch08 §3.3.2).
 *
 * <p>{@code incBackgroundThreadsIfNeeded} only grows a pool and is atomic on the native side,
 * so there is no read-then-set race with a concurrent open, and pool sizes can be read back.
 * Same API as the 5.15.10 adapter in {@code platform/src/main/java/x86}; common code must call
 * only this class, never a line-specific RocksJava API.</p>
 */
public final class RocksDbEnvBudget {

  public static final String JNI_LINE = "9.7.4";

  private RocksDbEnvBudget() {
  }

  public static boolean canReadPoolSizes() {
    return true;
  }

  /** Grows the C++ LOW (compaction) and HIGH (flush) pools to at least the given sizes. */
  public static void raise(int compactionThreads, int flushThreads) {
    RocksDB.loadLibrary();
    Env env = Env.getDefault();
    env.incBackgroundThreadsIfNeeded(MathWrapper.max(1, compactionThreads), Priority.LOW);
    env.incBackgroundThreadsIfNeeded(MathWrapper.max(1, flushThreads), Priority.HIGH);
  }

  /** C++ LOW (compaction) pool size, or -1 when the line cannot read it back. */
  public static int compactionPoolSize() {
    RocksDB.loadLibrary();
    return Env.getDefault().getBackgroundThreads(Priority.LOW);
  }

  /** C++ HIGH (flush) pool size, or -1 when the line cannot read it back. */
  public static int flushPoolSize() {
    RocksDB.loadLibrary();
    return Env.getDefault().getBackgroundThreads(Priority.HIGH);
  }

  /** Same as {@link #compactionPoolSize()} on this line (sizes are readable). */
  public static int compactionHighWater() {
    return compactionPoolSize();
  }

  /** Same as {@link #flushPoolSize()} on this line (sizes are readable). */
  public static int flushHighWater() {
    return flushPoolSize();
  }
}
