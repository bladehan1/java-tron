package org.tron.common.storage;

/**
 * Process high-water mark for the RocksJava 5.15.10 line (design v2 ch08 §3.3.2 item 2).
 *
 * <p>RocksJava 5.15.10 has neither {@code incBackgroundThreadsIfNeeded} nor a pool-size getter,
 * and {@code SetBackgroundThreads} may shrink a pool. The adapter therefore keeps the largest
 * value ever requested per pool and always (re)sets exactly that value: H is never below any
 * value an open requested through {@code IncBackgroundThreadsIfNeeded}, so the order between a
 * set and a concurrent open does not matter and the pool is never lowered by this code.</p>
 *
 * <p><b>Pool ids are inverted relative to their names on 5.15.10</b>
 * ({@code java/rocksjni/env.cc:29-41}): Java {@code Env.FLUSH_POOL} (0) maps to C++
 * {@code Priority::LOW}, the compaction pool; Java {@code Env.COMPACTION_POOL} (1) maps to C++
 * {@code Priority::HIGH}, the flush pool. The compaction budget is therefore sent with
 * {@link #JAVA_FLUSH_POOL_ID} and the flush budget with {@link #JAVA_COMPACTION_POOL_ID}.</p>
 *
 * <p>Kept in the {@code common} source set (no RocksJava types) so the inversion and the
 * high-water rule are unit-testable on every build.</p>
 */
public final class RocksDbEnvHighWater {

  /** RocksJava 5.15.10 {@code Env.FLUSH_POOL}: C++ LOW pool, i.e. compaction. */
  public static final int JAVA_FLUSH_POOL_ID = 0;
  /** RocksJava 5.15.10 {@code Env.COMPACTION_POOL}: C++ HIGH pool, i.e. flush. */
  public static final int JAVA_COMPACTION_POOL_ID = 1;

  /** Receives one {@code Env.setBackgroundThreads(threads, javaPoolId)} call. */
  public interface PoolSetter {
    void setBackgroundThreads(int threads, int javaPoolId);
  }

  private int compactionHighWater;
  private int flushHighWater;

  /**
   * Raises the marks to at least the given values and applies both marks. Values below 1 are
   * ignored (the mark only grows).
   */
  public synchronized void raiseAndApply(int compactionThreads, int flushThreads,
      PoolSetter setter) {
    compactionHighWater = Math.max(compactionHighWater, Math.max(1, compactionThreads));
    flushHighWater = Math.max(flushHighWater, Math.max(1, flushThreads));
    // LOW (compaction) through FLUSH_POOL, HIGH (flush) through COMPACTION_POOL: see class doc.
    setter.setBackgroundThreads(compactionHighWater, JAVA_FLUSH_POOL_ID);
    setter.setBackgroundThreads(flushHighWater, JAVA_COMPACTION_POOL_ID);
  }

  public synchronized int compactionHighWater() {
    return compactionHighWater;
  }

  public synchronized int flushHighWater() {
    return flushHighWater;
  }
}
