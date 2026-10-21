package org.tron.common.setting;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.storage.RocksDbEnvBudget;

/**
 * Process-wide RocksDB background-thread budget (design v2 ch08 §3.3, E6, WP8-4).
 *
 * <p>RocksDB background threads belong to the process-wide {@code Env::Default()} pools (LOW =
 * compaction, HIGH = flush). A database's {@code max_background_compactions/flushes} only caps
 * that database's concurrent jobs, so the pool size is set here, once per process, through the
 * per-JNI-line adapter {@link RocksDbEnvBudget} (never through a line-specific RocksJava API).
 * </p>
 *
 * <ul>
 *   <li>Activation: only when an enabled State Archive / PathState family resolves to ROCKSDB
 *   in the Archive node policy. Inactive, no Env API is called at
 *   all (G3).</li>
 *   <li>Only-increase: the compaction target is max(budget, Chainbase {@code compactThreads}
 *   when Chainbase is RocksDB, every auxiliary {@code backgroundCompactions}); the flush target
 *   is max(budget, every auxiliary {@code backgroundFlushes}). 9.7.4 applies them with
 *   {@code incBackgroundThreadsIfNeeded}; 5.15.10 sets its process high-water mark.</li>
 *   <li>Re-assertion: before and after each auxiliary RocksDB open; after every Chainbase
 *   {@code RocksDbSettings.getOptionsByDbName} (whose {@code setIncreaseParallelism(1)} resets
 *   the pools; the Options themselves are untouched); and periodically. 9.7.4 reads the pools
 *   back and counts a lowered pool; 5.15.10 cannot read back and counts each re-application.</li>
 * </ul>
 */
@Slf4j(topic = "DB")
public final class RocksDbThreadBudget {

  /** The Env operations; production uses the per-JNI-line adapter. */
  public interface EnvOps {
    void raise(int compactionThreads, int flushThreads);

    boolean canReadPoolSizes();

    int compactionPoolSize();

    int flushPoolSize();
  }

  static final EnvOps PLATFORM = new EnvOps() {
    @Override
    public void raise(int compactionThreads, int flushThreads) {
      RocksDbEnvBudget.raise(compactionThreads, flushThreads);
    }

    @Override
    public boolean canReadPoolSizes() {
      return RocksDbEnvBudget.canReadPoolSizes();
    }

    @Override
    public int compactionPoolSize() {
      return RocksDbEnvBudget.compactionPoolSize();
    }

    @Override
    public int flushPoolSize() {
      return RocksDbEnvBudget.flushPoolSize();
    }
  };

  private static final long PERIOD_SECONDS = 60L;

  private static EnvOps ops = PLATFORM;
  private static volatile boolean active;
  private static int compactionTarget;
  private static int flushTarget;
  private static ScheduledExecutorService periodic;
  private static final AtomicLong ENV_CALLS = new AtomicLong();
  private static final AtomicLong REASSERTIONS = new AtomicLong();
  private static final AtomicLong LOWERED = new AtomicLong();

  private RocksDbThreadBudget() {
  }

  /**
   * Activates the budget. {@code chainbaseCompactThreads} is the Chainbase RocksDB
   * {@code compactThreads}, or 0 when Chainbase is LevelDB. Idempotent; later calls only raise.
   */
  public static synchronized void activate(int compactionThreads, int flushThreads,
      int chainbaseCompactThreads, boolean schedulePeriodicCheck) {
    if (compactionThreads <= 0 || flushThreads <= 0) {
      throw new IllegalArgumentException("RocksDB thread budget must be positive");
    }
    compactionTarget = Math.max(compactionTarget,
        Math.max(compactionThreads, chainbaseCompactThreads));
    flushTarget = Math.max(flushTarget, flushThreads);
    active = true;
    apply();
    logger.info("RocksDB process thread budget active: jniLine={}, LOW(compaction)={}, "
            + "HIGH(flush)={}, readBack={}", RocksDbEnvBudget.JNI_LINE, compactionTarget,
        flushTarget, describePools());
    if (schedulePeriodicCheck && periodic == null) {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "rocksdb-thread-budget");
        thread.setDaemon(true);
        return thread;
      });
      executor.scheduleWithFixedDelay(RocksDbThreadBudget::periodicCheck, PERIOD_SECONDS,
          PERIOD_SECONDS, TimeUnit.SECONDS);
      periodic = executor;
    }
  }

  public static boolean isActive() {
    return active;
  }

  /** Before an auxiliary RocksDB open: its job limits join the targets (only-increase). */
  public static synchronized void beforeAuxiliaryOpen(int backgroundCompactions,
      int backgroundFlushes) {
    if (!active) {
      return;
    }
    compactionTarget = Math.max(compactionTarget, backgroundCompactions);
    flushTarget = Math.max(flushTarget, backgroundFlushes);
    apply();
  }

  /** After an auxiliary RocksDB open. */
  public static void afterAuxiliaryOpen() {
    reassert();
  }

  /**
   * After Chainbase Options construction ({@code setIncreaseParallelism(1)} resets both pools).
   * Changes only the Env, never the Options, so Chainbase {@code OPTIONS-*} stay upstream.
   */
  public static void afterChainbaseOptions() {
    if (active) {
      reassert();
    }
  }

  /** Periodic re-check (also used by tests). */
  public static void periodicCheck() {
    try {
      reassert();
    } catch (RuntimeException failure) {
      logger.warn("RocksDB thread budget re-check failed", failure);
    }
  }

  private static synchronized void reassert() {
    if (!active) {
      return;
    }
    if (ops.canReadPoolSizes()) {
      int compaction = ops.compactionPoolSize();
      int flush = ops.flushPoolSize();
      if (compaction >= compactionTarget && flush >= flushTarget) {
        return;
      }
      LOWERED.incrementAndGet();
      logger.debug("RocksDB pool lowered to LOW={} HIGH={}; restoring {}/{}", compaction, flush,
          compactionTarget, flushTarget);
    }
    REASSERTIONS.incrementAndGet();
    apply();
  }

  private static void apply() {
    ENV_CALLS.incrementAndGet();
    ops.raise(compactionTarget, flushTarget);
  }

  private static String describePools() {
    if (!ops.canReadPoolSizes()) {
      return "unavailable(5.15.10 high-water " + compactionTarget + "/" + flushTarget + ")";
    }
    return ops.compactionPoolSize() + "/" + ops.flushPoolSize();
  }

  public static synchronized int getCompactionTarget() {
    return compactionTarget;
  }

  public static synchronized int getFlushTarget() {
    return flushTarget;
  }

  /** Number of Env calls made by this class; 0 whenever the budget never activated (G3). */
  public static long getEnvCallCount() {
    return ENV_CALLS.get();
  }

  /** Re-applications: a lowered pool on 9.7.4, or every unverifiable re-check on 5.15.10. */
  public static long getReassertionCount() {
    return REASSERTIONS.get();
  }

  /** Pools found below target (9.7.4 only). */
  public static long getLoweredCount() {
    return LOWERED.get();
  }

  /** Stops observation without shrinking the process-wide native pools. */
  public static synchronized void deactivate() {
    active = false;
    if (periodic != null) {
      periodic.shutdownNow();
      periodic = null;
    }
  }

  /** Test hook: replaces the Env operations and resets all state. */
  public static synchronized void resetForTest(EnvOps replacement) {
    ops = replacement == null ? PLATFORM : replacement;
    active = false;
    compactionTarget = 0;
    flushTarget = 0;
    ENV_CALLS.set(0);
    REASSERTIONS.set(0);
    LOWERED.set(0);
    if (periodic != null) {
      periodic.shutdownNow();
      periodic = null;
    }
  }
}
