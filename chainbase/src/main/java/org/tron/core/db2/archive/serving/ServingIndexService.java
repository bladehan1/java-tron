package org.tron.core.db2.archive.serving;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.contract.CheckpointPointReader;
import org.tron.core.db2.archive.contract.HistoricalQueryException;
import org.tron.core.db2.archive.contract.StateArchivePointSource;
import org.tron.core.db2.archive.history.io.ArchiveFileDurability;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.source.ServingHistorySource;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.Committed;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.CommittedBlock;
import org.tron.core.db2.archive.serving.source.StateArchiveServingSource;
import org.tron.core.db2.archive.serving.status.ServingFailure;
import org.tron.core.db2.archive.serving.status.ServingFailure.Reason;
import org.tron.core.db2.archive.serving.status.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.serving.status.ServingState;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * The derived V5 serving index (ch04): one build thread, one long-lived write handle, the state
 * machine OPENING → BULK_CATCH_UP / LIVE / DEGRADED / REBUILDING → CLOSED, and the query lease
 * protocol.
 *
 * <ul>
 *   <li>Decoupled (D5, §3.3.1): Common/History only call {@link #onCommonPublished}, which is an
 *       O(1) lock-free mailbox update plus unpark and never throws. Nothing in Common or History
 *       reads serving state. Serving failures only degrade serving (S-INV-2).</li>
 *   <li>Merged watermark (S2): the work range is always {@code (I, W]} of the published History
 *       boundary; notifications only wake the thread.</li>
 *   <li>Budgets choose boundaries, never success (S3/S4, S-INV-11): normal batches are halved at
 *       block boundaries; a single oversized block is a key-atomic SOLO batch.</li>
 *   <li>Readiness (S5, S-INV-8): the initial state is derived from durable data; LIVE is entered
 *       when a commit reaches {@code I == W} and left when {@code W - I > live.maxLagBlocks}. No
 *       block-import call, no P2P signal.</li>
 *   <li>Never dies (S6, S-INV-9): every build failure is classified (§5.1) and moves to DEGRADED;
 *       transient failures retry with backoff after draining leases and reopening the handle.</li>
 * </ul>
 */
public final class ServingIndexService implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger("DB");
  static final String DISCARDED_PREFIX = "discarded-";
  /** PR #55 location of serving inside the History root (ch04 §3.2.5). */
  static final String LEGACY_DIRECTORY = "serving-index-v3";
  /** Above this backlog the exact backlog bytes are not measured (reported as -1). */
  private static final long BACKLOG_BYTES_LIMIT = 2_000;

  private final Path servingRoot;
  private final Path historyRoot;
  private final Path legacyServing;
  private final Engine engine;
  private final ServingHistorySource source;
  private final Config config;
  private final Hooks hooks;
  private final LongSupplier clock;
  private final ServingBatchPlanner planner;
  private final ServingIndexStore.WriteBudget budget;
  private final AtomicReference<CommonCheckpointTarget> latestNotified = new AtomicReference<>();
  private final AtomicInteger leases = new AtomicInteger();
  private final AtomicReference<ServingFailure> reportedByQuery = new AtomicReference<>();
  private final Object viewMonitor = new Object();
  private final Thread thread;
  private volatile ServingView view;
  private volatile boolean closing;
  private volatile boolean started;
  /** Operator-thread request, consumed by the build thread. */
  private volatile boolean rebuildRequested;
  /** ch09 OPS-D6: true while the disk budget is at RED or above (ch04 §5.1 DISK_PRESSURE). */
  private volatile BooleanSupplier diskPressure = () -> false;

  // Build-thread state.
  private ServingIndexStore store;
  private long handleSeq;
  private ServingState state = ServingState.OPENING;
  private long stateSince;
  private ServingFailure lastFailure;
  private ServingFailure failure;
  private long lastW = -1;
  private long backlogBytes = -1;
  private long backlogMeasuredI = -2;
  private long backlogMeasuredW = -2;
  private long retryAtMillis;
  private long backoffMillis;
  private long pendingSinceMillis;
  private long backlogObservedSinceMillis;
  private final long[] batchCounters = new long[3];

  ServingIndexService(Path servingRoot, Path historyRoot, Path legacyServing, Engine engine,
      ServingHistorySource source, Config config, Hooks hooks, LongSupplier clock) {
    this.servingRoot = Objects.requireNonNull(servingRoot, "servingRoot");
    this.historyRoot = historyRoot;
    this.legacyServing = legacyServing;
    this.engine = Objects.requireNonNull(engine, "engine");
    this.source = Objects.requireNonNull(source, "source");
    this.config = Objects.requireNonNull(config, "config");
    this.hooks = Objects.requireNonNull(hooks, "hooks");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.planner = new ServingBatchPlanner(config);
    this.budget = new ServingIndexStore.WriteBudget(config.getMaxWriteMutations(),
        config.getMaxWriteBytes());
    this.stateSince = clock.getAsLong();
    this.view = new ServingView(ServingState.OPENING, stateSince, -1, -1, null, -1, null,
        false, 0, 0, 0, null);
    this.thread = new Thread(this::run, "state-archive-serving");
    this.thread.setDaemon(true);
  }

  /**
   * The disk-budget signal (ch09 OPS-D6, ch04 §5.1 DISK_PRESSURE): while it is true the build
   * thread deletes {@code discarded-*} (never paused, §3.8 P3) and then stays DEGRADED, building
   * and rebuilding nothing; it retries with backoff once the signal clears. Set before
   * {@link #start()}; {@code null} means no disk budget (always false).
   */
  public void setDiskPressureSignal(BooleanSupplier signal) {
    this.diskPressure = signal == null ? () -> false : signal;
    LockSupport.unpark(thread);
  }

  /** Returns immediately; opening and validation run on the build thread (§3.3.1 rule 3). */
  public synchronized void start() {
    if (!started) {
      started = true;
      thread.start();
    }
  }

  /**
   * The only Common → Serving call (§3.3.1 rule 2): keeps the higher watermark and wakes the
   * build thread. No lock, no I/O, never throws; it runs inside the Common owner write lock.
   */
  public void onCommonPublished(CommonCheckpointTarget target) {
    try {
      if (target != null) {
        latestNotified.accumulateAndGet(target, (current, next) -> current == null
            || next.getLastBlock().getBlockNumber() > current.getLastBlock().getBlockNumber()
            ? next : current);
      }
      LockSupport.unpark(thread);
    } catch (Throwable ignored) {
      // Must never affect the Common commit (D5).
    }
  }

  public static ServingIndexService forHistory(Path directory, Engine engine,
      ServingHistorySource source) {
    return new ServingIndexService(directory, null, null, engine, source, new Config(),
        Hooks.NONE, System::currentTimeMillis);
  }

  /** Transfers one exact-baseline index pin to a short-lived point-history reader. */
  public <T extends StateArchivePointSource & java.io.Closeable> CheckpointPointReader pinHistory(
      CommonCheckpointTarget target, StateArchiveServingSource source, T point) throws IOException {
    PersistentServingKeyIndexGeneration index = pinStrict(target.getLastBlock().getBlockNumber());
    try {
      return new StateArchiveAppendReadAdapterV3(index, source, point, target, false);
    } catch (IOException | RuntimeException failure) {
      point.close();
      throw failure;
    }
  }

  PersistentServingKeyIndexGeneration pinStrict(long baseline) throws IOException {
    ServingLease lease = acquireLease();
    if (lease.indexedThrough() != baseline) {
      lease.close();
      throw new HistoricalQueryException(HistoricalQueryException.Reason.UNAVAILABLE,
          "Serving coverage has not reached the exact Common baseline");
    }
    return lease.strictGeneration();
  }

  public ServingView status() {
    return view;
  }

  // ---------------------------------------------------------------- query side (§3.10.1)

  /**
   * Acquires a lease: count first, then read the view (linearizable with the drain protocol).
   *
   * @throws HistoricalQueryException UNAVAILABLE with detail {@code SERVING_<state>} when serving
   *     is not LIVE
   */
  public ServingLease acquireLease() throws IOException {
    leases.incrementAndGet();
    boolean acquired = false;
    ArchiveIndexStorage.Reader snapshot = null;
    try {
      hooks.afterLeaseCounted();
      ServingView current = view;
      if (current.getState() != ServingState.LIVE || current.store() == null) {
        throw notReady(current);
      }
      hooks.afterLeaseViewRead();
      ServingIndexStore handle = current.store();
      snapshot = handle.snapshot();
      ServingLease lease = new ServingLease(this, handle, current.getHandleSeq(), snapshot,
          ServingIndexStore.readDescriptor(snapshot));
      acquired = true;
      return lease;
    } finally {
      if (!acquired) {
        if (snapshot != null) {
          try {
            snapshot.close();
          } catch (IOException | RuntimeException ignored) {
            // The count is returned below regardless.
          }
        }
        releaseLease();
      }
    }
  }

  static HistoricalQueryException notReady(ServingView current) {
    return new HistoricalQueryException(HistoricalQueryException.Reason.UNAVAILABLE,
        "Historical serving index is " + current.getState());
  }

  void releaseLease() {
    if (leases.decrementAndGet() < 0) {
      leases.incrementAndGet();
      throw new IllegalStateException("Serving lease released twice");
    }
  }

  void leaseRefreshed() {
    ServingIndexTiming.leaseRefreshed();
  }

  int activeLeases() {
    return leases.get();
  }

  /**
   * Bounded wait (query thread only, §3.6) until the published {@code I} reaches {@code block}.
   * Holds no lock any import or build thread needs; returns false on timeout or when serving is
   * not LIVE.
   */
  public boolean awaitIndexedThrough(long block, Duration timeout) {
    long deadline = System.nanoTime() + StrictMathWrapper.max(0L, timeout.toNanos());
    synchronized (viewMonitor) {
      while (true) {
        ServingView current = view;
        if (current.getState() == ServingState.LIVE && current.getIndexedThrough() >= block) {
          return true;
        }
        if (current.getState() != ServingState.LIVE && current.getState()
            != ServingState.OPENING || closing) {
          return false;
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return false;
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(viewMonitor, remaining);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  /** Test, tool and operator use only; never on the block-import or Common path (§3.6, T7). */
  public boolean awaitLive(Duration timeout) {
    long deadline = System.nanoTime() + StrictMathWrapper.max(0L, timeout.toNanos());
    synchronized (viewMonitor) {
      while (view.getState() != ServingState.LIVE) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || closing) {
          return false;
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(viewMonitor, remaining);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return true;
    }
  }

  /** A query found undecodable or inconsistent index data (§3.10.3): degrade serving. */
  void reportIndexCorruption(String message) {
    reportedByQuery.compareAndSet(null,
        new ServingFailure(Reason.INDEX_CORRUPT, message, clock.getAsLong()));
    LockSupport.unpark(thread);
  }

  /** Operator request (§3.8): rebuild in place at the next opportunity. */
  public void requestRebuild() {
    rebuildRequested = true;
    LockSupport.unpark(thread);
  }

  /** Batches by kind (normal, split, solo), for tests and metrics. */
  long batches(int kind) {
    return batchCounters[kind];
  }

  // ---------------------------------------------------------------- build thread

  private void run() {
    publish();
    while (!closing) {
      try {
        if (state == ServingState.DEGRADED) {
          if (!handleDegraded()) {
            observeHistoryWhileDegraded();
            park(retryAtMillis - clock.getAsLong());
          }
          continue;
        }
        if (underDiskPressure()) {
          enterDiskPressure();
          continue;
        }
        if (store == null) {
          open();
          continue;
        }
        ServingFailure reported = reportedByQuery.getAndSet(null);
        if (reported != null) {
          throw new ServingFaultException(reported.getReason(), reported.getMessage());
        }
        if (rebuildRequested) {
          rebuildRequested = false;
          rebuild();
          continue;
        }
        long waitMillis = step();
        if (waitMillis > 0) {
          park(waitMillis);
        }
      } catch (ClosingException stop) {
        break;
      } catch (Throwable failure) {
        degrade(classify(failure), failure);
      }
    }
    publish();
  }

  /** OPENING (§3.4.3): migrate, open and validate, then derive the initial state. */
  private void open() throws IOException {
    // Reuse the existing serving directory; no unit/layout migration is introduced.
    deleteDiscardedAsync();
    Committed history = source.committed();
    long w = history.getCommittedThrough();
    long start = history.getHistoryStartBlock();
    Path directory = servingRoot.resolve(ServingIndexStore.DIRECTORY);
    ServingIndexStore opened = ServingIndexStore.open(directory, engine, start,
        history.getBaselineParentHash(), w >= start, clock.getAsLong());
    try {
      long i = opened.indexedThrough();
      if (i > w) {
        throw new ServingFaultException(Reason.INDEX_AHEAD,
            "Serving I=" + i + " is ahead of History W=" + w);
      }
      if (!Arrays.equals(source.blockHash(history, i), opened.headHash())) {
        throw new ServingFaultException(Reason.IDENTITY,
            "Serving hash(I) differs from History at " + i);
      }
    } catch (IOException | RuntimeException failure) {
      try {
        opened.close();
      } catch (IOException | RuntimeException closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
    store = opened;
    handleSeq++;
    lastW = w;
    backoffMillis = 0;
    if (store.isRebuildInProgress()) {
      if (store.indexedThrough() == w) {
        store.clearRebuildFlag();
        // Rebuild completion remains local to the derived index.
        transition(ServingState.LIVE, w);
      } else {
        transition(ServingState.REBUILDING, w);
      }
    } else {
      transition(store.indexedThrough() == w ? ServingState.LIVE : ServingState.BULK_CATCH_UP,
          w);
    }
  }

  /**
   * One build round. Returns how long to wait before the next round (0 = run again now).
   */
  private long step() throws IOException {
    Committed history = source.committed();
    long w = history.getCommittedThrough();
    if (w < lastW) {
      throw new ServingFaultException(Reason.SOURCE_INCONSISTENT,
          "History W decreased from " + lastW + " to " + w);
    }
    lastW = w;
    long i = store.indexedThrough();
    if (i > w) {
      throw new ServingFaultException(Reason.INDEX_AHEAD, "Serving I is ahead of History W");
    }
    long now = clock.getAsLong();
    if (i == w) {
      pendingSinceMillis = 0;
      backlogObservedSinceMillis = 0;
      if (state == ServingState.REBUILDING) {
        store.clearRebuildFlag();
        // Rebuild completion remains local to the derived index.
      }
      if (state != ServingState.LIVE) {
        transition(ServingState.LIVE, w);
      } else {
        publish();
      }
      return config.getPollMillis();
    }
    if (backlogObservedSinceMillis == 0) {
      backlogObservedSinceMillis = now;
    }
    if (state == ServingState.LIVE && w - i > config.getLiveMaxLagBlocks()) {
      transition(ServingState.BULK_CATCH_UP, w);
    }
    if ((state == ServingState.BULK_CATCH_UP || state == ServingState.REBUILDING)
        && config.getTailDelayMillis() > 0 && w - i < config.getBulkMinBlocks()) {
      if (pendingSinceMillis == 0) {
        pendingSinceMillis = now;
      }
      long remaining = pendingSinceMillis + config.getTailDelayMillis() - now;
      if (remaining > 0) {
        publish();
        return remaining;
      }
    }
    ServingBatchPlanner.Selection selection = planner.select(source, history, i, w);
    if (selection.isSolo()) {
      buildSolo(history, selection.getFirst(), null, w);
    } else {
      buildRange(history, selection.getFirst(), selection.getLast(), w);
    }
    if (store.indexedThrough() == w) {
      pendingSinceMillis = 0;
      backlogObservedSinceMillis = 0;
      if (state == ServingState.REBUILDING) {
        // The flag was cleared in the same write batch (§3.8 P3 step 5).
        // Rebuild completion remains local to the derived index.
      }
      if (state != ServingState.LIVE) {
        transition(ServingState.LIVE, w);
        return 0;
      }
    }
    publish();
    return 0;
  }

  private void buildRange(Committed history, long first, long last, long w)
      throws IOException {
    hooks.beforeBatch(first, last, false);
    List<CommittedBlock> blocks = source.read(history, first, last);
    int k = planner.planPrefix(blocks);
    if (k == 0) {
      buildSolo(history, first, blocks.get(0), w);
      return;
    }
    boolean split = k < blocks.size();
    while (true) {
      requireOpen();
      List<BlockReverseDiff> diffs = new ArrayList<>(k);
      List<byte[]> steps = new ArrayList<>(k);
      for (int index = 0; index < k; index++) {
        diffs.add(blocks.get(index).getDiff());
        steps.add(blocks.get(index).getStepDigest());
      }
      ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.planCommittedDiffs(
          store.indexedThrough(), store.headHash(), diffs, steps);
      boolean reachesW = plan.getIndexedThrough() == w;
      long started = System.nanoTime();
      boolean committed;
      try (ServingIndexTiming timing = new ServingIndexTiming(state.name(),
          plan.getIndexedFrom(), plan.getIndexedThrough())) {
        committed = store.commit(plan, budget, reachesW);
        if (committed) {
          timing.succeeded();
        }
      }
      if (committed) {
        hooks.afterCommit(plan.getIndexedFrom() + 1, plan.getIndexedThrough());
        batchCounters[split ? 1 : 0]++;
        ServingIndexTiming.batch(split ? "split" : "normal");
        logBatch(plan, k, started);
        return;
      }
      if (k == 1) {
        buildSolo(history, first, blocks.get(0), w);
        return;
      }
      k = k / 2;
      split = true;
    }
  }

  private void buildSolo(Committed history, long block, CommittedBlock preRead, long w)
      throws IOException {
    requireOpen();
    hooks.beforeBatch(block, block, true);
    CommittedBlock committed = preRead != null ? preRead
        : source.read(history, block, block).get(0);
    ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.planCommittedDiffs(
        store.indexedThrough(), store.headHash(),
        java.util.Collections.singletonList(committed.getDiff()),
        java.util.Collections.singletonList(committed.getStepDigest()));
    long started = System.nanoTime();
    int chunks;
    try (ServingIndexTiming timing = new ServingIndexTiming(state.name(),
        plan.getIndexedFrom(), plan.getIndexedThrough())) {
      chunks = store.commitSolo(plan, budget, plan.getIndexedThrough() == w,
          (index, count) -> {
            if (index > 0 && closing) {
              throw new ClosingException();
            }
            hooks.beforeSoloChunk(index, count);
          });
      timing.succeeded();
    }
    hooks.afterCommit(block, block);
    batchCounters[2]++;
    ServingIndexTiming.batch("solo");
    logger.info("Serving index solo batch: block={}, chunks={}, durationMs={}", block, chunks,
        (System.nanoTime() - started) / 1_000_000L);
  }

  private void logBatch(ServingIndexIncrementalPlan plan, int blocks, long started) {
    logger.info("Serving index batch: state={}, from={}, through={}, blocks={}, durationMs={}",
        state, plan.getIndexedFrom(), plan.getIndexedThrough(), blocks,
        (System.nanoTime() - started) / 1_000_000L);
  }

  /** DEGRADED: returns true when it acted, false when the caller should wait. */
  private boolean handleDegraded() throws IOException {
    if (failure == null) {
      transitionToReopen();
      return true;
    }
    if (failure.getKind() == ServingFailure.Kind.PERMANENT) {
      if ((rebuildRequested || config.isAutoRebuild() && failure.getReason().isRebuildable())
          && underDiskPressure()) {
        // ch04 §5.1 DISK_PRESSURE pauses rebuilds too; the request is kept for later.
        if (retryAtMillis == Long.MAX_VALUE || clock.getAsLong() >= retryAtMillis) {
          deleteDiscardedNow();
          scheduleRetry(failure);
        }
        return false;
      }
      if (config.isAutoRebuild() && failure.getReason().isRebuildable()) {
        if (clock.getAsLong() < retryAtMillis) {
          return false;
        }
        rebuild();
        return true;
      }
      if (rebuildRequested) {
        rebuildRequested = false;
        rebuild();
        return true;
      }
      if (failure.getReason() != Reason.SOURCE_CORRUPT) {
        // Waits for the operator (§3.8); no fast retry loop (T19).
        retryAtMillis = Long.MAX_VALUE;
        return false;
      }
    }
    if (clock.getAsLong() < retryAtMillis) {
      return false;
    }
    if (failure.getReason() == Reason.DISK_PRESSURE && underDiskPressure()) {
      deleteDiscardedNow();
      scheduleRetry(failure);
      return false;
    }
    if (!drainLeases()) {
      scheduleRetry(failure);
      return false;
    }
    closeStoreQuietly();
    transitionToReopen();
    return true;
  }

  private boolean underDiskPressure() {
    try {
      return diskPressure.getAsBoolean();
    } catch (RuntimeException unavailable) {
      return false;
    }
  }

  /**
   * ch04 §5.1 DISK_PRESSURE: delete leftover {@code discarded-*} first (derived data, never
   * paused, §3.8 P3), then stay DEGRADED while the budget is still at RED or above.
   */
  private void enterDiskPressure() {
    deleteDiscardedNow();
    if (!underDiskPressure()) {
      return;
    }
    degrade(new ServingFailure(Reason.DISK_PRESSURE, "disk budget at RED or above (ch09 "
        + "OPS-D6): serving build and rebuild paused", clock.getAsLong()), null);
  }

  /** R2-11: committed_through and backlog keep moving while DEGRADED (§3.12). */
  private void observeHistoryWhileDegraded() {
    try {
      long w = source.committed().getCommittedThrough();
      if (w > lastW) {
        lastW = w;
        if (backlogObservedSinceMillis == 0) {
          backlogObservedSinceMillis = clock.getAsLong();
        }
        publish();
      }
    } catch (RuntimeException ignored) {
      // Observation only.
    }
  }

  private void transitionToReopen() {
    failure = null;
    state = ServingState.OPENING;
    stateSince = clock.getAsLong();
    publish();
  }

  /** §3.8 P3: drain, discard the database, recreate it with REBUILD_IN_PROGRESS. */
  private void rebuild() throws IOException {
    transition(ServingState.REBUILDING, lastW);
    if (!drainLeases()) {
      ServingFailure timeout = new ServingFailure(Reason.TRANSIENT_IO,
          "Lease drain timed out before rebuild", clock.getAsLong());
      lastFailure = timeout;
      failure = timeout;
      state = ServingState.DEGRADED;
      stateSince = clock.getAsLong();
      scheduleRetry(timeout);
      rebuildRequested = true;
      publish();
      return;
    }
    closeStoreQuietly();
    Path directory = servingRoot.resolve(ServingIndexStore.DIRECTORY);
    if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
      Path discarded = servingRoot.resolve(DISCARDED_PREFIX + clock.getAsLong());
      Files.move(directory, discarded, StandardCopyOption.ATOMIC_MOVE);
      ArchiveFileDurability.syncDirectory(servingRoot);
      deleteDiscardedAsync();
    }
    failure = null;
    open();
  }

  /** §3.10.1 drain: the caller already published a non-LIVE view. */
  private boolean drainLeases() {
    long deadline = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(config.getLeaseDrainTimeoutMillis());
    while (leases.get() != 0) {
      if (System.nanoTime() >= deadline) {
        logger.error("Serving lease drain timed out with {} leases; handle kept open",
            leases.get());
        return false;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    return true;
  }

  private void degrade(ServingFailure classified, Throwable cause) {
    ServingFailure failure = classified;
    this.failure = failure;
    lastFailure = failure;
    state = ServingState.DEGRADED;
    stateSince = clock.getAsLong();
    scheduleRetry(failure);
    ServingIndexTiming.failure(failure);
    logger.error("State Archive serving DEGRADED: {}", failure, cause);
    publish();
  }

  private void scheduleRetry(ServingFailure failure) {
    long now = clock.getAsLong();
    if (failure.getReason() == Reason.SOURCE_CORRUPT) {
      retryAtMillis = now + config.getSourceCorruptBackoffMillis();
      return;
    }
    backoffMillis = backoffMillis == 0 ? config.getRetryInitialBackoffMillis()
        : StrictMathWrapper.min(config.getRetryMaxBackoffMillis(), backoffMillis * 2);
    retryAtMillis = now + backoffMillis;
  }

  ServingFailure classify(Throwable failure) {
    long now = clock.getAsLong();
    String message = String.valueOf(failure.getMessage());
    if (failure instanceof ServingFaultException) {
      return new ServingFailure(((ServingFaultException) failure).getReason(), message, now);
    }
    if (failure instanceof ArchivePersistenceException) {
      return new ServingFailure(Reason.INDEX_CORRUPT, message, now);
    }
    if (failure instanceof IOException) {
      return new ServingFailure(Reason.TRANSIENT_IO, message, now);
    }
    if (failure instanceof Error) {
      return new ServingFailure(Reason.JVM_ERROR, failure.toString(), now);
    }
    return new ServingFailure(Reason.PROGRAM_ERROR, failure.toString(), now);
  }

  private void transition(ServingState next, long w) {
    if (state != next) {
      logger.info("State Archive serving {} -> {} (I={}, W={})", state, next,
          store == null ? -1 : store.indexedThrough(), w);
      state = next;
      stateSince = clock.getAsLong();
    }
    if (next != ServingState.DEGRADED) {
      failure = null;
    }
    publish();
  }

  /** Publishes the build thread's state as an immutable view and wakes bounded waiters. */
  private void publish() {
    ServingIndexStore handle = store;
    ServingState published = closing ? ServingState.CLOSED : state;
    long i = handle == null ? -1 : handle.indexedThrough();
    long now = clock.getAsLong();
    long backlogAge = backlogObservedSinceMillis == 0 ? 0 : now - backlogObservedSinceMillis;
    ServingView next = new ServingView(published, stateSince,
        handle == null ? -1 : handle.indexedFrom(), i, handle == null ? null : handle.headHash(),
        lastW, lastFailure, handle != null && handle.isRebuildInProgress(), handleSeq,
        measureBacklogBytes(i), backlogAge, handle);
    synchronized (viewMonitor) {
      if (view.getState() == ServingState.CLOSED && published != ServingState.CLOSED) {
        return;
      }
      view = next;
      viewMonitor.notifyAll();
    }
    ServingIndexTiming.serving(next);
  }

  /**
   * {@code (I, W]} bundle bytes from blocks.idx, re-measured only when I or W moved and only while
   * the backlog is small; -1 otherwise (deviation from §3.12's incremental counter, see notes).
   */
  private long measureBacklogBytes(long i) {
    if (Thread.currentThread() != thread || i < 0 || lastW < i) {
      return backlogBytes;
    }
    if (i == backlogMeasuredI && lastW == backlogMeasuredW) {
      return backlogBytes;
    }
    backlogMeasuredI = i;
    backlogMeasuredW = lastW;
    if (lastW == i) {
      backlogBytes = 0;
    } else if (lastW - i > BACKLOG_BYTES_LIMIT) {
      backlogBytes = -1;
    } else {
      try {
        long sum = 0;
        for (long size : source.bundleBytes(source.committed(), i + 1, lastW)) {
          sum += size;
        }
        backlogBytes = sum;
      } catch (IOException | RuntimeException unavailable) {
        backlogBytes = -1;
      }
    }
    return backlogBytes;
  }

  private void requireOpen() throws ClosingException {
    if (closing) {
      throw new ClosingException();
    }
  }

  private void park(long millis) {
    if (closing) {
      return;
    }
    long bounded = StrictMathWrapper.max(1L, StrictMathWrapper.min(millis,
        config.getPollMillis()));
    LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(bounded));
  }

  private void closeStoreQuietly() {
    ServingIndexStore handle = store;
    store = null;
    if (handle != null) {
      try {
        handle.close();
      } catch (IOException | RuntimeException failure) {
        logger.error("Failed to close serving index handle", failure);
      }
    }
  }

  /** Synchronous variant for DISK_PRESSURE; failures are logged, never raised. */
  private void deleteDiscardedNow() {
    if (!Files.isDirectory(servingRoot)) {
      return;
    }
    List<Path> discarded = new ArrayList<>();
    try (Stream<Path> entries = Files.list(servingRoot)) {
      entries.filter(path -> path.getFileName().toString().startsWith(DISCARDED_PREFIX))
          .forEach(discarded::add);
    } catch (IOException failure) {
      logger.warn("Failed to list discarded serving directories", failure);
      return;
    }
    for (Path path : discarded) {
      try {
        deleteRecursively(path);
      } catch (IOException | RuntimeException failure) {
        logger.warn("Failed to delete discarded serving directory {}", path, failure);
      }
    }
  }

  private void deleteDiscardedAsync() {
    List<Path> discarded = new ArrayList<>();
    if (!Files.isDirectory(servingRoot)) {
      return;
    }
    try (Stream<Path> entries = Files.list(servingRoot)) {
      entries.filter(path -> path.getFileName().toString().startsWith(DISCARDED_PREFIX))
          .forEach(discarded::add);
    } catch (IOException failure) {
      logger.warn("Failed to list discarded serving directories", failure);
      return;
    }
    if (discarded.isEmpty()) {
      return;
    }
    Thread deleter = new Thread(() -> {
      for (Path path : discarded) {
        try {
          deleteRecursively(path);
        } catch (IOException failure) {
          logger.warn("Failed to delete discarded serving directory {}", path, failure);
        }
      }
    }, "state-archive-serving-discard");
    deleter.setDaemon(true);
    deleter.start();
  }

  static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      List<Path> paths = new ArrayList<>();
      walk.forEach(paths::add);
      for (int index = paths.size() - 1; index >= 0; index--) {
        Files.deleteIfExists(paths.get(index));
      }
    }
  }

  /**
   * §3.9.4: publish CLOSED, stop the build thread cooperatively (bounded), drain leases (bounded)
   * and only then close the native handle. On any timeout the handle is left to process exit.
   */
  @Override
  public void close() {
    synchronized (viewMonitor) {
      closing = true;
      ServingView current = view;
      view = new ServingView(ServingState.CLOSED, clock.getAsLong(),
          current.getIndexedFrom(), current.getIndexedThrough(), current.getHeadHash(),
          current.getCommittedThrough(), current.getLastFailure(),
          current.isRebuildInProgress(), current.getHandleSeq(), current.getBacklogBytes(),
          current.getOldestBacklogAgeMillis(), current.store());
      viewMonitor.notifyAll();
    }
    LockSupport.unpark(thread);
    if (started) {
      try {
        thread.join(config.getCloseTimeoutMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      if (thread.isAlive()) {
        logger.error("Serving build thread did not stop within {} ms; native handle left to "
            + "process exit", config.getCloseTimeoutMillis());
        return;
      }
    }
    if (!drainLeases()) {
      logger.error("Serving leases did not drain on close; native handle left to process exit");
      return;
    }
    closeStoreQuietly();
    ServingIndexTiming.serving(view);
  }

  /** Thrown inside the build thread to stop between batches or solo chunks. */
  private static final class ClosingException extends IOException {
    private ClosingException() {
      super("Serving is closing");
    }
  }

  /** Test injection points (T5, T21); production uses {@link #NONE}. */
  interface Hooks {
    Hooks NONE = new Hooks() { };

    default void beforeBatch(long first, long last, boolean solo) throws IOException {
    }

    default void beforeSoloChunk(int index, int count) throws IOException {
    }

    /** After a durable commit; throwing simulates a lost write acknowledgement. */
    default void afterCommit(long first, long last) throws IOException {
    }

    default void afterLeaseCounted() {
    }

    default void afterLeaseViewRead() {
    }
  }

  /** {@code storage.stateArchive.serving.*} (ch04 §3.13). */
  public static final class Config {
    private int maxBlocks = 1000;
    private long maxSourceBytes = 32L * 1024 * 1024;
    private long maxPlanEntries = 250_000;
    private long maxPlanInputBytes = 128L * 1024 * 1024;
    private long maxWriteMutations = 500_000;
    private long maxWriteBytes = 64L * 1024 * 1024;
    private long tailDelayMillis;
    private long bulkMinBlocks = 1000;
    private long liveMaxLagBlocks = 100;
    private long retryInitialBackoffMillis = 1000;
    private long retryMaxBackoffMillis = 300_000;
    private long sourceCorruptBackoffMillis = 600_000;
    private boolean autoRebuild;
    private long closeTimeoutMillis = 30_000;
    private long leaseDrainTimeoutMillis = 15_000;
    private long pollMillis = 1000;

    public static Config defaults() {
      return new Config();
    }

    /** Validation rules of ch04 §3.13. */
    public Config validate() {
      requirePositive(maxBlocks, "batch.maxBlocks");
      if (maxBlocks > 100_000) {
        throw new IllegalArgumentException("serving.batch.maxBlocks must not exceed 100000");
      }
      if (maxSourceBytes < 1024L * 1024) {
        throw new IllegalArgumentException("serving.batch.maxSourceBytes must be >= 1 MiB");
      }
      requirePositive(maxPlanEntries, "batch.maxPlanEntries");
      requirePositive(maxPlanInputBytes, "batch.maxPlanInputBytes");
      requirePositive(maxWriteMutations, "batch.maxWriteMutations");
      requirePositive(maxWriteBytes, "batch.maxWriteBytes");
      if (maxWriteBytes > 1024L * 1024 * 1024) {
        throw new IllegalArgumentException("serving.batch.maxWriteBytes must be <= 1 GiB");
      }
      if (tailDelayMillis < 0 || tailDelayMillis > 60_000) {
        throw new IllegalArgumentException("serving.bulk.tailDelayMillis must be in [0, 60000]");
      }
      requirePositive(bulkMinBlocks, "bulk.minBlocks");
      requirePositive(liveMaxLagBlocks, "live.maxLagBlocks");
      requirePositive(retryInitialBackoffMillis, "retry.initialBackoffMillis");
      requirePositive(retryMaxBackoffMillis, "retry.maxBackoffMillis");
      requirePositive(sourceCorruptBackoffMillis, "retry.sourceCorruptBackoffMillis");
      requirePositive(closeTimeoutMillis, "closeTimeoutMillis");
      requirePositive(leaseDrainTimeoutMillis, "leaseDrainTimeoutMillis");
      requirePositive(pollMillis, "pollMillis");
      return this;
    }

    private static void requirePositive(long value, String name) {
      if (value <= 0) {
        throw new IllegalArgumentException("storage.stateArchive.serving." + name
            + " must be positive");
      }
    }

    public int getMaxBlocks() {
      return maxBlocks;
    }

    public Config setMaxBlocks(int value) {
      maxBlocks = value;
      return this;
    }

    public long getMaxSourceBytes() {
      return maxSourceBytes;
    }

    public Config setMaxSourceBytes(long value) {
      maxSourceBytes = value;
      return this;
    }

    public long getMaxPlanEntries() {
      return maxPlanEntries;
    }

    public Config setMaxPlanEntries(long value) {
      maxPlanEntries = value;
      return this;
    }

    public long getMaxPlanInputBytes() {
      return maxPlanInputBytes;
    }

    public Config setMaxPlanInputBytes(long value) {
      maxPlanInputBytes = value;
      return this;
    }

    public long getMaxWriteMutations() {
      return maxWriteMutations;
    }

    public Config setMaxWriteMutations(long value) {
      maxWriteMutations = value;
      return this;
    }

    public long getMaxWriteBytes() {
      return maxWriteBytes;
    }

    public Config setMaxWriteBytes(long value) {
      maxWriteBytes = value;
      return this;
    }

    public long getTailDelayMillis() {
      return tailDelayMillis;
    }

    public Config setTailDelayMillis(long value) {
      tailDelayMillis = value;
      return this;
    }

    public long getBulkMinBlocks() {
      return bulkMinBlocks;
    }

    public Config setBulkMinBlocks(long value) {
      bulkMinBlocks = value;
      return this;
    }

    public long getLiveMaxLagBlocks() {
      return liveMaxLagBlocks;
    }

    public Config setLiveMaxLagBlocks(long value) {
      liveMaxLagBlocks = value;
      return this;
    }

    public long getRetryInitialBackoffMillis() {
      return retryInitialBackoffMillis;
    }

    public Config setRetryInitialBackoffMillis(long value) {
      retryInitialBackoffMillis = value;
      return this;
    }

    public long getRetryMaxBackoffMillis() {
      return retryMaxBackoffMillis;
    }

    public Config setRetryMaxBackoffMillis(long value) {
      retryMaxBackoffMillis = value;
      return this;
    }

    public long getSourceCorruptBackoffMillis() {
      return sourceCorruptBackoffMillis;
    }

    public Config setSourceCorruptBackoffMillis(long value) {
      sourceCorruptBackoffMillis = value;
      return this;
    }

    public boolean isAutoRebuild() {
      return autoRebuild;
    }

    public Config setAutoRebuild(boolean value) {
      autoRebuild = value;
      return this;
    }

    public long getCloseTimeoutMillis() {
      return closeTimeoutMillis;
    }

    public Config setCloseTimeoutMillis(long value) {
      closeTimeoutMillis = value;
      return this;
    }

    public long getLeaseDrainTimeoutMillis() {
      return leaseDrainTimeoutMillis;
    }

    public Config setLeaseDrainTimeoutMillis(long value) {
      leaseDrainTimeoutMillis = value;
      return this;
    }

    public long getPollMillis() {
      return pollMillis;
    }

    public Config setPollMillis(long value) {
      pollMillis = value;
      return this;
    }
  }
}
