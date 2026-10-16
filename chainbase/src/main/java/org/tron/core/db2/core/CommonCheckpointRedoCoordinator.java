package org.tron.core.db2.core;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;

/**
 * Two-barrier, idempotent redo coordinator for one durable common checkpoint.
 *
 * <p>Durable-base advances (retained target replay plus generation pruning) run on a single
 * daemon worker submitted at the redo tail. The worker re-acquires the coordinator monitor,
 * so the whole restore-then-advance sequence is serialized against apply/recover/close and
 * always lands between two redo batches, never overlapping one. Crash semantics are unchanged:
 * the retained WAL stays intact until the retained target data has been forced, and an advance
 * failure surfaces at the next batch entry exactly like a synchronous redo failure.
 */
public final class CommonCheckpointRedoCoordinator implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger("DB");
  private static final Authority[] ORDER = {
      Authority.CHAINBASE, Authority.PATH_STATE, Authority.STATE_ARCHIVE};

  private final CommonCheckpointFile checkpointFile;
  private final Map<Authority, CommonCheckpointMaterializer> materializers;
  private final FaultHook faultHook;
  private final LongSupplier nanoTime;
  private final TimingSink timingSink;
  private final ExecutorService durableBaseExecutor = Executors.newSingleThreadExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "checkpoint-durable-base");
        thread.setDaemon(true);
        return thread;
      });
  private Future<?> pendingDurableBase;
  private boolean closed;
  private int durableBaseCadence;
  private int completedSinceBase;

  public CommonCheckpointRedoCoordinator(CommonCheckpointFile checkpointFile,
      CommonCheckpointMaterializer chainbase, CommonCheckpointMaterializer pathState,
      CommonCheckpointMaterializer stateArchive) {
    this(checkpointFile, chainbase, pathState, stateArchive, stage -> { });
  }

  CommonCheckpointRedoCoordinator(CommonCheckpointFile checkpointFile,
      CommonCheckpointMaterializer chainbase, CommonCheckpointMaterializer pathState,
      CommonCheckpointMaterializer stateArchive, FaultHook faultHook) {
    this(checkpointFile, chainbase, pathState, stateArchive, faultHook, System::nanoTime,
        CommonCheckpointRedoCoordinator::logTiming);
  }

  CommonCheckpointRedoCoordinator(CommonCheckpointFile checkpointFile,
      CommonCheckpointMaterializer chainbase, CommonCheckpointMaterializer pathState,
      CommonCheckpointMaterializer stateArchive, FaultHook faultHook, LongSupplier nanoTime,
      TimingSink timingSink) {
    this.checkpointFile = Objects.requireNonNull(checkpointFile, "checkpointFile");
    this.materializers = new EnumMap<>(Authority.class);
    admit(Authority.CHAINBASE, chainbase);
    admit(Authority.PATH_STATE, pathState);
    admit(Authority.STATE_ARCHIVE, stateArchive);
    chainbase.useRetainedRedo(checkpointFile.retainsGenerations());
    pathState.useRetainedRedo(checkpointFile.retainsGenerations());
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.timingSink = Objects.requireNonNull(timingSink, "timingSink");
  }

  /** Durably publishes the redo payload before applying it to any authority. */
  public RecoveryAction apply(CommonCheckpointPayload payload) throws IOException {
    awaitPendingDurableBase();
    synchronized (this) {
      DurableResult result = applyDurableLocked(payload);
      notifyCommitted(result.getTarget());
      return result.getAction();
    }
  }

  DurableResult applyDurable(CommonCheckpointPayload payload) throws IOException {
    awaitPendingDurableBase();
    synchronized (this) {
      return applyDurableLocked(payload);
    }
  }

  private DurableResult applyDurableLocked(CommonCheckpointPayload payload) throws IOException {
    requireOpen();
    CommonCheckpointPayload admitted = Objects.requireNonNull(payload, "payload");
    long targetStart = nanoTime.getAsLong();
    CommonCheckpointTarget target = CommonCheckpointTarget.from(admitted);
    Timing timing = new Timing("apply", target, admitted.getBlocks().size());
    timing.targetBuildUs += elapsedUs(targetStart);
    timing.targetBuildCount++;
    long totalStart = nanoTime.getAsLong();
    timing.walPublishUs = timed(() -> checkpointFile.publish(admitted));
    // walLoadUs stays zero: publish() already guarantees the durable bytes are identical to the
    // admitted payload encoding, so redo consumes the admitted payload without reading it back.
    RecoveryAction action = redo(admitted, target, timing);
    timing.totalUs = elapsedUs(totalStart);
    emitTiming(timing);
    return new DurableResult(action, target);
  }

  /** Resumes the only durable checkpoint, or performs no work when none exists. */
  public RecoveryAction recover() throws IOException {
    awaitPendingDurableBase();
    synchronized (this) {
      return recoverLocked();
    }
  }

  private RecoveryAction recoverLocked() throws IOException {
    requireOpen();
    long totalStart = nanoTime.getAsLong();
    Holder<CommonCheckpointPayload> loaded = new Holder<>();
    long loadUs = timed(() -> loaded.value = checkpointFile.loadIfPresent());
    long retainedLoadUs = -1L;
    if (loaded.value == null) {
      if (checkpointFile.retainsGenerations()) {
        Holder<java.util.List<CommonCheckpointPayload>> retained = new Holder<>();
        retainedLoadUs = timed(() -> retained.value = checkpointFile.retained());
        if (!retained.value.isEmpty()) {
          loaded.value = retained.value.get(retained.value.size() - 1);
        }
      }
      if (loaded.value == null) {
        return RecoveryAction.NO_CHECKPOINT;
      }
    }
    long targetStart = nanoTime.getAsLong();
    CommonCheckpointTarget target = CommonCheckpointTarget.from(loaded.value);
    Timing timing = new Timing("recover", target, loaded.value.getBlocks().size());
    timing.targetBuildUs += elapsedUs(targetStart);
    timing.targetBuildCount++;
    timing.walLoadUs = loadUs;
    if (retainedLoadUs >= 0) {
      timing.retainedLoadUs += retainedLoadUs;
      timing.retainedLoadCount++;
    }
    if (checkpointFile.retainsGenerations()) {
      restoreRetained(false, timing);
    }
    RecoveryAction action = redo(loaded.value, target, timing);
    timing.totalUs = elapsedUs(totalStart);
    emitTiming(timing);
    notifyCommitted(target);
    return action;
  }

  synchronized void requireMaterializer(Authority authority,
      CommonCheckpointMaterializer expected) {
    if (materializers.get(Objects.requireNonNull(authority, "authority"))
        != Objects.requireNonNull(expected, "expected")) {
      throw new IllegalArgumentException(
          "common checkpoint runtime materializer identity differs: " + authority);
    }
  }

  synchronized void notifyCommitted(CommonCheckpointTarget target) {
    for (Authority authority : ORDER) {
      try {
        materializers.get(authority).afterCommit(target);
      } catch (RuntimeException failure) {
        logger.error("Derived post-commit work failed for {} at {}", authority,
            target.getLastBlock().getBlockNumber(), failure);
      }
    }
  }

  /** Requires every authority to expose the same fully published startup target. */
  synchronized void requirePublished(CommonCheckpointTarget target) throws IOException {
    requireOpen();
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    for (Authority authority : ORDER) {
      requireStatus(authority, Status.PUBLISHED,
          materializers.get(authority).inspect(admitted), "startup validation");
    }
  }

  /** Closes runtime-owned authority resources in reverse publication order. */
  @Override
  public void close() throws IOException {
    Throwable failure = null;
    try {
      // Wait for the in-flight durable-base advance before forcing the final one. Even when this
      // wait is interrupted, the synchronized section below still serializes against the worker.
      awaitPendingDurableBase();
    } catch (IOException | RuntimeException awaiting) {
      failure = awaiting;
    }
    synchronized (this) {
      if (closed) {
        return;
      }
      try {
        checkpointDurableBase(null);
      } catch (IOException | RuntimeException forcing) {
        if (failure == null) {
          failure = forcing;
        } else {
          failure.addSuppressed(forcing);
        }
      }
      closed = true;
      for (int index = ORDER.length - 1; index >= 0; index--) {
        CommonCheckpointMaterializer materializer = materializers.get(ORDER[index]);
        try {
          materializer.close();
        } catch (IOException | RuntimeException closing) {
          if (failure == null) {
            failure = closing;
          } else {
            failure.addSuppressed(closing);
          }
        }
      }
      try {
        checkpointFile.close();
      } catch (IOException closing) {
        if (failure == null) {
          failure = closing;
        } else {
          failure.addSuppressed(closing);
        }
      }
      shutdownDurableBaseExecutor();
    }
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure != null) {
      throw (RuntimeException) failure;
    }
  }

  private void shutdownDurableBaseExecutor() {
    durableBaseExecutor.shutdown();
    try {
      if (!durableBaseExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        durableBaseExecutor.shutdownNow();
        logger.warn("Common checkpoint durable-base worker did not stop within 10s");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      durableBaseExecutor.shutdownNow();
    }
  }

  private void restoreRetained(boolean sync, Timing timing) throws IOException {
    Holder<java.util.List<CommonCheckpointPayload>> held = new Holder<>();
    long loadUs = timed(() -> held.value = checkpointFile.retained());
    java.util.List<CommonCheckpointPayload> payloads = held.value;
    long chainbaseUs = timed(
        () -> materializers.get(Authority.CHAINBASE).replayRetained(payloads, sync));
    long pathStateUs = timed(
        () -> materializers.get(Authority.PATH_STATE).replayRetained(payloads, sync));
    if (timing != null) {
      timing.retainedLoadUs += loadUs;
      timing.retainedLoadCount++;
      timing.chainbaseRetainedReplayUs += chainbaseUs;
      timing.chainbaseRetainedReplayCount++;
      timing.pathStateRetainedReplayUs += pathStateUs;
      timing.pathStateRetainedReplayCount++;
    }
  }

  /** Bounds retained redo using the configured durable-base cadence. */
  public synchronized void configureDurableBaseCadence(int generations) {
    if (generations <= 0) {
      throw new IllegalArgumentException("durable base cadence must be positive");
    }
    durableBaseCadence = generations;
  }

  /** Force the retained target data before advancing the durable base and pruning generations. */
  public void checkpointDurableBase() throws IOException {
    awaitPendingDurableBase();
    synchronized (this) {
      checkpointDurableBase(null);
    }
  }

  /** Blocks until a scheduled asynchronous durable-base advance completes or fails. */
  public void awaitDurableBaseIdle() throws IOException {
    awaitPendingDurableBase();
  }

  /**
   * Hands the durable-base advance to the background worker. The cadence counter resets at
   * submission; a failed advance surfaces at the next await point instead of failing this batch.
   */
  private void scheduleDurableBase(CommonCheckpointTarget target, int blocks) {
    completedSinceBase = 0;
    pendingDurableBase = durableBaseExecutor.submit(() -> {
      runAsyncDurableBase(target, blocks);
      return null;
    });
  }

  /**
   * Runs the same durable-base sequence as the synchronous path on the background worker, with
   * its own timing record (mode=durableBase) so the offloaded cost stays visible per advance.
   */
  private void runAsyncDurableBase(CommonCheckpointTarget target, int blocks) throws IOException {
    long totalStart = nanoTime.getAsLong();
    Timing timing = new Timing("durableBase", target, blocks);
    try {
      synchronized (this) {
        if (!closed) {
          checkpointDurableBase(timing);
        }
      }
    } finally {
      timing.totalUs = elapsedUs(totalStart);
      emitTiming(timing);
    }
  }

  /**
   * Waits for the in-flight asynchronous durable-base advance, if any. The wait happens WITHOUT
   * the coordinator monitor so the worker can finish its serialized section; a failure is
   * rethrown exactly like a synchronous redo failure so the owner fails closed the same way.
   */
  private void awaitPendingDurableBase() throws IOException {
    Future<?> pending;
    synchronized (this) {
      pending = pendingDurableBase;
      pendingDurableBase = null;
    }
    if (pending == null) {
      return;
    }
    try {
      pending.get();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted awaiting common checkpoint durable base", failure);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException("Common checkpoint durable base advance failed", cause);
    }
  }

  private void checkpointDurableBase(Timing timing) throws IOException {
    requireOpen();
    if (checkpointFile.retainsGenerations() && !checkpointFile.isPresent()) {
      restoreRetained(true, timing);
      long advanceUs = timed(checkpointFile::advanceDurableBase);
      if (timing != null) {
        timing.durableBaseAdvanceUs += advanceUs;
      }
      completedSinceBase = 0;
    }
  }

  private RecoveryAction redo(CommonCheckpointPayload payload, CommonCheckpointTarget target,
      Timing timing) throws IOException {
    try (CheckpointScope ignored = new CheckpointScope(target, timing)) {
      return redoBarrier(payload, target, timing);
    }
  }

  private RecoveryAction redoBarrier(CommonCheckpointPayload payload, CommonCheckpointTarget target,
      Timing timing) throws IOException {
    Map<Authority, Status> initial = inspectAll(target, timing);
    if (initial.containsValue(Status.PUBLISHED)
        && initial.containsValue(Status.NEEDS_MATERIALIZATION)) {
      throw new IOException("common checkpoint has published authority before materialization "
          + "barrier");
    }

    for (Authority authority : ORDER) {
      CommonCheckpointMaterializer materializer = materializers.get(authority);
      if (initial.get(authority) == Status.NEEDS_MATERIALIZATION) {
        timing.materializeUs[authority.ordinal()] += timed(
            () -> materializer.materialize(payload, target));
        timing.materializeCount[authority.ordinal()]++;
        requireStatus(authority, Status.MATERIALIZED, inspect(authority, target, timing),
            "materialization");
        faultHook.after(materializeStage(authority));
      }
    }

    Map<Authority, Status> materialized = inspectAll(target, timing);
    if (materialized.containsValue(Status.NEEDS_MATERIALIZATION)) {
      throw new IOException("common checkpoint materialization barrier is incomplete");
    }
    for (Authority authority : ORDER) {
      CommonCheckpointMaterializer materializer = materializers.get(authority);
      if (materialized.get(authority) == Status.MATERIALIZED) {
        timing.publishUs[authority.ordinal()] += timed(() -> materializer.publish(target));
        timing.publishCount[authority.ordinal()]++;
        requireStatus(authority, Status.PUBLISHED, inspect(authority, target, timing),
            "publication");
        faultHook.after(publishStage(authority));
      }
    }

    Map<Authority, Status> published = inspectAll(target, timing);
    for (Authority authority : ORDER) {
      requireStatus(authority, Status.PUBLISHED, published.get(authority), "retirement");
    }
    faultHook.after(Stage.BEFORE_CHECKPOINT_RETIRE);
    timing.walRetireUs = timed(checkpointFile::retire);
    faultHook.after(Stage.AFTER_CHECKPOINT_RETIRE);
    if (durableBaseCadence > 0 && ++completedSinceBase >= durableBaseCadence) {
      scheduleDurableBase(target, timing.blocks);
    }
    return RecoveryAction.COMPLETED_REDO;
  }

  private final class CheckpointScope implements AutoCloseable {

    private final CommonCheckpointTarget target;
    private final Timing timing;
    private int opened;

    private CheckpointScope(CommonCheckpointTarget target, Timing timing) throws IOException {
      this.target = target;
      this.timing = timing;
      try {
        for (Authority authority : ORDER) {
          timing.beginUs[authority.ordinal()] += timed(
              () -> materializers.get(authority).beginCheckpoint(target));
          opened++;
        }
      } catch (IOException | RuntimeException failure) {
        try {
          close();
        } catch (IOException closing) {
          failure.addSuppressed(closing);
        }
        throw failure;
      }
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      while (opened > 0) {
        Authority authority = ORDER[--opened];
        CommonCheckpointMaterializer materializer = materializers.get(authority);
        try {
          timing.endUs[authority.ordinal()] += timed(() -> materializer.endCheckpoint(target));
        } catch (IOException closing) {
          if (failure == null) {
            failure = closing;
          } else {
            failure.addSuppressed(closing);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private Map<Authority, Status> inspectAll(CommonCheckpointTarget target, Timing timing)
      throws IOException {
    Map<Authority, Status> statuses = new EnumMap<>(Authority.class);
    for (Authority authority : ORDER) {
      Status status = inspect(authority, target, timing);
      if (status == null) {
        throw new IOException("common checkpoint " + authority + " returned null status");
      }
      statuses.put(authority, status);
    }
    return statuses;
  }

  private Status inspect(Authority authority, CommonCheckpointTarget target, Timing timing)
      throws IOException {
    Holder<Status> status = new Holder<>();
    timing.inspectUs[authority.ordinal()] += timed(
        () -> status.value = materializers.get(authority).inspect(target));
    timing.inspectCount[authority.ordinal()]++;
    return status.value;
  }

  private long timed(IoAction action) throws IOException {
    long start = nanoTime.getAsLong();
    action.run();
    return elapsedUs(start);
  }

  private long elapsedUs(long start) {
    return StrictMathWrapper.max(0L, (nanoTime.getAsLong() - start) / 1_000L);
  }

  private void emitTiming(Timing timing) {
    try {
      timingSink.accept(timing);
    } catch (RuntimeException ignored) {
      // Diagnostics must not turn an already durable checkpoint into a caller-visible failure.
    }
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw new IOException("common checkpoint coordinator is closed");
    }
  }

  private static void logTiming(Timing timing) {
    logger.info("Common checkpoint redo stages: mode={}, head={}, blocks={}, walPublishUs={}, "
            + "walLoadUs={}, chainbaseBeginUs={}, pathStateBeginUs={}, archiveBeginUs={}, "
            + "chainbaseInspectUs={}/{}, pathStateInspectUs={}/{}, archiveInspectUs={}/{}, "
            + "chainbaseMaterializeUs={}/{}, pathStateMaterializeUs={}/{}, "
            + "archiveMaterializeUs={}/{}, chainbasePublishUs={}/{}, "
            + "pathStatePublishUs={}/{}, archivePublishUs={}/{}, walRetireUs={}, "
            + "chainbaseEndUs={}, pathStateEndUs={}, archiveEndUs={}, targetBuildUs={}/{}, "
            + "retainedLoadUs={}/{}, chainbaseRetainedReplayUs={}/{}, "
            + "pathStateRetainedReplayUs={}/{}, durableBaseAdvanceUs={}, totalUs={}",
        timing.mode, timing.head, timing.blocks, timing.walPublishUs, timing.walLoadUs,
        timing.begin(Authority.CHAINBASE), timing.begin(Authority.PATH_STATE),
        timing.begin(Authority.STATE_ARCHIVE), timing.inspect(Authority.CHAINBASE),
        timing.inspectCount(Authority.CHAINBASE), timing.inspect(Authority.PATH_STATE),
        timing.inspectCount(Authority.PATH_STATE), timing.inspect(Authority.STATE_ARCHIVE),
        timing.inspectCount(Authority.STATE_ARCHIVE), timing.materialize(Authority.CHAINBASE),
        timing.materializeCount(Authority.CHAINBASE), timing.materialize(Authority.PATH_STATE),
        timing.materializeCount(Authority.PATH_STATE), timing.materialize(Authority.STATE_ARCHIVE),
        timing.materializeCount(Authority.STATE_ARCHIVE), timing.publish(Authority.CHAINBASE),
        timing.publishCount(Authority.CHAINBASE), timing.publish(Authority.PATH_STATE),
        timing.publishCount(Authority.PATH_STATE), timing.publish(Authority.STATE_ARCHIVE),
        timing.publishCount(Authority.STATE_ARCHIVE), timing.walRetireUs,
        timing.end(Authority.CHAINBASE), timing.end(Authority.PATH_STATE),
        timing.end(Authority.STATE_ARCHIVE), timing.targetBuildUs, timing.targetBuildCount,
        timing.retainedLoadUs, timing.retainedLoadCount, timing.chainbaseRetainedReplayUs,
        timing.chainbaseRetainedReplayCount, timing.pathStateRetainedReplayUs,
        timing.pathStateRetainedReplayCount, timing.durableBaseAdvanceUs, timing.totalUs);
  }

  private void admit(Authority expected, CommonCheckpointMaterializer materializer) {
    CommonCheckpointMaterializer admitted = Objects.requireNonNull(materializer,
        expected + " materializer");
    if (admitted.authority() != expected || materializers.put(expected, admitted) != null) {
      throw new IllegalArgumentException("common checkpoint materializer authority differs: "
          + expected);
    }
  }

  private static void requireStatus(Authority authority, Status expected, Status actual,
      String operation) throws IOException {
    if (actual != expected) {
      throw new IOException("common checkpoint " + authority + " " + operation
          + " returned " + actual + " instead of " + expected);
    }
  }

  static final class Timing {

    private final String mode;
    private final long head;
    private final int blocks;
    private final long[] beginUs = new long[Authority.values().length];
    private final long[] inspectUs = new long[Authority.values().length];
    private final int[] inspectCount = new int[Authority.values().length];
    private final long[] materializeUs = new long[Authority.values().length];
    private final int[] materializeCount = new int[Authority.values().length];
    private final long[] publishUs = new long[Authority.values().length];
    private final int[] publishCount = new int[Authority.values().length];
    private final long[] endUs = new long[Authority.values().length];
    private long walPublishUs;
    private long walLoadUs;
    private long walRetireUs;
    private long targetBuildUs;
    private int targetBuildCount;
    private long retainedLoadUs;
    private int retainedLoadCount;
    private long chainbaseRetainedReplayUs;
    private int chainbaseRetainedReplayCount;
    private long pathStateRetainedReplayUs;
    private int pathStateRetainedReplayCount;
    private long durableBaseAdvanceUs;
    private long totalUs;

    private Timing(String mode, CommonCheckpointTarget target, int blocks) {
      this.mode = mode;
      this.head = target.getLastBlock().getBlockNumber();
      this.blocks = blocks;
    }

    String getMode() {
      return mode;
    }

    long getHead() {
      return head;
    }

    int getBlocks() {
      return blocks;
    }

    long getWalPublishUs() {
      return walPublishUs;
    }

    long getWalLoadUs() {
      return walLoadUs;
    }

    long getWalRetireUs() {
      return walRetireUs;
    }

    long getTargetBuildUs() {
      return targetBuildUs;
    }

    int getTargetBuildCount() {
      return targetBuildCount;
    }

    long getRetainedLoadUs() {
      return retainedLoadUs;
    }

    int getRetainedLoadCount() {
      return retainedLoadCount;
    }

    long getChainbaseRetainedReplayUs() {
      return chainbaseRetainedReplayUs;
    }

    int getChainbaseRetainedReplayCount() {
      return chainbaseRetainedReplayCount;
    }

    long getPathStateRetainedReplayUs() {
      return pathStateRetainedReplayUs;
    }

    int getPathStateRetainedReplayCount() {
      return pathStateRetainedReplayCount;
    }

    long getDurableBaseAdvanceUs() {
      return durableBaseAdvanceUs;
    }

    long getTotalUs() {
      return totalUs;
    }

    long begin(Authority authority) {
      return beginUs[authority.ordinal()];
    }

    long inspect(Authority authority) {
      return inspectUs[authority.ordinal()];
    }

    int inspectCount(Authority authority) {
      return inspectCount[authority.ordinal()];
    }

    long materialize(Authority authority) {
      return materializeUs[authority.ordinal()];
    }

    int materializeCount(Authority authority) {
      return materializeCount[authority.ordinal()];
    }

    long publish(Authority authority) {
      return publishUs[authority.ordinal()];
    }

    int publishCount(Authority authority) {
      return publishCount[authority.ordinal()];
    }

    long end(Authority authority) {
      return endUs[authority.ordinal()];
    }
  }

  @FunctionalInterface
  interface TimingSink {
    void accept(Timing timing);
  }

  @FunctionalInterface
  private interface IoAction {
    void run() throws IOException;
  }

  private static final class Holder<T> {
    private T value;
  }

  private static Stage materializeStage(Authority authority) {
    switch (authority) {
      case CHAINBASE:
        return Stage.AFTER_CHAINBASE_MATERIALIZE;
      case PATH_STATE:
        return Stage.AFTER_PATH_STATE_MATERIALIZE;
      case STATE_ARCHIVE:
        return Stage.AFTER_ARCHIVE_MATERIALIZE;
      default:
        throw new IllegalArgumentException("unsupported checkpoint authority " + authority);
    }
  }

  private static Stage publishStage(Authority authority) {
    switch (authority) {
      case CHAINBASE:
        return Stage.AFTER_CHAINBASE_PUBLISH;
      case PATH_STATE:
        return Stage.AFTER_PATH_STATE_PUBLISH;
      case STATE_ARCHIVE:
        return Stage.AFTER_ARCHIVE_PUBLISH;
      default:
        throw new IllegalArgumentException("unsupported checkpoint authority " + authority);
    }
  }

  public enum RecoveryAction {
    NO_CHECKPOINT,
    COMPLETED_REDO
  }

  /** The redo outcome together with the already-built target identity for post-commit work. */
  static final class DurableResult {

    private final RecoveryAction action;
    private final CommonCheckpointTarget target;

    private DurableResult(RecoveryAction action, CommonCheckpointTarget target) {
      this.action = action;
      this.target = target;
    }

    RecoveryAction getAction() {
      return action;
    }

    CommonCheckpointTarget getTarget() {
      return target;
    }
  }

  enum Stage {
    AFTER_CHAINBASE_MATERIALIZE,
    AFTER_PATH_STATE_MATERIALIZE,
    AFTER_ARCHIVE_MATERIALIZE,
    AFTER_CHAINBASE_PUBLISH,
    AFTER_PATH_STATE_PUBLISH,
    AFTER_ARCHIVE_PUBLISH,
    BEFORE_CHECKPOINT_RETIRE,
    AFTER_CHECKPOINT_RETIRE
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage) throws IOException;
  }
}
