package org.tron.core.db2.checkpoint.common;

import java.io.IOException;
import java.util.Objects;
import org.tron.core.db2.archive.query.HistoricalQueryControl;
import org.tron.core.db2.archive.query.snapshot.StateArchiveCheckpointReadSnapshot;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;

/** Default-off lifecycle attachment that exclusively owns one next-format runtime. */
public final class CommonCheckpointRuntimeAttachment implements AutoCloseable {

  private final CommonCheckpointRuntime runtime;
  private volatile State state;
  private java.util.concurrent.ScheduledExecutorService servingMonitor;

  private CommonCheckpointRuntimeAttachment(CommonCheckpointRuntime runtime, State state) {
    this.runtime = runtime;
    this.state = state;
  }

  /**
   * Does not invoke the factory when disabled. Enabled construction returns only after startup
   * redo succeeds; a failed startup closes the newly created runtime before propagating failure.
   */
  public static CommonCheckpointRuntimeAttachment open(boolean enabled, RuntimeFactory factory)
      throws IOException {
    Objects.requireNonNull(factory, "factory");
    if (!enabled) {
      return new CommonCheckpointRuntimeAttachment(null, State.DISABLED);
    }
    CommonCheckpointRuntime runtime = Objects.requireNonNull(factory.open(),
        "common checkpoint runtime factory returned null");
    try {
      runtime.recoverBeforeServing();
      return new CommonCheckpointRuntimeAttachment(runtime, State.READY);
    } catch (IOException | RuntimeException failure) {
      try {
        runtime.close();
      } catch (RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  /** Applies one immutable flush prefix; any protocol failure permanently closes admission. */
  public synchronized CommonCheckpointTarget checkpointAndRebase(int flushCount)
      throws IOException {
    requireReady();
    try {
      return runtime.checkpointAndRebase(flushCount);
    } catch (IOException | RuntimeException failure) {
      state = State.FAILED;
      throw failure;
    }
  }

  public synchronized void appendFinalizedHistory(int flushCount) throws IOException {
    requireReady();
    try {
      runtime.appendFinalizedHistory(flushCount);
    } catch (IOException | RuntimeException failure) {
      state = State.FAILED;
      throw failure;
    }
  }

  /** Non-blocking mode signal; completion still belongs to the serialized runtime. */
  public void requestFastServingCatchUp() {
    if (state == State.READY && runtime != null) {
      runtime.requestFastServingCatchUp();
    }
  }

  /** Polls even without a next block; only this attachment serializes with Common commits. */
  public synchronized void startServingMonitor(java.util.function.BooleanSupplier nearTip) {
    requireReady();
    if (servingMonitor != null) {
      return;
    }
    servingMonitor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "archive-serving-handoff");
      thread.setDaemon(true);
      return thread;
    });
    servingMonitor.scheduleWithFixedDelay(() -> {
      synchronized (CommonCheckpointRuntimeAttachment.this) {
        if (state != State.READY) {
          return;
        }
        try {
          runtime.refreshServingMode(nearTip.getAsBoolean());
        } catch (IOException | RuntimeException failure) {
          state = State.FAILED;
          org.slf4j.LoggerFactory.getLogger("DB").error(
              "Serving handoff failed; Common admission closed", failure);
        }
      }
    }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
  }

  /** Pins one point-only query from a fully recovered and non-failed runtime. */
  public StateArchiveCheckpointReadSnapshot pinPoint(long targetBlock)
      throws IOException {
    requireReady();
    return runtime.pinPoint(targetBlock);
  }

  public StateArchiveCheckpointReadSnapshot pinPoint(long targetBlock,
      org.tron.core.db2.archive.query.HistoricalQueryControl control) throws IOException {
    requireReady();
    return runtime.pinPoint(targetBlock, control);
  }

  public synchronized State getState() {
    return state;
  }

  public synchronized boolean isEnabled() {
    return runtime != null;
  }

  @Override
  public void close() {
    if (runtime != null) {
      runtime.cancelServingCommitWait();
    }
    closeSerialized();
  }

  private synchronized void closeSerialized() {
    if (servingMonitor != null) {
      servingMonitor.shutdownNow();
    }
    if (state == State.CLOSED) {
      return;
    }
    try {
      if (runtime != null) {
        runtime.close();
      }
    } finally {
      state = State.CLOSED;
    }
  }

  private void requireReady() {
    if (state != State.READY) {
      throw new IllegalStateException("common checkpoint attachment is not ready: " + state);
    }
  }

  public enum State {
    DISABLED,
    READY,
    FAILED,
    CLOSED
  }

  @FunctionalInterface
  public interface RuntimeFactory {
    CommonCheckpointRuntime open() throws IOException;
  }
}
