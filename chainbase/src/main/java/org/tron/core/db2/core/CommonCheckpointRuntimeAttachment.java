package org.tron.core.db2.core;

import java.io.IOException;
import java.util.Objects;
import org.tron.core.db2.archive.StateArchiveCheckpointReadSnapshot;

/** Default-off lifecycle attachment that exclusively owns one next-format runtime. */
public final class CommonCheckpointRuntimeAttachment implements AutoCloseable {

  private final CommonCheckpointRuntime runtime;
  private State state;

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

  /** Pins one point-only query from a fully recovered and non-failed runtime. */
  public synchronized StateArchiveCheckpointReadSnapshot pinPoint(long targetBlock)
      throws IOException {
    requireReady();
    return runtime.pinPoint(targetBlock);
  }

  public synchronized State getState() {
    return state;
  }

  public synchronized boolean isEnabled() {
    return runtime != null;
  }

  @Override
  public synchronized void close() {
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
