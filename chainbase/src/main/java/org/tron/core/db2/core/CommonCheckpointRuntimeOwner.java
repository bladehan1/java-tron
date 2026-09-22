package org.tron.core.db2.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.tron.common.math.StrictMathWrapper;

/** Explicit lifecycle and read gate for the next-format common-checkpoint runtime. */
public final class CommonCheckpointRuntimeOwner implements AutoCloseable {

  private final CommonCheckpointRedoCoordinator coordinator;
  private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
  private volatile State state = State.NEW;

  public CommonCheckpointRuntimeOwner(CommonCheckpointRedoCoordinator coordinator) {
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
  }

  void requireMaterializer(CommonCheckpointMaterializer materializer) {
    CommonCheckpointMaterializer admitted = Objects.requireNonNull(materializer, "materializer");
    coordinator.requireMaterializer(admitted.authority(), admitted);
  }

  /** Completes any durable redo before allowing the first read lease. */
  public CommonCheckpointRedoCoordinator.RecoveryAction recoverBeforeServing()
      throws IOException {
    gate.writeLock().lock();
    try {
      requireState(State.NEW, "common checkpoint startup recovery already attempted");
      state = State.RECOVERING;
      try {
        CommonCheckpointRedoCoordinator.RecoveryAction action = coordinator.recover();
        state = State.READY;
        return action;
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
        closeAfterFailure(failure);
        throw failure;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  /** Cross-validates all published authorities before the attachment becomes externally ready. */
  void requirePublishedBeforeServing(CommonCheckpointTarget target) throws IOException {
    gate.writeLock().lock();
    try {
      requireState(State.READY, "common checkpoint startup recovery is not complete");
      try {
        coordinator.requirePublished(Objects.requireNonNull(target, "target"));
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
        closeAfterFailure(failure);
        throw failure;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  /** Blocks all read leases while the durable payload and both barriers are in progress. */
  public CommonCheckpointRedoCoordinator.RecoveryAction apply(CommonCheckpointPayload payload)
      throws IOException {
    return apply(payload, () -> { });
  }

  /** Keeps the write gate through publication and its required in-memory completion action. */
  CommonCheckpointRedoCoordinator.RecoveryAction apply(CommonCheckpointPayload payload,
      CompletionAction completion) throws IOException {
    gate.writeLock().lock();
    try {
      requireState(State.READY, "common checkpoint runtime is not ready to flush");
      state = State.CHECKPOINTING;
      try {
        CommonCheckpointRedoCoordinator.RecoveryAction action = coordinator.applyDurable(
            Objects.requireNonNull(payload, "payload"));
        Objects.requireNonNull(completion, "completion").run();
        state = State.READY;
        coordinator.notifyCommitted(CommonCheckpointTarget.from(payload));
        return action;
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
        closeAfterFailure(failure);
        throw failure;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  /**
   * Phase A of an async checkpoint on the caller thread: moves READY to CHECKPOINTING (read
   * leases keep their current behavior) and forces the redo WAL. Returns the WAL publication
   * cost in microseconds, which the caller must hand to {@link #completeAsyncCheckpoint}.
   */
  long beginAsyncCheckpoint(CommonCheckpointPayload payload) throws IOException {
    gate.writeLock().lock();
    try {
      requireState(State.READY, "common checkpoint runtime is not ready to flush");
      state = State.CHECKPOINTING;
      try {
        return coordinator.publishWal(Objects.requireNonNull(payload, "payload"));
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
        closeAfterFailure(failure);
        throw failure;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  /**
   * Phase B of an async checkpoint on the background materialize thread: redoes the durable WAL
   * under the write gate (which first waits for outstanding read leases), publishes the target,
   * then returns to READY. Fail-closed exactly like the synchronous path.
   */
  void completeAsyncCheckpoint(CommonCheckpointTarget target, long walPublishUs)
      throws IOException {
    gate.writeLock().lock();
    try {
      requireState(State.CHECKPOINTING, "common checkpoint runtime is not checkpointing");
      try {
        coordinator.redoPublished(walPublishUs);
        state = State.READY;
        coordinator.notifyCommitted(Objects.requireNonNull(target, "target"));
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
        closeAfterFailure(failure);
        throw failure;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  /** Runs one query only while no startup redo or checkpoint publication can interleave. */
  public <T> T read(ReadableOperation<T> operation) throws IOException {
    try (ReadLease ignored = acquireReadLease()) {
      return Objects.requireNonNull(operation, "operation").read();
    }
  }

  /** Acquires a request-thread-owned lease that blocks checkpoint publication until closed. */
  public ReadLease acquireReadLease() throws IOException {
    return acquireReadLease(null);
  }

  public ReadLease acquireReadLease(
      org.tron.core.db2.archive.HistoricalQueryControl control) throws IOException {
    if (control == null) {
      gate.readLock().lock();
    } else {
      try {
        while (!gate.readLock().tryLock(StrictMathWrapper.min(control.remainingNanos(),
            java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50)),
            java.util.concurrent.TimeUnit.NANOSECONDS)) {
          control.checkActive();
        }
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new org.tron.core.db2.archive.HistoricalQueryException(
            org.tron.core.db2.archive.HistoricalQueryException.Reason.CANCELLED,
            "Historical pin interrupted", failure);
      }
    }
    try {
      if (control != null) {
        control.checkActive();
      }
      requireState(State.READY, "common checkpoint runtime is not readable");
      return new ReadLease(Thread.currentThread());
    } catch (IOException | RuntimeException failure) {
      gate.readLock().unlock();
      throw failure;
    }
  }

  public State getState() {
    return state;
  }

  /** Permanently fails this owner and releases authority resources after an outer runtime error. */
  void fail(Throwable failure) {
    gate.writeLock().lock();
    try {
      if (state != State.CLOSED) {
        state = State.FAILED;
        closeAfterFailure(Objects.requireNonNull(failure, "failure"));
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    gate.writeLock().lock();
    try {
      if (state == State.CLOSED) {
        return;
      }
      try {
        coordinator.close();
      } catch (IOException failure) {
        throw new UncheckedIOException("Failed to close common checkpoint authorities", failure);
      } finally {
        state = State.CLOSED;
      }
    } finally {
      gate.writeLock().unlock();
    }
  }

  private void closeAfterFailure(Throwable failure) {
    try {
      coordinator.close();
    } catch (IOException | RuntimeException closing) {
      failure.addSuppressed(closing);
    }
  }

  private void requireState(State expected, String message) throws IOException {
    if (state != expected) {
      throw new IOException(message + ": " + state);
    }
  }

  public enum State {
    NEW,
    RECOVERING,
    READY,
    CHECKPOINTING,
    FAILED,
    CLOSED
  }

  @FunctionalInterface
  public interface ReadableOperation<T> {
    T read() throws IOException;
  }

  @FunctionalInterface
  interface CompletionAction {
    void run() throws IOException;
  }

  /** One same-thread request lease; close it only after every pinned read resource is released. */
  public final class ReadLease implements AutoCloseable {

    private final Thread ownerThread;
    private boolean closed;

    private ReadLease(Thread ownerThread) {
      this.ownerThread = ownerThread;
    }

    @Override
    public void close() {
      if (Thread.currentThread() != ownerThread) {
        throw new IllegalStateException("common checkpoint read lease changed threads");
      }
      if (!closed) {
        closed = true;
        gate.readLock().unlock();
      }
    }
  }
}
