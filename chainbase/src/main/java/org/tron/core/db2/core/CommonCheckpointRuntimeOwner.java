package org.tron.core.db2.core;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Explicit lifecycle and read gate for the next-format common-checkpoint runtime. */
public final class CommonCheckpointRuntimeOwner implements AutoCloseable {

  private final CommonCheckpointRedoCoordinator coordinator;
  private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
  private volatile State state = State.NEW;

  public CommonCheckpointRuntimeOwner(CommonCheckpointRedoCoordinator coordinator) {
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
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
        CommonCheckpointRedoCoordinator.RecoveryAction action = coordinator.apply(
            Objects.requireNonNull(payload, "payload"));
        Objects.requireNonNull(completion, "completion").run();
        state = State.READY;
        return action;
      } catch (IOException | RuntimeException failure) {
        state = State.FAILED;
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
    gate.readLock().lock();
    try {
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

  @Override
  public void close() {
    gate.writeLock().lock();
    try {
      state = State.CLOSED;
    } finally {
      gate.writeLock().unlock();
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
