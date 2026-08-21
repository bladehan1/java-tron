package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/** Tracks request-owned archive snapshots while a runtime enters quiescence. */
public final class ArchiveRuntimeQueryGate implements Closeable {

  public enum State {
    RUNNING,
    QUIESCING,
    CLOSED
  }

  private final SnapshotPinSource source;
  private State state = State.RUNNING;
  private int activeLeases;

  public ArchiveRuntimeQueryGate(ArchiveGenerationCapsule capsule) {
    this(Objects.requireNonNull(capsule, "capsule")::pin);
  }

  ArchiveRuntimeQueryGate(SnapshotPinSource source) {
    this.source = Objects.requireNonNull(source, "source");
  }

  /** Pins and registers one request atomically against the quiesce transition. */
  public synchronized Lease pin(long targetBlock) throws IOException {
    if (state != State.RUNNING) {
      throw new IllegalStateException("Archive query gate is not running: " + state);
    }
    ArchiveReadSnapshot snapshot = Objects.requireNonNull(source.pin(targetBlock), "snapshot");
    activeLeases++;
    return new Lease(this, snapshot);
  }

  /** Stops admission of new requests without waiting for existing leases. */
  public synchronized void quiesce() {
    if (state == State.RUNNING) {
      state = State.QUIESCING;
    }
  }

  public synchronized State getState() {
    return state;
  }

  public synchronized int getActiveLeaseCount() {
    return activeLeases;
  }

  public synchronized boolean isDrained() {
    return activeLeases == 0;
  }

  /** Finishes closure only after quiescence has rejected new pins and every lease has drained. */
  @Override
  public synchronized void close() {
    quiesce();
    if (state == State.CLOSED) {
      return;
    }
    if (activeLeases != 0) {
      throw new IllegalStateException(
          "Archive query gate still has active leases: " + activeLeases);
    }
    state = State.CLOSED;
  }

  private synchronized void release() {
    if (activeLeases <= 0) {
      throw new IllegalStateException("Archive query lease count underflow");
    }
    activeLeases--;
  }

  @FunctionalInterface
  interface SnapshotPinSource {
    ArchiveReadSnapshot pin(long targetBlock) throws IOException;
  }

  /** One request-owned snapshot whose close releases both resources and gate accounting. */
  public static final class Lease implements Closeable {

    private final ArchiveRuntimeQueryGate gate;
    private final ArchiveReadSnapshot snapshot;
    private boolean closed;

    private Lease(ArchiveRuntimeQueryGate gate, ArchiveReadSnapshot snapshot) {
      this.gate = gate;
      this.snapshot = snapshot;
    }

    public synchronized ArchiveReadSnapshot getSnapshot() {
      if (closed) {
        throw new IllegalStateException("Archive query lease is closed");
      }
      return snapshot;
    }

    @Override
    public void close() throws IOException {
      synchronized (this) {
        if (closed) {
          return;
        }
        closed = true;
      }
      try {
        snapshot.close();
      } finally {
        gate.release();
      }
    }
  }
}
