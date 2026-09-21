package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Atomically publishes immutable reader generations while pinned readers drain. */
final class StateArchiveReaderGenerationHolderV5 implements Closeable {

  private final List<Slot> slots = new ArrayList<>();
  private Slot current;
  private boolean closed;
  private IOException closeFailure;

  StateArchiveReaderGenerationHolderV5(StateArchiveReaderGenerationV5 initial) {
    current = new Slot(Objects.requireNonNull(initial, "initial"));
    slots.add(current);
  }

  synchronized PinnedReader pin() throws IOException {
    if (closed) {
      throw new IOException("State Archive V5 reader generation holder is closed");
    }
    current.pins++;
    return new PinnedReader(this, current);
  }

  synchronized long getCommittedBlockNumber() throws IOException {
    if (closed) {
      throw new IOException("State Archive V5 reader generation holder is closed");
    }
    return current.generation.getCommittedBlockNumber();
  }

  void publish(StateArchiveReaderGenerationV5 replacement) throws IOException {
    StateArchiveReaderGenerationV5 admitted = Objects.requireNonNull(replacement,
        "replacement");
    List<Slot> closable;
    IOException rejection = null;
    synchronized (this) {
      if (closed) {
        rejection = new IOException("State Archive V5 reader generation holder is closed");
        closable = Collections.emptyList();
      } else if (admitted.getCommittedBlockNumber()
          <= current.generation.getCommittedBlockNumber()) {
        rejection = new IOException(
            "State Archive V5 replacement generation must advance committed block");
        closable = Collections.emptyList();
      } else {
        Slot previous = current;
        current = new Slot(admitted);
        slots.add(current);
        previous.retired = true;
        closable = claimClosableLocked();
      }
    }
    if (rejection != null) {
      try {
        admitted.close();
      } catch (IOException closeError) {
        rejection.addSuppressed(closeError);
      }
      throw rejection;
    }
    IOException failure = closeSlots(closable);
    if (failure != null) {
      throw failure;
    }
  }

  synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public void close() throws IOException {
    List<Slot> closable;
    synchronized (this) {
      closed = true;
      for (Slot slot : slots) {
        slot.retired = true;
      }
      closable = claimClosableLocked();
    }
    closeSlots(closable);

    boolean interrupted = false;
    synchronized (this) {
      while (!slots.isEmpty()) {
        try {
          wait();
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  private void release(Slot slot) throws IOException {
    List<Slot> closable;
    synchronized (this) {
      if (slot.pins <= 0) {
        throw new IllegalStateException("State Archive V5 reader generation pin underflow");
      }
      slot.pins--;
      closable = claimClosableLocked();
    }
    IOException failure = closeSlots(closable);
    if (failure != null) {
      throw failure;
    }
  }

  private List<Slot> claimClosableLocked() {
    List<Slot> result = new ArrayList<>();
    for (Slot slot : slots) {
      if (slot.retired && slot.pins == 0 && !slot.closing) {
        slot.closing = true;
        result.add(slot);
      }
    }
    return result;
  }

  private IOException closeSlots(List<Slot> closable) {
    IOException result = null;
    for (Slot slot : closable) {
      IOException failure = null;
      try {
        slot.generation.close();
      } catch (IOException closeError) {
        failure = closeError;
        if (result == null) {
          result = closeError;
        } else {
          result.addSuppressed(closeError);
        }
      } finally {
        synchronized (this) {
          slots.remove(slot);
          if (failure != null) {
            recordCloseFailureLocked(failure);
          }
          notifyAll();
        }
      }
    }
    return result;
  }

  private void recordCloseFailureLocked(IOException failure) {
    if (closeFailure == null) {
      closeFailure = failure;
    } else if (closeFailure != failure) {
      closeFailure.addSuppressed(failure);
    }
  }

  static final class PinnedReader implements StateArchivePointSource, Closeable {
    private final StateArchiveReaderGenerationHolderV5 owner;
    private final Slot slot;
    private boolean released;

    private PinnedReader(StateArchiveReaderGenerationHolderV5 owner, Slot slot) {
      this.owner = owner;
      this.slot = slot;
    }

    long getCommittedBlockNumber() {
      return slot.generation.getCommittedBlockNumber();
    }

    @Override
    public synchronized OldValue readCommittedOldValue(String dbName, byte[] rawKey,
        long blockNumber) throws IOException {
      requirePinned();
      return slot.generation.readCommittedOldValue(dbName, rawKey, blockNumber);
    }

    @Override
    public synchronized void close() throws IOException {
      if (released) {
        return;
      }
      released = true;
      owner.release(slot);
    }

    private void requirePinned() throws IOException {
      if (released) {
        throw new IOException("State Archive V5 pinned reader is closed");
      }
    }
  }

  private static final class Slot {
    private final StateArchiveReaderGenerationV5 generation;
    private int pins;
    private boolean retired;
    private boolean closing;

    private Slot(StateArchiveReaderGenerationV5 generation) {
      this.generation = generation;
    }
  }
}
