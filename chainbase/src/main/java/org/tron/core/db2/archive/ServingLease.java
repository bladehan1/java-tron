package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.Descriptor;

/**
 * Per-request serving lease (ch04 §3.10.1, S9): one native snapshot of the serving write handle
 * plus the progress descriptor read from that same snapshot. It fixes {@code (I_L, hash(I_L))}
 * and only requires {@code I_L <= W} (S-INV-1); it never requires {@code I == W}.
 *
 * <p>Acquisition counts first and reads the view second (see {@link ServingIndexService});
 * {@link #close()} releases the snapshot before the count, so a draining owner that observed a
 * zero count can never see a snapshot on a closed handle (S-INV-12). Single-owner object: the
 * request thread that acquired it uses and closes it.
 */
public final class ServingLease implements AutoCloseable {

  private final ServingIndexService service;
  private final ServingIndexStore store;
  private final long handleSeq;
  private StateArchiveIndexDatabase.Reader snapshot;
  private Descriptor descriptor;
  private PersistentServingKeyIndexGeneration generation;
  private boolean closed;

  ServingLease(ServingIndexService service, ServingIndexStore store, long handleSeq,
      StateArchiveIndexDatabase.Reader snapshot, Descriptor descriptor) {
    this.service = Objects.requireNonNull(service, "service");
    this.store = Objects.requireNonNull(store, "store");
    this.handleSeq = handleSeq;
    install(snapshot, descriptor);
  }

  private void install(StateArchiveIndexDatabase.Reader reader, Descriptor progress) {
    this.snapshot = reader;
    this.descriptor = progress;
    this.generation = PersistentServingKeyIndexGeneration.overSnapshot(store.getDirectory(),
        progress, store.getEngine(), reader);
  }

  /** {@code B0 - 1}. */
  public long indexedFrom() {
    ensureOpen();
    return descriptor.indexedFrom();
  }

  /** {@code I_L}, read from the same snapshot as every index lookup of this lease. */
  public long indexedThrough() {
    ensureOpen();
    return descriptor.indexedThrough();
  }

  /** {@code hash(I_L)}. */
  public byte[] headHash() {
    ensureOpen();
    return descriptor.headHash();
  }

  long handleSeq() {
    return handleSeq;
  }

  PersistentServingKeyIndexGeneration strictGeneration() {
    ensureOpen();
    return PersistentServingKeyIndexGeneration.overSnapshot(store.getDirectory(), descriptor,
        store.getEngine(), snapshot, this::close);
  }

  /**
   * First block in {@code (target, I_L]} that changed {@code (dbName, rawKey)}; the upper bound is
   * always {@code I_L}, so an interrupted solo residue at {@code I + 1} is never visible.
   * Undecodable index data is INDEX_CORRUPT: the query fails CORRUPT and serving degrades.
   */
  public OptionalLong firstChangeAfter(String dbName, byte[] rawKey, long target)
      throws IOException {
    ensureOpen();
    try {
      return generation.firstChangeAfter(dbName, rawKey, target, descriptor.indexedThrough());
    } catch (ArchivePersistenceException corrupt) {
      service.reportIndexCorruption("serving index lookup failed: " + corrupt.getMessage());
      throw new HistoricalQueryException(HistoricalQueryException.Reason.CORRUPT,
          "Historical serving index is corrupt", corrupt);
    }
  }

  /**
   * Replaces this lease's snapshot with a newer one on the same handle when the published
   * {@code I} advanced and serving is LIVE. Safe because this lease still holds a count, so no
   * drain can complete and close the handle meanwhile.
   */
  public boolean refreshIfAdvanced() throws IOException {
    ensureOpen();
    ServingView view = service.status();
    if (view.getState() != ServingState.LIVE || view.getHandleSeq() != handleSeq
        || view.store() != store || view.getIndexedThrough() <= descriptor.indexedThrough()) {
      return false;
    }
    StateArchiveIndexDatabase.Reader next = store.snapshot();
    Descriptor progress;
    try {
      progress = ServingIndexStore.readDescriptor(next);
    } catch (IOException | RuntimeException failure) {
      next.close();
      throw failure;
    }
    if (progress.indexedThrough() <= descriptor.indexedThrough()) {
      next.close();
      return false;
    }
    PersistentServingKeyIndexGeneration previous = generation;
    install(next, progress);
    previous.close();
    service.leaseRefreshed();
    return true;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      generation.close();
    } catch (IOException | RuntimeException ignored) {
      // The snapshot release cannot fail meaningfully; the count must still be returned.
    } finally {
      service.releaseLease();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Serving lease is closed");
    }
  }
}
