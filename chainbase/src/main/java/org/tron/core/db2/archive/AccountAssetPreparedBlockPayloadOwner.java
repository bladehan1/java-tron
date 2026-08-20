package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.tron.core.db2.archive.AccountAssetBlockProjectionBridge.PreparedBlockProjection;

/** Standalone ownership seam from one block layer to one immutable contiguous flush batch. */
public final class AccountAssetPreparedBlockPayloadOwner {

  private static final Object OWNERSHIP_LOCK = new Object();

  private final BlockSnapshotMeta meta;
  private PreparedBlockProjection projection;
  private State state = State.EMPTY;

  public AccountAssetPreparedBlockPayloadOwner(BlockSnapshotMeta meta) {
    this.meta = Objects.requireNonNull(meta, "meta");
  }

  public void attach(PreparedBlockProjection prepared) {
    synchronized (OWNERSHIP_LOCK) {
      if (state != State.EMPTY) {
        throw new ArchivePersistenceException("Block payload owner already left empty state");
      }
      PreparedBlockProjection candidate = Objects.requireNonNull(prepared, "prepared");
      if (!meta.equals(candidate.getMeta())) {
        throw new ArchivePersistenceException("Block payload owner target mismatch");
      }
      candidate.requirePreparedOwnership();
      projection = candidate;
      state = State.ATTACHED;
    }
  }

  public BlockReverseDiff getReverseDiff() {
    synchronized (OWNERSHIP_LOCK) {
      requireAttached();
      return projection.getReverseDiff();
    }
  }

  public void discard() {
    synchronized (OWNERSHIP_LOCK) {
      requireAttached();
      projection.abort();
      projection = null;
      state = State.DISCARDED;
    }
  }

  public boolean isAttachedTo(BlockSnapshotMeta expectedMeta) {
    synchronized (OWNERSHIP_LOCK) {
      return state == State.ATTACHED && meta.equals(expectedMeta);
    }
  }

  public static FrozenBatch freezeContiguous(
      List<AccountAssetPreparedBlockPayloadOwner> owners) {
    synchronized (OWNERSHIP_LOCK) {
      List<AccountAssetPreparedBlockPayloadOwner> candidates = new ArrayList<>(
          Objects.requireNonNull(owners, "owners"));
      if (candidates.isEmpty()) {
        throw new ArchivePersistenceException("Flush batch must contain at least one payload");
      }
      Set<AccountAssetPreparedBlockPayloadOwner> unique = new HashSet<>();
      BlockSnapshotMeta previous = null;
      for (AccountAssetPreparedBlockPayloadOwner owner : candidates) {
        AccountAssetPreparedBlockPayloadOwner candidate = Objects.requireNonNull(owner, "owner");
        if (!unique.add(candidate)) {
          throw new ArchivePersistenceException("Flush batch contains duplicate payload owner");
        }
        candidate.requireAttached();
        if (previous != null && !isNext(previous, candidate.meta)) {
          throw new ArchivePersistenceException("Flush batch payloads are not contiguous");
        }
        previous = candidate.meta;
      }

      List<PreparedBlockProjection> payloads = new ArrayList<>();
      for (AccountAssetPreparedBlockPayloadOwner owner : candidates) {
        payloads.add(owner.transfer());
      }
      return new FrozenBatch(payloads);
    }
  }

  private static boolean isNext(BlockSnapshotMeta previous, BlockSnapshotMeta current) {
    return current.getEpoch() == previous.getEpoch() + 1
        && current.getBlockNumber() == previous.getBlockNumber() + 1
        && Arrays.equals(current.getParentHash(), previous.getBlockHash());
  }

  private PreparedBlockProjection transfer() {
    requireAttached();
    PreparedBlockProjection transferred = projection;
    projection = null;
    state = State.TRANSFERRED;
    return transferred;
  }

  private void requireAttached() {
    if (state != State.ATTACHED) {
      throw new ArchivePersistenceException("Block payload owner is not attached");
    }
  }

  private enum State {
    EMPTY,
    ATTACHED,
    TRANSFERRED,
    DISCARDED
  }

  /** Immutable flush-range owner; a marker mismatch does not consume any block payload. */
  public static final class FrozenBatch {
    private final List<BlockSnapshotMeta> expectedMetas;
    private List<PreparedBlockProjection> payloads;
    private BatchState state = BatchState.FROZEN;

    private FrozenBatch(List<PreparedBlockProjection> payloads) {
      this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
      List<BlockSnapshotMeta> metas = new ArrayList<>(payloads.size());
      for (PreparedBlockProjection payload : payloads) {
        metas.add(payload.getMeta());
      }
      expectedMetas = Collections.unmodifiableList(metas);
    }

    public synchronized List<BlockSnapshotMeta> getExpectedMetas() {
      requireFrozen();
      return expectedMetas;
    }

    public synchronized boolean contains(BlockSnapshotMeta meta) {
      return expectedMetas.contains(Objects.requireNonNull(meta, "meta"));
    }

    public synchronized List<ArchiveBlockForwardPayload> seal(
        List<HistoryCommitMarker> markers) {
      requireFrozen();
      List<HistoryCommitMarker> targets = new ArrayList<>(
          Objects.requireNonNull(markers, "markers"));
      if (targets.size() != payloads.size()) {
        throw new ArchivePersistenceException("Flush batch marker count mismatch");
      }

      List<ArchiveBlockForwardPayload> sealed = new ArrayList<>();
      for (int i = 0; i < payloads.size(); i++) {
        sealed.add(payloads.get(i).previewSealPayload(targets.get(i)));
      }
      for (PreparedBlockProjection payload : payloads) {
        payload.completeSeal();
      }
      payloads = Collections.emptyList();
      state = BatchState.SEALED;
      return Collections.unmodifiableList(sealed);
    }

    public synchronized void abort() {
      requireFrozen();
      for (PreparedBlockProjection payload : payloads) {
        payload.abort();
      }
      payloads = Collections.emptyList();
      state = BatchState.ABORTED;
    }

    public synchronized void abortIfFrozen() {
      if (state == BatchState.FROZEN) {
        abort();
      }
    }

    private void requireFrozen() {
      if (state != BatchState.FROZEN) {
        throw new ArchivePersistenceException("Flush batch payload owner is terminal");
      }
    }

    private enum BatchState {
      FROZEN,
      SEALED,
      ABORTED
    }
  }
}
