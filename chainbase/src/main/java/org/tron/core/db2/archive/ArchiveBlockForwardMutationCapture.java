package org.tron.core.db2.archive;

import java.util.Objects;
import org.tron.core.db2.archive.BlockChangeView.PostValue;

/** One-shot owner of a block's explicit AccountAsset events, post-state view, and output batch. */
public final class ArchiveBlockForwardMutationCapture {

  private final BlockSnapshotMeta targetMeta;
  private final AccountAssetForwardMutationRecorder accountAssetRecorder;
  private BlockChangeView view;
  private State state = State.OPEN;

  public ArchiveBlockForwardMutationCapture(BlockSnapshotMeta targetMeta,
      ArchiveBlockForwardMutationLimits limits) {
    this.targetMeta = Objects.requireNonNull(targetMeta, "targetMeta");
    accountAssetRecorder = new AccountAssetForwardMutationRecorder(targetMeta,
        Objects.requireNonNull(limits, "limits"));
  }

  public synchronized void recordAccount(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, PostValue rawAccountPostValue,
      PostValue canonicalAccountPostValue) {
    requireOpen();
    accountAssetRecorder.recordAccount(eventMeta, accountPhysicalKey, rawAccountPostValue,
        canonicalAccountPostValue);
  }

  public synchronized void recordAssetPut(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, byte[] assetPhysicalKey, byte[] value) {
    requireOpen();
    accountAssetRecorder.recordAssetPut(eventMeta, accountPhysicalKey, assetPhysicalKey, value);
  }

  public synchronized void recordAssetDelete(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, byte[] assetPhysicalKey) {
    requireOpen();
    accountAssetRecorder.recordAssetDelete(eventMeta, accountPhysicalKey, assetPhysicalKey);
  }

  public synchronized void attach(BlockChangeView blockChangeView) {
    requireOpen();
    BlockChangeView attached = Objects.requireNonNull(blockChangeView, "blockChangeView");
    if (!targetMeta.equals(attached.getMeta())) {
      throw new ArchivePersistenceException("Block forward capture view meta mismatch");
    }
    if (view != null) {
      throw new ArchivePersistenceException("Block forward capture view is already attached");
    }
    accountAssetRecorder.reserveView(attached);
    view = attached;
  }

  public synchronized ArchiveParticipantMutationBatch seal(HistoryCommitMarker committedTarget) {
    requireOpen();
    if (view == null) {
      throw new ArchivePersistenceException("Block forward capture view is missing");
    }
    HistoryCommitMarker target = Objects.requireNonNull(committedTarget, "committedTarget");
    if (!targetMeta.equals(target.getMeta())) {
      throw new ArchivePersistenceException("Block forward capture marker meta mismatch");
    }
    AccountAssetForwardMutationManifest manifest = accountAssetRecorder.seal(target);
    try {
      ArchiveParticipantMutationBatch batch =
          new ArchiveParticipantMutationBatchCollector(manifest).collect(target, view);
      state = State.SEALED;
      return batch;
    } catch (RuntimeException e) {
      state = State.FAILED;
      throw e;
    } finally {
      view = null;
    }
  }

  public synchronized void abort() {
    requireOpen();
    accountAssetRecorder.discard();
    view = null;
    state = State.ABORTED;
  }

  synchronized boolean hasAttachedView() {
    return view != null;
  }

  synchronized boolean isPayloadReleased() {
    return accountAssetRecorder.isPayloadReleased();
  }

  private void requireOpen() {
    if (state != State.OPEN) {
      throw new ArchivePersistenceException(
          "Block forward capture is terminal: " + state.name());
    }
  }

  private enum State {
    OPEN,
    SEALED,
    FAILED,
    ABORTED
  }
}
