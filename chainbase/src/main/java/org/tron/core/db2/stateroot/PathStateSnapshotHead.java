package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** In-process snapshot authority that advances only with the durable block-final CURRENT head. */
public final class PathStateSnapshotHead {

  private final PathStateStoreManifest manifest;
  private final PathStateLayerLimits limits;
  private PathStateRootMetadata head;
  private PathStateRoot.Snapshot snapshot;
  private boolean failed;

  private PathStateSnapshotHead(PathStateStoreManifest manifest, PathStateLayerLimits limits,
      PathStateRootMetadata head, PathStateRoot.Snapshot snapshot) {
    this.manifest = manifest;
    this.limits = limits;
    this.head = head;
    this.snapshot = snapshot;
  }

  /** Restores and verifies the exact durable CURRENT root before owning its detached snapshot. */
  public static PathStateSnapshotHead open(PathStateStoreManifest manifest,
      PathStateLayerLimits limits) throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateLayerLimits admittedLimits = Objects.requireNonNull(limits, "limits");
    PathStateRootMetadata current = new PathStateCurrentStore(admitted).current();
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openPublished(admitted, current)) {
      PathStateRoot root = stores.createRoot();
      PathStateRoot.Snapshot restored = root.snapshot();
      if (!Arrays.equals(restored.getStateRoot(), current.getStateRoot())) {
        throw new IOException("path-state snapshot head restore root mismatch");
      }
      return new PathStateSnapshotHead(admitted, admittedLimits, current, restored);
    } catch (IllegalArgumentException | IllegalStateException failure) {
      throw new IOException("path-state snapshot head restore failed", failure);
    }
  }

  /** Applies one exact child and publishes its snapshot only after durable layer commit succeeds. */
  public synchronized PathStateRootMetadata advance(PathStateBlockTransition transition)
      throws IOException {
    requireHealthy();
    PathStateBlockTransition admitted = Objects.requireNonNull(transition, "transition");
    requireChild(admitted);
    PathStateRootMetadata previous = head;
    PathStateRoot.Snapshot candidateSnapshot;
    PathStateRootMetadata committed;
    try (PathStateLayer layer = PathStateLayer.beginFromSnapshot(manifest, previous, snapshot,
        admitted.getBlockNumber(), admitted.getBlockHash(), admitted.getParentHash(),
        admitted.getTimestamp(), admitted.getPhase(), admitted.getPayloadDigest(), limits)) {
      if (!admitted.getMutations().isEmpty()) {
        layer.apply(admitted.getMutations());
      }
      candidateSnapshot = layer.prepareSnapshot();
      committed = layer.commit();
    } catch (IOException | RuntimeException failure) {
      failIfAuthorityMoved(previous, failure);
      throw failure;
    }
    if (!same(committed, new PathStateCurrentStore(manifest).current())
        || committed.getBlockNumber() != admitted.getBlockNumber()
        || !Arrays.equals(committed.getBlockHash(), admitted.getBlockHash())
        || !Arrays.equals(committed.getParentHash(), admitted.getParentHash())
        || !Arrays.equals(committed.getPayloadDigest(), admitted.getPayloadDigest())
        || !Arrays.equals(committed.getStateRoot(), candidateSnapshot.getStateRoot())) {
      failed = true;
      throw new IOException("path-state committed snapshot identity mismatch");
    }
    head = committed;
    snapshot = candidateSnapshot;
    return committed;
  }

  public synchronized PathStateRootMetadata getHead() throws IOException {
    requireHealthy();
    return head;
  }

  public synchronized PathStateRoot.Snapshot getSnapshot() throws IOException {
    requireHealthy();
    return snapshot;
  }

  public synchronized boolean isFailed() {
    return failed;
  }

  private void requireChild(PathStateBlockTransition transition) throws IOException {
    if (transition.getBlockNumber() != head.getBlockNumber() + 1
        || !Arrays.equals(transition.getParentHash(), head.getBlockHash())) {
      throw new IOException("path-state snapshot transition does not extend owned head");
    }
  }

  private void failIfAuthorityMoved(PathStateRootMetadata previous, Throwable failure) {
    try {
      if (!same(previous, new PathStateCurrentStore(manifest).current())) {
        failed = true;
      }
    } catch (IOException currentFailure) {
      failed = true;
      failure.addSuppressed(currentFailure);
    }
  }

  private void requireHealthy() throws IOException {
    if (failed) {
      throw new IOException("path-state snapshot head is failed");
    }
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return Arrays.equals(left.encode(), right.encode());
  }
}
