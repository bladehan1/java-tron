package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** In-process snapshot authority that advances only with the durable block-final CURRENT head. */
public final class PathStateSnapshotHead implements PathStateHead {

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
    new PathStateBaseCompaction(admitted, admittedLimits).recover();
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
    return advancePrepared(prepare(transition));
  }

  /** Switches the owned durable CURRENT and snapshot to an exact reversible ancestor. */
  public synchronized PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash)
      throws IOException {
    requireHealthy();
    PathStateRootMetadata previous = head;
    try {
      PathStateCurrentStore currentStore = new PathStateCurrentStore(manifest);
      PathStateRootMetadata target = currentStore.findAncestor(blockNumber, blockHash, limits);
      PathStateRootMetadata switched = new PathStateLayerRetirement(manifest, limits)
          .switchToAncestor(target);
      PathStateSnapshotHead restored = open(manifest, limits);
      if (!same(switched, restored.head)
          || restored.head.getBlockNumber() != blockNumber
          || !Arrays.equals(restored.head.getBlockHash(), blockHash)) {
        failed = true;
        throw new IOException("path-state rewound snapshot identity mismatch");
      }
      head = restored.head;
      snapshot = restored.snapshot;
      return head;
    } catch (IOException | RuntimeException failure) {
      failIfAuthorityMoved(previous, failure);
      throw failure;
    }
  }

  /** Compacts the exact Chainbase-flushed prefix while retaining this newer reversible head. */
  public synchronized PathStateRootMetadata flushBaseThrough(long blockNumber, byte[] blockHash)
      throws IOException {
    requireHealthy();
    PathStateRootMetadata previous = head;
    try {
      PathStateRootMetadata base = new PathStateBaseCompaction(manifest, limits)
          .compactThrough(blockNumber, blockHash);
      if (!same(previous, new PathStateCurrentStore(manifest).current())) {
        failed = true;
        throw new IOException("path-state base flush changed the owned head");
      }
      return base;
    } catch (IOException | RuntimeException failure) {
      failIfAuthorityMoved(previous, failure);
      throw failure;
    }
  }

  /** Computes an immutable candidate without opening or changing durable path-state storage. */
  public synchronized PreparedPathStateTransition prepare(PathStateBlockTransition transition)
      throws IOException {
    requireHealthy();
    PathStateBlockTransition admitted = Objects.requireNonNull(transition, "transition");
    requireChild(admitted);
    return PreparedPathStateTransition.prepare(head, snapshot, admitted);
  }

  @Override
  public synchronized byte[] preview(PathStateBlockTransition transition) throws IOException {
    return prepare(transition).getStateRoot();
  }

  /** Publishes one exact prepared child and adopts it only after CURRENT confirms durability. */
  public synchronized PathStateRootMetadata advancePrepared(
      PreparedPathStateTransition prepared) throws IOException {
    requireHealthy();
    PreparedPathStateTransition admitted = Objects.requireNonNull(prepared, "prepared");
    if (!admitted.extendsParent(head)) {
      throw new IOException("path-state prepared transition does not extend owned head");
    }
    PathStateRootMetadata previous = head;
    PathStateRoot.Snapshot candidateSnapshot = admitted.getSnapshot();
    PathStateRootMetadata committed;
    PathStateBlockTransition transition = admitted.getTransition();
    try (PathStateLayer layer = PathStateLayer.beginPrepared(manifest, previous, admitted, limits)) {
      committed = layer.commit();
    } catch (IOException | RuntimeException failure) {
      failIfAuthorityMoved(previous, failure);
      throw failure;
    }
    if (!same(committed, new PathStateCurrentStore(manifest).current())
        || committed.getBlockNumber() != transition.getBlockNumber()
        || !Arrays.equals(committed.getBlockHash(), transition.getBlockHash())
        || !Arrays.equals(committed.getParentHash(), transition.getParentHash())
        || !Arrays.equals(committed.getPayloadDigest(), transition.getPayloadDigest())
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

  @Override
  public void close() {
    // The legacy head opens native stores only inside bounded operations.
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
