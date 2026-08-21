package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;

/** One writable, current-only path-state layer derived from the published canonical parent. */
public final class PathStateLayer implements Closeable {

  private final PathStateStoreManifest manifest;
  private final PathStateLayerPublication publication;
  private final PathStateNodeStoreSet stores;
  private final PathStateRoot root;
  private final PathStateRootMetadata parent;
  private final long blockNumber;
  private final byte[] blockHash;
  private final byte[] parentHash;
  private final long timestamp;
  private final P66Phase phase;
  private final byte[] transitionDigest;
  private PathStateRootMetadata prepared;
  private PathStateRootMetadata committed;

  private PathStateLayer(PathStateStoreManifest manifest, PathStateLayerPublication publication,
      PathStateNodeStoreSet stores, PathStateRoot root, PathStateRootMetadata parent,
      long blockNumber, byte[] blockHash, byte[] parentHash, long timestamp, P66Phase phase,
      byte[] transitionDigest) {
    this.manifest = manifest;
    this.publication = publication;
    this.stores = stores;
    this.root = root;
    this.parent = parent;
    this.blockNumber = blockNumber;
    this.blockHash = copy32(blockHash, "blockHash");
    this.parentHash = copy32(parentHash, "parentHash");
    this.timestamp = timestamp;
    this.phase = Objects.requireNonNull(phase, "phase");
    this.transitionDigest = copy32(transitionDigest, "transitionDigest");
  }

  /** Begins a child layer only when the supplied parent is the exact verified CURRENT record. */
  public static PathStateLayer begin(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] transitionDigest) throws IOException {
    return begin(manifest, parent, blockNumber, blockHash, parentHash, timestamp, phase,
        transitionDigest, PathStateLayerLimits.defaults());
  }

  public static PathStateLayer begin(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] transitionDigest, PathStateLayerLimits limits)
      throws IOException {
    return begin(manifest, parent, blockNumber, blockHash, parentHash, timestamp, phase,
        transitionDigest, limits, stage -> { });
  }

  static PathStateLayer begin(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] transitionDigest,
      PathStateLayerPublication.FaultHook faultHook) throws IOException {
    return begin(manifest, parent, blockNumber, blockHash, parentHash, timestamp, phase,
        transitionDigest, PathStateLayerLimits.defaults(), faultHook);
  }

  static PathStateLayer begin(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] transitionDigest, PathStateLayerLimits limits,
      PathStateLayerPublication.FaultHook faultHook) throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateLayerLimits admittedLimits = Objects.requireNonNull(limits, "limits");
    PathStateRootMetadata admittedParent = Objects.requireNonNull(parent, "parent");
    PathStateCurrentStore currentStore = new PathStateCurrentStore(admitted);
    requireSame(admittedParent, currentStore.current(),
        "path-state layer parent is not CURRENT");
    if (blockNumber != admittedParent.getBlockNumber() + 1
        || !Arrays.equals(parentHash, admittedParent.getBlockHash())) {
      throw new IOException("path-state layer identity does not extend CURRENT");
    }

    PathStateRootMetadata identity = PathStateRootMetadata.layer(blockNumber, blockHash,
        parentHash, timestamp, phase, admitted.getIdentityDigest(),
        admittedParent.getStateRoot(), admittedParent.getStateRoot(), transitionDigest);
    Path layerDirectory = admitted.getLayerDirectory(blockNumber, blockHash);
    admittedLimits.verifyCanBegin(admitted, layerDirectory);
    try (PathStateNodeStoreSet parentStores =
        PathStateNodeStoreSet.openPublished(admitted, admittedParent)) {
      PathStateRoot parentRoot = parentStores.createRoot();
      PathStateNodeStoreSet childStores = PathStateNodeStoreSet.beginLayer(admitted, identity);
      try {
        PathStateRoot childRoot = childStores.createRootFrom(parentStores.leafRecords(),
            parentRoot.rootHash());
        return new PathStateLayer(admitted,
            new PathStateLayerPublication(admitted, admittedLimits, faultHook),
            childStores, childRoot,
            admittedParent, blockNumber, blockHash, parentHash, timestamp, phase,
            transitionDigest);
      } catch (RuntimeException failure) {
        try {
          childStores.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
        throw failure;
      }
    }
  }

  public synchronized void apply(Collection<PathStateMutation> mutations) {
    requireUncommitted();
    root.apply(mutations);
  }

  /** Persists this layer's nodes/leaves/progress before publishing metadata and CURRENT. */
  public synchronized PathStateRootMetadata commit() throws IOException {
    if (committed != null) {
      return committed;
    }
    if (prepared == null) {
      prepared = PathStateRootMetadata.layer(blockNumber, blockHash, parentHash, timestamp, phase,
          manifest.getIdentityDigest(), parent.getStateRoot(), root.rootHash(), transitionDigest);
    }
    committed = publication.publish(stores, prepared);
    return committed;
  }

  public synchronized byte[] rootHash() {
    return root.rootHash();
  }

  @Override
  public synchronized void close() throws IOException {
    stores.close();
  }

  private void requireUncommitted() {
    if (prepared != null) {
      throw new IllegalStateException("path-state layer is already frozen for commit");
    }
  }

  private static void requireSame(PathStateRootMetadata expected,
      PathStateRootMetadata actual, String error) throws IOException {
    if (!Arrays.equals(expected.encode(), actual.encode())) {
      throw new IOException(error);
    }
  }

  private static byte[] copy32(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != PathStateRootMetadata.DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
    return copy;
  }
}
