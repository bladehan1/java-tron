package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Runtime owner for a block-bound physical 27+1 CURRENT. */
public final class PathStatePhysicalSnapshotHead implements PathStateHead {

  private final PathStatePhysicalStoreSet stores;
  private final PathStateLayerLimits limits;
  private PathStateRootMetadata head;
  private boolean failed;
  private boolean closed;

  private PathStatePhysicalSnapshotHead(PathStatePhysicalStoreSet stores,
      PathStateRootMetadata head, PathStateLayerLimits limits) {
    this.stores = stores;
    this.head = head;
    this.limits = limits;
  }

  /** Opens and verifies the exact 28-database target before exposing its block identity. */
  public static PathStatePhysicalSnapshotHead open(Path directory, Engine engine)
      throws IOException {
    return open(directory, engine, PathStateLayerLimits.defaults());
  }

  /** Opens with explicit bounded reverse-journal count and logical-byte limits. */
  public static PathStatePhysicalSnapshotHead open(Path directory, Engine engine,
      PathStateLayerLimits limits) throws IOException {
    PathStatePhysicalStoreSet opened = PathStatePhysicalStoreSet.openExisting(directory,
        new PathStateCanonicalizer().participantScope(), engine);
    try {
      opened.recoverPublication();
      PathStateLayerLimits admitted = Objects.requireNonNull(limits, "limits");
      opened.verifyReverseJournals(admitted);
      return new PathStatePhysicalSnapshotHead(opened, opened.currentMetadata(), admitted);
    } catch (IOException | RuntimeException failure) {
      try {
        opened.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  @Override
  public synchronized PathStateRootMetadata advance(PathStateBlockTransition transition)
      throws IOException {
    requireHealthy();
    PathStateRootMetadata previous = head;
    try {
      PathStateRootMetadata committed = stores.applyAndPublish(transition, limits);
      if (!same(committed, stores.currentMetadata())) {
        failed = true;
        throw new IOException("physical path-state committed CURRENT identity mismatch");
      }
      head = committed;
      return PathStateRootMetadata.decode(committed.encode());
    } catch (IOException | RuntimeException failure) {
      try {
        if (!same(previous, stores.currentMetadata())) {
          this.failed = true;
        }
      } catch (IOException | RuntimeException verificationFailure) {
        this.failed = true;
        failure.addSuppressed(verificationFailure);
      }
      throw failure;
    }
  }

  @Override
  public synchronized PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash)
      throws IOException {
    requireHealthy();
    PathStateRootMetadata previous = head;
    try {
      PathStateRootMetadata rewound = stores.rewindTo(blockNumber, blockHash, limits);
      if (!same(rewound, stores.currentMetadata())) {
        failed = true;
        throw new IOException("physical path-state rewound CURRENT identity mismatch");
      }
      head = rewound;
      return PathStateRootMetadata.decode(rewound.encode());
    } catch (IOException | RuntimeException failure) {
      try {
        if (!same(previous, stores.currentMetadata())) {
          this.failed = true;
        }
      } catch (IOException | RuntimeException verificationFailure) {
        this.failed = true;
        failure.addSuppressed(verificationFailure);
      }
      throw failure;
    }
  }

  @Override
  public synchronized PathStateRootMetadata flushBaseThrough(long blockNumber, byte[] blockHash)
      throws IOException {
    requireHealthy();
    return head;
  }

  @Override
  public synchronized byte[] preview(PathStateBlockTransition transition) throws IOException {
    requireHealthy();
    return stores.previewTransition(Objects.requireNonNull(transition, "transition"))
        .getStateRoot();
  }

  @Override
  public synchronized PathStateRootMetadata getHead() throws IOException {
    requireHealthy();
    return PathStateRootMetadata.decode(head.encode());
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      stores.close();
    }
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw new IOException("physical path-state head is closed");
    }
  }

  private void requireHealthy() throws IOException {
    requireOpen();
    if (failed) {
      throw new IOException("physical path-state head failed closed");
    }
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return java.util.Arrays.equals(left.encode(), right.encode());
  }
}
