package org.tron.core.db2.stateroot.physical;

import java.io.Closeable;
import java.io.IOException;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.stateroot.PathStateLayerLimits;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;
import org.tron.core.db2.stateroot.commitment.PathStateBlockTransition;
import org.tron.core.db2.stateroot.format.PathStateRootMetadata;

/** Head-owned publication capability; native stores and fault hooks stay with their owner. */
public interface PathStatePhysicalHeadStorage extends Closeable {

  void recoverPublication() throws IOException;

  void verifyReverseJournals(PathStateLayerLimits limits) throws IOException;

  PathStateRootMetadata currentMetadata() throws IOException;

  PathStateRootMetadata previewTransition(PathStateBlockTransition transition) throws IOException;

  PreparedTransition prepareSnapshotDelta(
      BlockSnapshotMeta meta, PathStateBlockTransition transition) throws IOException;

  PathStateRootMetadata applyAndPublish(PreparedTransition prepared, PathStateLayerLimits limits)
      throws IOException;

  PathStateRootMetadata applyAndPublish(
      PathStateBlockTransition transition, PathStateLayerLimits limits) throws IOException;

  PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash, PathStateLayerLimits limits)
      throws IOException;

  /** Read-only token accepted only by the storage instance that prepared it. */
  interface PreparedTransition {

    boolean matches(PathStateBlockTransition transition);

    PathStateSnapshotDelta getSnapshotDelta();
  }
}
