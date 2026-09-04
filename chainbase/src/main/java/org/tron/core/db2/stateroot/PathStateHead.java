package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Runtime-owned current path-state authority used by Manager lifecycle integration. */
public interface PathStateHead extends Closeable {

  PathStateRootMetadata advance(PathStateBlockTransition transition) throws IOException;

  PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash) throws IOException;

  PathStateRootMetadata flushBaseThrough(long blockNumber, byte[] blockHash) throws IOException;

  /** Computes the child state root without publishing or adopting it. */
  byte[] preview(PathStateBlockTransition transition) throws IOException;

  /** Prepares an optional Snapshot-owned forward delta without publishing durable state. */
  default PathStateSnapshotDelta prepareSnapshotDelta(BlockSnapshotMeta meta,
      PathStateBlockTransition transition) throws IOException {
    return null;
  }

  PathStateRootMetadata getHead() throws IOException;

  @Override
  void close() throws IOException;
}
