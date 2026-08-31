package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;

/** Runtime-owned current path-state authority used by Manager lifecycle integration. */
public interface PathStateHead extends Closeable {

  PathStateRootMetadata advance(PathStateBlockTransition transition) throws IOException;

  PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash) throws IOException;

  PathStateRootMetadata flushBaseThrough(long blockNumber, byte[] blockHash) throws IOException;

  /** Computes the child state root without publishing or adopting it. */
  byte[] preview(PathStateBlockTransition transition) throws IOException;

  PathStateRootMetadata getHead() throws IOException;

  @Override
  void close() throws IOException;
}
