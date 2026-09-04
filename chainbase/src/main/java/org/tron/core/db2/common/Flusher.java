package org.tron.core.db2.common;

import java.util.Map;

public interface Flusher {

  void flush(Map<WrappedByteArray, WrappedByteArray> batch);

  /** Flushes one checkpoint batch with an explicit durability barrier. */
  default void flushSynced(Map<WrappedByteArray, WrappedByteArray> batch) {
    throw new UnsupportedOperationException("Synchronous checkpoint flush is not supported");
  }

  void close();

  void reset();
}
