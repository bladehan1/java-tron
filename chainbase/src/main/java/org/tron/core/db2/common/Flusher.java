package org.tron.core.db2.common;

import java.util.Map;

public interface Flusher {

  void flush(Map<WrappedByteArray, WrappedByteArray> batch);

  /** Flushes one checkpoint batch with an explicit durability barrier. */
  default void flushSynced(Map<WrappedByteArray, WrappedByteArray> batch) {
    throw new UnsupportedOperationException("Synchronous checkpoint flush is not supported");
  }

  default void flushUnsynced(Map<WrappedByteArray, WrappedByteArray> batch) {
    throw new UnsupportedOperationException("Unsynced common flush is not supported");
  }

  /**
   * Explicit durability barrier without data: forces every write that preceded this call on this
   * store to disk (checkpoint per-batch consolidation phase 2.2).
   */
  default void flushBarrier() {
    flushSynced(java.util.Collections.emptyMap());
  }

  void close();

  void reset();
}
