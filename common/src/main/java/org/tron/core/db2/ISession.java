package org.tron.core.db2;

import org.tron.core.db2.archive.BlockSnapshotMeta;

public interface ISession extends AutoCloseable {

  void commit();

  /** Commit a successfully applied block and bind its canonical identity to the snapshot. */
  default void commit(BlockSnapshotMeta meta) {
    commit();
  }

  void revoke();

  void merge();

  void destroy();

  void close();

}
