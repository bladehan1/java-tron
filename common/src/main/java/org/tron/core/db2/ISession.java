package org.tron.core.db2;

import java.util.function.Consumer;
import org.tron.core.db2.archive.BlockSnapshotMeta;

public interface ISession extends AutoCloseable {

  void commit();

  /** Commit a successfully applied block and bind its canonical identity to the snapshot. */
  default void commit(BlockSnapshotMeta meta) {
    commit();
  }

  /** Runs explicit block-final stages while the session owns its snapshot layer. */
  default void finalizeBlock(BlockSnapshotMeta meta, Consumer<BlockFinalization> pipeline) {
    throw new UnsupportedOperationException("Explicit block finalization is not supported");
  }

  interface BlockFinalization {
    void normalizeSnapshot();

    void startBlockDiff();

    void buildPathState();

    void completeArtifacts();

    void commitSession();
  }

  void revoke();

  void merge();

  void destroy();

  void close();

}
