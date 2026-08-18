package org.tron.core.db2.archive;

import java.util.List;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Downstream boundary for an archive writer or a bounded writer queue. */
public interface BlockReverseDiffSink {

  void accept(BlockReverseDiff diff);

  default void acceptAll(List<BlockReverseDiff> diffs) {
    diffs.forEach(this::accept);
  }

  default void revert(BlockSnapshotMeta meta) {
    // A durable writer will override this and truncate/discard its uncommitted canonical tail.
  }
}
