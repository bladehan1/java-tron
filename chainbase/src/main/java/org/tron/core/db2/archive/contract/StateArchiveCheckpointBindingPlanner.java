package org.tron.core.db2.archive.contract;

import java.io.IOException;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.hot.StateArchiveHotBatchDescriptor;

/** Computes an Archive binding without holding the durable publication protocol. */
@FunctionalInterface
public interface StateArchiveCheckpointBindingPlanner {

  StateArchiveHotBatchDescriptor planCheckpoint(List<BlockReverseDiff> diffs)
      throws IOException;
}
