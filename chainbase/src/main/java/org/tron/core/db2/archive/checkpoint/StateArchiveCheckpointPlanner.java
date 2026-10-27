package org.tron.core.db2.archive.checkpoint;

import java.io.IOException;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.contract.StateArchiveCheckpointBindingPlanner;
import org.tron.core.db2.archive.history.hot.StateArchiveHotBatchDescriptor;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointMaterializer;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;
import org.tron.core.db2.checkpoint.common.payload.CommonCheckpointCapture;

/** Computes the transient Archive binding embedded in a Common checkpoint v2 payload. */
public interface StateArchiveCheckpointPlanner
    extends CommonCheckpointMaterializer, StateArchiveCheckpointBindingPlanner {

  StateArchiveHotBatchDescriptor planCheckpoint(List<BlockReverseDiff> diffs)
      throws IOException;

  CommonCheckpointTarget prepare(CommonCheckpointCapture capture) throws IOException;
}
