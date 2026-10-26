package org.tron.core.db2.archive.checkpoint;

import java.io.IOException;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.hot.StateArchiveHotBatchDescriptor;
import org.tron.core.db2.checkpoint.common.CommonCheckpointCapture;
import org.tron.core.db2.checkpoint.common.CommonCheckpointMaterializer;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/** Computes the transient Archive binding embedded in a Common checkpoint v2 payload. */
public interface StateArchiveCheckpointPlanner extends CommonCheckpointMaterializer {

  StateArchiveHotBatchDescriptor planCheckpoint(List<BlockReverseDiff> diffs)
      throws IOException;

  CommonCheckpointTarget prepare(CommonCheckpointCapture capture) throws IOException;
}
