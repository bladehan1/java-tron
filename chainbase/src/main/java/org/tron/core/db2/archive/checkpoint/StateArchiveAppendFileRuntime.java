package org.tron.core.db2.archive.checkpoint;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.contract.CheckpointPointReader;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;

/** Runtime capabilities shared by append-file Archive format implementations. */
public interface StateArchiveAppendFileRuntime extends StateArchiveCheckpointPlanner {

  Optional<CommonCheckpointTarget> loadPublishedTargetIfPresent() throws IOException;

  void appendFinalized(List<BlockReverseDiff> diffs) throws IOException;

  void completeServingInitialSync(CommonCheckpointTarget boundary) throws IOException;

  CheckpointPointReader pinHistory(CommonCheckpointTarget target) throws IOException;
}
