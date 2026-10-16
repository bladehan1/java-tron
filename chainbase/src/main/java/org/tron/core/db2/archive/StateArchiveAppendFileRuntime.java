package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Runtime capabilities shared by append-file Archive format implementations. */
public interface StateArchiveAppendFileRuntime extends StateArchiveCheckpointPlanner {

  Optional<CommonCheckpointTarget> loadPublishedTargetIfPresent() throws IOException;

  void appendFinalized(List<BlockReverseDiff> diffs) throws IOException;

  void completeServingInitialSync(CommonCheckpointTarget boundary) throws IOException;

  CheckpointPointHistory pinHistory(CommonCheckpointTarget target) throws IOException;
}
