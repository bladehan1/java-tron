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

  /** Admits the serving writer only after the owning runtime has completed recovery. */
  default void startServingAfterRecovery() throws IOException {
  }

  /** V5 serving handoff; other formats keep their existing lifecycle. */
  default void requestFastServingCatchUp() {
  }

  default void completePublishedServing(CommonCheckpointTarget target) throws IOException {
  }

  default void cancelServingCommitWait() {
  }

  CheckpointPointReader pinHistory(CommonCheckpointTarget target) throws IOException;
}
