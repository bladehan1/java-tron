package org.tron.core.db2.archive.history.write;

import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;

/** Read-only committed marker/body source; implementations retain their own locking. */
public interface CommittedHistoryEvidenceSource {

  HistoryCommitMarker marker(long epoch);

  BlockReverseDiff readCommitted(long epoch);
}
