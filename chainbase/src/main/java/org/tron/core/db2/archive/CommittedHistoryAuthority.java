package org.tron.core.db2.archive;

/** Read-only committed State History authority shared by normal and recovery paths. */
public interface CommittedHistoryAuthority {

  HistoryCommitMarker head();

  HistoryCommitMarker get(long epoch);

  long firstEpoch();

  HistoryCoverage coverage();
}
