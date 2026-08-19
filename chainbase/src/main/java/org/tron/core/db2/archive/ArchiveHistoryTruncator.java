package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** Shrinks commit authority before removing stale authoritative-index and body suffixes. */
public final class ArchiveHistoryTruncator {

  private final HistoryCommitStore commits;
  private final HistoryIndexStore index;
  private final HistorySegmentStore bodies;
  private final FaultHook faultHook;

  public ArchiveHistoryTruncator(HistoryCommitStore commits, HistoryIndexStore index,
      HistorySegmentStore bodies) {
    this(commits, index, bodies, stage -> { });
  }

  ArchiveHistoryTruncator(HistoryCommitStore commits, HistoryIndexStore index,
      HistorySegmentStore bodies, FaultHook faultHook) {
    this.commits = Objects.requireNonNull(commits, "commits");
    this.index = Objects.requireNonNull(index, "index");
    this.bodies = Objects.requireNonNull(bodies, "bodies");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public void truncateAfter(long lastEpoch) throws IOException {
    HistoryCommitMarker target = commits.get(lastEpoch);
    if (target == null) {
      throw new ArchivePersistenceException(
          "Archive truncation target is outside committed history: " + lastEpoch);
    }
    HistoryIndexRecord targetIndex = index.read(target.getIndexLocation());
    BlockReverseDiff targetBody = bodies.read(target.getHistoryLocation());
    if (!target.getMeta().equals(targetIndex.getMeta())
        || !target.getMeta().equals(targetBody.getMeta())
        || !sameLocation(target.getHistoryLocation(), targetIndex.getHistoryLocation())) {
      throw new ArchivePersistenceException(
          "Archive truncation target identity does not match index and body");
    }

    commits.truncateAfter(lastEpoch);
    faultHook.afterDurableStage(Stage.COMMIT_AUTHORITY);
    index.truncateAfter(target.getIndexLocation(), commits.size());
    faultHook.afterDurableStage(Stage.AUTHORITATIVE_INDEX);
    bodies.truncateAfter(target.getHistoryLocation(), commits.size());
    faultHook.afterDurableStage(Stage.HISTORY_BODY);
  }

  private static boolean sameLocation(HistoryLocation expected, HistoryLocation actual) {
    return expected.getSegmentId() == actual.getSegmentId()
        && expected.getOffset() == actual.getOffset()
        && expected.getRecordLength() == actual.getRecordLength()
        && expected.getBodyChecksum() == actual.getBodyChecksum()
        && Arrays.equals(expected.getBodyDigest(), actual.getBodyDigest());
  }

  public enum Stage {
    COMMIT_AUTHORITY,
    AUTHORITATIVE_INDEX,
    HISTORY_BODY
  }

  @FunctionalInterface
  interface FaultHook {
    void afterDurableStage(Stage stage) throws IOException;
  }
}
