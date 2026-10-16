package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Collects one drift-checked, read-only snapshot of archive startup authorities. */
public final class ArchiveAuthoritySnapshotCollector {

  private final HistorySource history;
  private final ProgressSource progress;
  private final ServingSource serving;
  private final LatestSource latest;

  public ArchiveAuthoritySnapshotCollector(HistorySource history, ProgressSource progress,
      ServingSource serving, LatestSource latest) {
    this.history = Objects.requireNonNull(history, "history");
    this.progress = Objects.requireNonNull(progress, "progress");
    this.serving = Objects.requireNonNull(serving, "serving");
    this.latest = Objects.requireNonNull(latest, "latest");
  }

  /**
   * Reads mutable boundary authorities twice and rejects the complete result if any changed.
   * Source implementations must be read-only; this class never opens or repairs storage.
   */
  public ArchiveAuthoritySourceBundle collect() throws IOException {
    HistoryCoverage coverageBefore = required(history.coverage(), "history coverage");
    HistoryCommitMarker first = required(history.first(), "first history marker");
    HistoryCommitMarker headBefore = required(history.head(), "history head");
    ArchiveProgressEnvelope readerBefore = required(progress.readerVisible(), "reader visible");
    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot servingBefore = required(
        serving.current(), "serving generation");
    byte[] latestBefore = required(latest.sourceIdentityDigest(),
        "latest source identity digest");

    boolean planBefore = progress.mutationPlanPresent();
    ArchiveProgressEnvelope checkpoint = required(progress.applyCheckpoint(),
        "apply checkpoint");
    Map<String, ArchiveProgressEnvelope> participantProgress = exactParticipantProgress(
        progress.participantProgress());

    boolean planAfter = progress.mutationPlanPresent();
    byte[] latestAfter = required(latest.sourceIdentityDigest(),
        "latest source identity digest");
    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot servingAfter = required(
        serving.current(), "serving generation");
    ArchiveProgressEnvelope readerAfter = required(progress.readerVisible(), "reader visible");
    HistoryCommitMarker headAfter = required(history.head(), "history head");
    HistoryCoverage coverageAfter = required(history.coverage(), "history coverage");

    if (planBefore != planAfter
        || !sameCoverage(coverageBefore, coverageAfter)
        || !sameMarker(headBefore, headAfter)
        || !sameProgress(readerBefore, readerAfter)
        || !sameServing(servingBefore, servingAfter)
        || !Arrays.equals(latestBefore, latestAfter)) {
      throw new ArchivePersistenceException(
          "Archive authority changed while collecting startup snapshot");
    }
    return new ArchiveAuthoritySourceBundle(planBefore, coverageBefore, first, headBefore,
        checkpoint, participantProgress, readerBefore, servingBefore, latestBefore);
  }

  private static boolean sameCoverage(HistoryCoverage left, HistoryCoverage right) {
    return left.getFirstEpoch() == right.getFirstEpoch()
        && left.getRecordCount() == right.getRecordCount()
        && left.getHeadEpoch() == right.getHeadEpoch()
        && Arrays.equals(left.getHeadHash(), right.getHeadHash());
  }

  private static Map<String, ArchiveProgressEnvelope> exactParticipantProgress(
      Map<String, ArchiveProgressEnvelope> actual) {
    required(actual, "participant progress");
    List<String> participants = ArchiveParticipantDescriptor.current().getParticipants();
    if (!actual.keySet().equals(new LinkedHashSet<>(participants))) {
      throw new ArchivePersistenceException("Participant progress set is not exact-27");
    }
    Map<String, ArchiveProgressEnvelope> copy = new LinkedHashMap<>();
    for (String participant : participants) {
      copy.put(participant, required(actual.get(participant), participant + " progress"));
    }
    return copy;
  }

  private static boolean sameMarker(HistoryCommitMarker left, HistoryCommitMarker right) {
    return left.getMeta().equals(right.getMeta())
        && left.getPreviousEpoch() == right.getPreviousEpoch()
        && sameHistoryLocation(left.getHistoryLocation(), right.getHistoryLocation())
        && sameIndexLocation(left.getIndexLocation(), right.getIndexLocation())
        && Arrays.equals(left.getBatchId(), right.getBatchId())
        && left.getDatabases().equals(right.getDatabases());
  }

  private static boolean sameHistoryLocation(HistoryLocation left, HistoryLocation right) {
    return left.getSegmentId() == right.getSegmentId()
        && left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && left.getBodyChecksum() == right.getBodyChecksum()
        && Arrays.equals(left.getBodyDigest(), right.getBodyDigest());
  }

  private static boolean sameIndexLocation(HistoryIndexLocation left,
      HistoryIndexLocation right) {
    return left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && Arrays.equals(left.getDigest(), right.getDigest());
  }

  private static boolean sameProgress(ArchiveProgressEnvelope left,
      ArchiveProgressEnvelope right) {
    return left.getKind() == right.getKind()
        && Objects.equals(left.getParticipant(), right.getParticipant())
        && left.getEpoch() == right.getEpoch()
        && Arrays.equals(left.getBlockHash(), right.getBlockHash())
        && Arrays.equals(left.getBatchId(), right.getBatchId())
        && Arrays.equals(left.getPayloadDigest(), right.getPayloadDigest())
        && Arrays.equals(left.getMutationPlanDigest(), right.getMutationPlanDigest())
        && left.getParticipants().equals(right.getParticipants())
        && left.getScopeIdentity().equals(right.getScopeIdentity());
  }

  private static boolean sameServing(
      ArchiveAuthoritySourceBundle.ServingGenerationSnapshot left,
      ArchiveAuthoritySourceBundle.ServingGenerationSnapshot right) {
    return Objects.equals(left.getScopeIdentity(), right.getScopeIdentity())
        && Objects.equals(left.getParticipants(), right.getParticipants())
        && left.getIndexedFromEpoch() == right.getIndexedFromEpoch()
        && left.getIndexedThroughEpoch() == right.getIndexedThroughEpoch()
        && Arrays.equals(left.getHeadHash(), right.getHeadHash())
        && Arrays.equals(left.getAuthoritativePrefixDigest(),
            right.getAuthoritativePrefixDigest())
        && Arrays.equals(left.getLatestSourceIdentityDigest(),
            right.getLatestSourceIdentityDigest());
  }

  private static <T> T required(T value, String name) {
    if (value == null) {
      throw new ArchivePersistenceException("Missing archive authority: " + name);
    }
    return value;
  }

  /** Read-only committed-history identities. */
  public interface HistorySource {
    HistoryCoverage coverage() throws IOException;

    HistoryCommitMarker first() throws IOException;

    HistoryCommitMarker head() throws IOException;
  }

  /** Read-only plan and C/D/R identities. */
  public interface ProgressSource {
    boolean mutationPlanPresent() throws IOException;

    ArchiveProgressEnvelope applyCheckpoint() throws IOException;

    Map<String, ArchiveProgressEnvelope> participantProgress() throws IOException;

    ArchiveProgressEnvelope readerVisible() throws IOException;
  }

  /** Read-only current serving generation identity. */
  public interface ServingSource {
    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot current() throws IOException;
  }

  /** Read-only identity of the pinned latest-state source set. */
  public interface LatestSource {
    byte[] sourceIdentityDigest() throws IOException;
  }
}
