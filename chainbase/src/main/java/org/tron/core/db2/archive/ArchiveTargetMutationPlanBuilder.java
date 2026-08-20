package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.ArchiveParticipantMutationBatch.Mutation;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Builds one canonical target plan from an immutable exact physical participant batch. */
final class ArchiveTargetMutationPlanBuilder {

  private final List<String> participants;

  ArchiveTargetMutationPlanBuilder() {
    List<String> expected = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(expected);
    participants = Collections.unmodifiableList(expected);
  }

  ArchiveTargetMutationPlan build(HistoryCommitMarker committedTarget,
      ArchiveParticipantMutationBatch batch) {
    HistoryCommitMarker target = Objects.requireNonNull(committedTarget, "committedTarget");
    ArchiveParticipantMutationBatch input = Objects.requireNonNull(batch, "batch");
    requireTargetIdentity(target, input);
    if (!target.getDatabases().equals(participants)
        || !input.getParticipants().equals(participants)) {
      throw new ArchivePersistenceException(
          "Participant mutation batch does not contain the exact VERSIONED_STATE set");
    }
    Map<String, List<ArchiveParticipantMutation>> grouped = new LinkedHashMap<>();
    for (String participant : participants) {
      grouped.put(participant, new ArrayList<>());
    }
    for (Mutation mutation : input.getMutations()) {
      String dbName = mutation.getDbName();
      List<ArchiveParticipantMutation> participantMutations = grouped.get(dbName);
      if (participantMutations == null || !ArchiveStoreScope.isStateDatabase(dbName)) {
        throw new ArchivePersistenceException(
            "Unknown or derived archive participant mutation: " + dbName);
      }
      byte[] value = mutation.getValue();
      participantMutations.add(value == null
          ? ArchiveParticipantMutation.delete(mutation.getPhysicalRawKey())
          : ArchiveParticipantMutation.put(mutation.getPhysicalRawKey(), value));
    }
    ArchiveProgressEnvelope targetEnvelope = new ArchiveProgressEnvelope(
        Kind.APPLY_CHECKPOINT, null, target.getMeta().getEpoch(),
        target.getMeta().getBlockHash(), target.getBatchId(),
        target.getHistoryLocation().getBodyDigest(), participants);
    return new ArchiveTargetMutationPlan(targetEnvelope, grouped);
  }

  private void requireTargetIdentity(HistoryCommitMarker target,
      ArchiveParticipantMutationBatch batch) {
    if (target.getMeta().getEpoch() != batch.getTargetEpoch()
        || !Arrays.equals(target.getMeta().getBlockHash(), batch.getBlockHash())
        || !Arrays.equals(target.getBatchId(), batch.getBatchId())
        || !Arrays.equals(target.getHistoryLocation().getBodyDigest(),
            batch.getHistoryPayloadDigest())
        || !target.getDatabases().equals(batch.getParticipants())) {
      throw new ArchivePersistenceException(
          "Participant mutation batch target identity mismatch");
    }
  }
}
