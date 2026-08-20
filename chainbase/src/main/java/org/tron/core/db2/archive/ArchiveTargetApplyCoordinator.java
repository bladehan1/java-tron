package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;

/** Advances one standalone normal target through C, mixed D, latest refresh, and R. */
public final class ArchiveTargetApplyCoordinator {

  private final HistoryCommitStore history;
  private final ArchiveProgressFile checkpointFile;
  private final ArchiveTargetMutationPlanFile mutationPlanFile;
  private final Map<String, ArchiveParticipant> participantEngines;
  private final List<String> participants;
  private final ArchiveRecoveryAuthorityScanner scanner;
  private final ArchiveReaderPublicationGate publicationGate;
  private final FaultHook faultHook;

  public ArchiveTargetApplyCoordinator(HistoryCommitStore history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipant> participantEngines, Path readerVisiblePath,
      List<String> participants, ArchiveStateBarrier barrier) {
    this(history, checkpointPath, participantEngines, readerVisiblePath, participants, barrier,
        (stage, participant) -> { }, temporary -> { }, (stage, path) -> { });
  }

  ArchiveTargetApplyCoordinator(HistoryCommitStore history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipant> participantEngines, Path readerVisiblePath,
      List<String> participants, ArchiveStateBarrier barrier, FaultHook faultHook,
      ArchiveProgressFile.FaultHook publicationFaultHook) {
    this(history, checkpointPath, participantEngines, readerVisiblePath, participants, barrier,
        faultHook, publicationFaultHook, (stage, path) -> { });
  }

  ArchiveTargetApplyCoordinator(HistoryCommitStore history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipant> participantEngines, Path readerVisiblePath,
      List<String> participants, ArchiveStateBarrier barrier, FaultHook faultHook,
      ArchiveProgressFile.FaultHook publicationFaultHook,
      ArchiveTargetMutationPlanFile.FaultHook planFaultHook) {
    this.history = Objects.requireNonNull(history, "history");
    Path checkedCheckpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath");
    Path checkedReaderPath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.participants = validateParticipants(participants);
    TreeMap<String, ArchiveParticipant> sorted = new TreeMap<>(
        Objects.requireNonNull(participantEngines, "participantEngines"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive target participant engine set mismatch");
    }
    this.participantEngines = Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    this.checkpointFile = new ArchiveProgressFile(checkedCheckpointPath, codec);
    this.mutationPlanFile = new ArchiveTargetMutationPlanFile(checkedCheckpointPath,
        Objects.requireNonNull(planFaultHook, "planFaultHook"));
    this.scanner = ArchiveRecoveryAuthorityScanner.forParticipants(history,
        checkedCheckpointPath, this.participantEngines, checkedReaderPath, this.participants);
    this.publicationGate = new ArchiveReaderPublicationGate(history, checkpointFile::load,
        this.participantEngines, checkedReaderPath, this.participants,
        Objects.requireNonNull(barrier, "barrier"),
        Objects.requireNonNull(publicationFaultHook, "publicationFaultHook"));
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public void apply(long targetEpoch, Phase targetPhase,
      Map<String, ? extends List<ArchiveParticipantMutation>> mutationPlans,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    HistoryCommitMarker target = validateTarget(targetEpoch);
    Map<String, List<ArchiveParticipantMutation>> plans = validatePlans(mutationPlans);
    ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlan(
        progress(Kind.APPLY_CHECKPOINT, null, target, null),
        P66AccountAssetCodec.FORMAT_ID, Objects.requireNonNull(targetPhase, "targetPhase"), plans);
    apply(target, plan, refresh);
  }

  public void apply(ArchiveParticipantMutationBatch batch,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    ArchiveParticipantMutationBatch input = Objects.requireNonNull(batch, "batch");
    HistoryCommitMarker target = validateTarget(input.getTargetEpoch());
    apply(target, new ArchiveTargetMutationPlanBuilder().build(target, input), refresh);
  }

  private void apply(HistoryCommitMarker target, ArchiveTargetMutationPlan plan,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    long targetEpoch = target.getMeta().getEpoch();
    plan.requireIdentity(target, participants);
    requireFixedPointBeforeTarget(targetEpoch);
    byte[] mutationPlanDigest = plan.digest();
    mutationPlanFile.store(plan);
    faultHook.afterDurableStage(Stage.AFTER_PLAN, null);
    ArchiveProgressEnvelope checkpoint = progress(Kind.APPLY_CHECKPOINT, null, target,
        mutationPlanDigest);
    checkpointFile.store(checkpoint);
    faultHook.afterDurableStage(Stage.AFTER_CHECKPOINT, null);
    for (String participant : participants) {
      participantEngines.get(participant).apply(plan.getMutations(participant),
          progress(Kind.PARTICIPANT_PROGRESS, participant, target, mutationPlanDigest));
      faultHook.afterDurableStage(Stage.AFTER_PARTICIPANT, participant);
    }
    publicationGate.publishAfterRefresh(targetEpoch,
        Objects.requireNonNull(refresh, "refresh"));
    faultHook.afterDurableStage(Stage.AFTER_READER, null);
    mutationPlanFile.retire();
  }

  private HistoryCommitMarker validateTarget(long targetEpoch) {
    if (targetEpoch < 0) {
      throw new IllegalArgumentException("Archive apply target must be non-negative");
    }
    HistoryCommitMarker target = history.get(targetEpoch);
    if (target == null || target.getMeta().getEpoch() != targetEpoch
        || !target.getDatabases().equals(participants)) {
      throw new ArchivePersistenceException("Missing or mismatched archive apply target");
    }
    return target;
  }

  private Map<String, List<ArchiveParticipantMutation>> validatePlans(
      Map<String, ? extends List<ArchiveParticipantMutation>> mutationPlans) {
    TreeMap<String, ? extends List<ArchiveParticipantMutation>> sorted = new TreeMap<>(
        Objects.requireNonNull(mutationPlans, "mutationPlans"));
    if (!new ArrayList<>(sorted.keySet()).equals(participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive target mutation plan set mismatch");
    }
    Map<String, List<ArchiveParticipantMutation>> copy = new LinkedHashMap<>();
    sorted.forEach((participant, mutations) -> {
      List<ArchiveParticipantMutation> mutationCopy = new ArrayList<>(mutations);
      if (mutationCopy.contains(null)) {
        throw new IllegalArgumentException("Archive target mutation plan contains null");
      }
      copy.put(participant, Collections.unmodifiableList(mutationCopy));
    });
    return Collections.unmodifiableMap(copy);
  }

  private void requireFixedPointBeforeTarget(long targetEpoch) throws IOException {
    RecoverySnapshot current = scanner.scan();
    long checkpoint = current.getCheckpointHead();
    if (mutationPlanFile.loadIfPresent() != null) {
      throw new ArchivePersistenceException("Archive mutation plan requires recovery before apply");
    }
    if (current.getHistoryHead() < targetEpoch || checkpoint + 1 != targetEpoch
        || current.getReaderVisibleHead() != checkpoint) {
      throw new ArchivePersistenceException("Archive apply source is not a safe fixed point");
    }
    for (long participantHead : current.getParticipantHeads().values()) {
      if (participantHead != checkpoint) {
        throw new ArchivePersistenceException("Archive participant requires recovery before apply");
      }
    }
  }

  private ArchiveProgressEnvelope progress(Kind kind, String participant,
      HistoryCommitMarker marker, byte[] mutationPlanDigest) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), mutationPlanDigest, participants);
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Archive target participant set must not be empty");
    }
    String previous = null;
    for (String participant : copy) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Archive target participants must be non-empty, unique, and sorted");
      }
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  enum Stage {
    AFTER_PLAN,
    AFTER_CHECKPOINT,
    AFTER_PARTICIPANT,
    AFTER_READER
  }

  @FunctionalInterface
  interface FaultHook {
    void afterDurableStage(Stage stage, String participant) throws IOException;
  }
}
