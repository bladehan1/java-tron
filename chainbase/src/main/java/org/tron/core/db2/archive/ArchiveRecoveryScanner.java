package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;

/** Validates durable C and D[i] identities before exposing one recovery snapshot. */
public final class ArchiveRecoveryScanner {

  private final HistoryIdentitySource history;
  private final ProgressIdentitySource progress;
  private final List<String> participants;

  public ArchiveRecoveryScanner(HistoryIdentitySource history, ProgressIdentitySource progress,
      List<String> participants) {
    this.history = Objects.requireNonNull(history, "history");
    this.progress = Objects.requireNonNull(progress, "progress");
    this.participants = validateParticipants(participants);
  }

  public RecoverySnapshot scan() throws IOException {
    ArchiveProgressEnvelope checkpoint = progress.loadCheckpoint();
    if (checkpoint == null) {
      throw new ArchivePersistenceException("Missing archive apply checkpoint");
    }
    Map<String, ArchiveProgressEnvelope> loadedProgress = progress.loadParticipantProgress();
    if (loadedProgress == null) {
      throw new ArchivePersistenceException("Missing archive participant progress set");
    }
    SortedMap<String, ArchiveProgressEnvelope> participantProgress =
        new TreeMap<>(loadedProgress);
    if (!new ArrayList<>(participantProgress.keySet()).equals(participants)) {
      throw new ArchivePersistenceException("Archive participant progress set mismatch");
    }

    validateEnvelope(checkpoint, Kind.APPLY_CHECKPOINT, null);
    SortedMap<String, Long> participantHeads = new TreeMap<>();
    for (String participant : participants) {
      ArchiveProgressEnvelope envelope = participantProgress.get(participant);
      if (envelope == null) {
        throw new ArchivePersistenceException(
            "Missing archive participant progress: " + participant);
      }
      validateEnvelope(envelope, Kind.PARTICIPANT_PROGRESS, participant);
      if (envelope.getEpoch() == checkpoint.getEpoch()
          && !Arrays.equals(envelope.getMutationPlanDigest(),
              checkpoint.getMutationPlanDigest())) {
        throw new ArchivePersistenceException(
            "Archive participant mutation-plan digest mismatch: " + participant);
      }
      participantHeads.put(participant, envelope.getEpoch());
    }
    ArchiveProgressEnvelope readerVisible = progress.loadReaderVisible();
    if (readerVisible == null) {
      throw new ArchivePersistenceException("Missing archive reader-visible progress");
    }
    validateEnvelope(readerVisible, Kind.READER_VISIBLE, null);
    if (readerVisible.getEpoch() == checkpoint.getEpoch()
        && !Arrays.equals(readerVisible.getMutationPlanDigest(),
            checkpoint.getMutationPlanDigest())) {
      throw new ArchivePersistenceException(
          "Archive reader mutation-plan digest mismatch");
    }
    return new RecoverySnapshot(history.committedHeadEpoch(), checkpoint.getEpoch(),
        participantHeads, readerVisible.getEpoch());
  }

  private void validateEnvelope(ArchiveProgressEnvelope envelope, Kind kind, String participant)
      throws IOException {
    HistoryCommitMarker marker = history.committedMarker(envelope.getEpoch());
    if (marker == null) {
      throw new ArchivePersistenceException(
          "Missing committed history identity at epoch " + envelope.getEpoch());
    }
    if (marker.getMeta().getEpoch() != envelope.getEpoch()
        || !marker.getDatabases().equals(participants)) {
      throw new ArchivePersistenceException(
          "Committed history identity mismatch at epoch " + envelope.getEpoch());
    }
    envelope.requireIdentity(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), participants);
  }

  private static List<String> validateParticipants(List<String> participants) {
    Objects.requireNonNull(participants, "participants");
    if (participants.isEmpty()) {
      throw new IllegalArgumentException("Archive participant set must not be empty");
    }
    List<String> copy = new ArrayList<>(participants.size());
    String previous = null;
    for (String participant : participants) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Archive participants must be non-empty, unique, and sorted");
      }
      copy.add(participant);
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  /** Committed history identity lookup. A missing epoch returns {@code null}. */
  public interface HistoryIdentitySource {
    long committedHeadEpoch() throws IOException;

    HistoryCommitMarker committedMarker(long epoch) throws IOException;
  }

  /** Durable apply checkpoint, participant progress and reader-visible head lookup. */
  public interface ProgressIdentitySource {
    ArchiveProgressEnvelope loadCheckpoint() throws IOException;

    Map<String, ArchiveProgressEnvelope> loadParticipantProgress() throws IOException;

    ArchiveProgressEnvelope loadReaderVisible() throws IOException;
  }
}
