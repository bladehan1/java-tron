package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Publishes reader-visible R only after fresh H/C/D identity convergence under one barrier. */
public final class ArchiveReaderPublicationGate {

  private final HistoryCommitStore history;
  private final ProgressSource checkpointSource;
  private final Map<String, ArchiveParticipantProgressSource> participantSources;
  private final Path readerVisiblePath;
  private final ArchiveProgressFile readerVisibleFile;
  private final ArchiveReaderHeadPublisher publisher;
  private final List<String> participants;
  private final ArchiveStateBarrier barrier;

  public ArchiveReaderPublicationGate(HistoryCommitStore history,
      ProgressSource checkpointSource,
      Map<String, ? extends ArchiveParticipantProgressSource> participantSources,
      Path readerVisiblePath, List<String> participants, ArchiveStateBarrier barrier) {
    this(history, checkpointSource, participantSources, readerVisiblePath, participants, barrier,
        temporary -> { });
  }

  ArchiveReaderPublicationGate(HistoryCommitStore history,
      ProgressSource checkpointSource,
      Map<String, ? extends ArchiveParticipantProgressSource> participantSources,
      Path readerVisiblePath, List<String> participants, ArchiveStateBarrier barrier,
      ArchiveProgressFile.FaultHook faultHook) {
    this.history = Objects.requireNonNull(history, "history");
    this.checkpointSource = Objects.requireNonNull(checkpointSource, "checkpointSource");
    this.participants = validateParticipants(participants);
    TreeMap<String, ArchiveParticipantProgressSource> sorted = new TreeMap<>(
        Objects.requireNonNull(participantSources, "participantSources"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive publication participant source set mismatch");
    }
    this.participantSources = Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    this.readerVisiblePath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.readerVisibleFile = new ArchiveProgressFile(readerVisiblePath,
        new ArchiveProgressEnvelopeCodec());
    this.publisher = new ArchiveReaderHeadPublisher(history, readerVisiblePath, this.participants,
        Objects.requireNonNull(faultHook, "faultHook"));
    this.barrier = Objects.requireNonNull(barrier, "barrier");
  }

  public static ArchiveReaderPublicationGate forFiles(HistoryCommitStore history,
      Path checkpointPath, Map<String, Path> participantPaths, Path readerVisiblePath,
      List<String> participants, ArchiveStateBarrier barrier) {
    Objects.requireNonNull(checkpointPath, "checkpointPath");
    TreeMap<String, Path> sorted = new TreeMap<>(
        Objects.requireNonNull(participantPaths, "participantPaths"));
    if (sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive publication participant path is missing");
    }
    Map<String, ArchiveParticipantProgressSource> sources = new LinkedHashMap<>();
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    sorted.forEach((participant, path) -> sources.put(participant,
        () -> new ArchiveProgressFile(path, codec).load()));
    return new ArchiveReaderPublicationGate(history,
        () -> new ArchiveProgressFile(checkpointPath, codec).load(), sources,
        readerVisiblePath, participants, barrier);
  }

  public void publish(long targetEpoch) throws IOException {
    publishAfterRefresh(targetEpoch, () -> { });
  }

  public void publishAfterRefresh(long targetEpoch,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    if (targetEpoch < 0) {
      throw new IllegalArgumentException("Reader publication target must be non-negative");
    }
    Objects.requireNonNull(refresh, "refresh");
    barrier.run(() -> {
      refresh.run();
      publishInsideBarrier(targetEpoch);
    });
  }

  private void publishInsideBarrier(long targetEpoch) throws IOException {
    HistoryCommitMarker target = requireMarker(targetEpoch);
    validateCurrentReader(targetEpoch);
    byte[] firstDigest = validateAuthorities(target);
    byte[] secondDigest = validateAuthorities(target);
    if (!Arrays.equals(firstDigest, secondDigest)) {
      throw new ArchivePersistenceException(
          "Archive mutation-plan authority drifted during reader publication");
    }
    HistoryCommitMarker reloaded = requireMarker(targetEpoch);
    if (!sameIdentity(target, reloaded)) {
      throw new ArchivePersistenceException(
          "Committed history identity drifted during reader publication");
    }
    publisher.publish(targetEpoch, secondDigest);
  }

  private HistoryCommitMarker requireMarker(long targetEpoch) {
    HistoryCommitMarker marker = history.get(targetEpoch);
    if (marker == null || marker.getMeta().getEpoch() != targetEpoch
        || !marker.getDatabases().equals(participants)) {
      throw new ArchivePersistenceException(
          "Missing or mismatched committed publication target: " + targetEpoch);
    }
    return marker;
  }

  private void validateCurrentReader(long targetEpoch) throws IOException {
    if (!Files.exists(readerVisiblePath)) {
      return;
    }
    ArchiveProgressEnvelope current = readerVisibleFile.load();
    HistoryCommitMarker marker = requireMarker(current.getEpoch());
    requireIdentity(current, Kind.READER_VISIBLE, null, marker);
    if (current.getEpoch() > targetEpoch) {
      throw new ArchivePersistenceException("Reader-visible authority cannot move backwards");
    }
  }

  private byte[] validateAuthorities(HistoryCommitMarker target) throws IOException {
    ArchiveProgressEnvelope checkpoint = load(checkpointSource, "archive apply checkpoint");
    requireIdentity(checkpoint, Kind.APPLY_CHECKPOINT, null, target);
    byte[] mutationPlanDigest = checkpoint.getMutationPlanDigest();
    for (Map.Entry<String, ArchiveParticipantProgressSource> entry
        : participantSources.entrySet()) {
      ArchiveProgressEnvelope progress = entry.getValue().loadProgress();
      if (progress == null) {
        throw new ArchivePersistenceException(
            "Missing archive participant progress: " + entry.getKey());
      }
      requireIdentity(progress, Kind.PARTICIPANT_PROGRESS, entry.getKey(), target);
      if (!Arrays.equals(mutationPlanDigest, progress.getMutationPlanDigest())) {
        throw new ArchivePersistenceException(
            "Archive participant mutation-plan digest mismatch: " + entry.getKey());
      }
    }
    return mutationPlanDigest;
  }

  private ArchiveProgressEnvelope load(ProgressSource source, String name) throws IOException {
    ArchiveProgressEnvelope envelope = source.load();
    if (envelope == null) {
      throw new ArchivePersistenceException("Missing " + name);
    }
    return envelope;
  }

  private void requireIdentity(ArchiveProgressEnvelope envelope, Kind kind, String participant,
      HistoryCommitMarker marker) {
    envelope.requireIdentity(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), participants);
  }

  private static boolean sameIdentity(HistoryCommitMarker left, HistoryCommitMarker right) {
    return left.getMeta().getEpoch() == right.getMeta().getEpoch()
        && Arrays.equals(left.getMeta().getBlockHash(), right.getMeta().getBlockHash())
        && Arrays.equals(left.getBatchId(), right.getBatchId())
        && Arrays.equals(left.getHistoryLocation().getBodyDigest(),
            right.getHistoryLocation().getBodyDigest())
        && left.getDatabases().equals(right.getDatabases());
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Archive publication participant set must not be empty");
    }
    String previous = null;
    for (String participant : copy) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Archive publication participants must be non-empty, unique, and sorted");
      }
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  @FunctionalInterface
  public interface ProgressSource {
    ArchiveProgressEnvelope load() throws IOException;
  }
}
