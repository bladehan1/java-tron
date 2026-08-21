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
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;

/** File-backed prototype authority adapters for one fresh validating recovery scan. */
public final class ArchiveRecoveryAuthorityScanner {

  private final CommittedHistoryAuthority history;
  private final Path checkpointPath;
  private final Map<String, ArchiveParticipantProgressSource> participantSources;
  private final Path readerVisiblePath;
  private final List<String> participants;
  private final ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();

  public ArchiveRecoveryAuthorityScanner(CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, Path> participantPaths, Path readerVisiblePath,
      List<String> participants) {
    this.history = Objects.requireNonNull(history, "history");
    this.checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath");
    this.readerVisiblePath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.participants = validateParticipants(participants);
    TreeMap<String, Path> sortedPaths = new TreeMap<>(
        Objects.requireNonNull(participantPaths, "participantPaths"));
    if (!new ArrayList<>(sortedPaths.keySet()).equals(this.participants)
        || sortedPaths.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant progress path set mismatch");
    }
    Map<String, ArchiveParticipantProgressSource> sources = new LinkedHashMap<>();
    sortedPaths.forEach((participant, path) ->
        sources.put(participant, () -> progressFile(path).load()));
    this.participantSources = Collections.unmodifiableMap(sources);
  }

  private ArchiveRecoveryAuthorityScanner(CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, ArchiveParticipantBatchFile> participantBatches, Path readerVisiblePath,
      List<String> participants, boolean batchAuthority) {
    this.history = Objects.requireNonNull(history, "history");
    this.checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath");
    this.readerVisiblePath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.participants = validateParticipants(participants);
    TreeMap<String, ArchiveParticipantBatchFile> sortedBatches = new TreeMap<>(
        Objects.requireNonNull(participantBatches, "participantBatches"));
    if (!new ArrayList<>(sortedBatches.keySet()).equals(this.participants)
        || sortedBatches.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant batch set mismatch");
    }
    Map<String, ArchiveParticipantProgressSource> sources = new LinkedHashMap<>();
    sortedBatches.forEach((participant, batch) ->
        sources.put(participant, () -> batch.load().getProgress()));
    this.participantSources = Collections.unmodifiableMap(sources);
  }

  private ArchiveRecoveryAuthorityScanner(CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipantProgressSource> participantSources,
      Path readerVisiblePath,
      List<String> participants, byte nativeEngineAuthority) {
    this.history = Objects.requireNonNull(history, "history");
    this.checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath");
    this.readerVisiblePath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.participants = validateParticipants(participants);
    TreeMap<String, ArchiveParticipantProgressSource> sortedSources = new TreeMap<>(
        Objects.requireNonNull(participantSources, "participantSources"));
    if (!new ArrayList<>(sortedSources.keySet()).equals(this.participants)
        || sortedSources.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant source set mismatch");
    }
    this.participantSources = Collections.unmodifiableMap(
        new LinkedHashMap<>(sortedSources));
  }

  public static ArchiveRecoveryAuthorityScanner forParticipantBatches(
      CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, ArchiveParticipantBatchFile> participantBatches, Path readerVisiblePath,
      List<String> participants) {
    return new ArchiveRecoveryAuthorityScanner(history, checkpointPath, participantBatches,
        readerVisiblePath, participants, true);
  }

  public static ArchiveRecoveryAuthorityScanner forParticipants(
      CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipantProgressSource> participantEngines,
      Path readerVisiblePath,
      List<String> participants) {
    Map<String, ArchiveParticipantProgressSource> sources = new LinkedHashMap<>();
    Objects.requireNonNull(participantEngines, "participantEngines")
        .forEach(sources::put);
    return new ArchiveRecoveryAuthorityScanner(history, checkpointPath, sources,
        readerVisiblePath, participants, (byte) 1);
  }

  public RecoverySnapshot scan() throws IOException {
    ArchiveRecoveryScanner.HistoryIdentitySource historySource =
        new ArchiveRecoveryScanner.HistoryIdentitySource() {
          @Override
          public long committedHeadEpoch() {
            HistoryCommitMarker head = history.head();
            if (head == null) {
              throw new ArchivePersistenceException("Committed archive history is empty");
            }
            return head.getMeta().getEpoch();
          }

          @Override
          public HistoryCommitMarker committedMarker(long epoch) {
            return history.get(epoch);
          }
        };
    ArchiveRecoveryScanner.ProgressIdentitySource progressSource =
        new ArchiveRecoveryScanner.ProgressIdentitySource() {
          @Override
          public ArchiveProgressEnvelope loadCheckpoint() throws IOException {
            return progressFile(checkpointPath).load();
          }

          @Override
          public Map<String, ArchiveProgressEnvelope> loadParticipantProgress()
              throws IOException {
            Map<String, ArchiveProgressEnvelope> loaded = new LinkedHashMap<>();
            for (Map.Entry<String, ArchiveParticipantProgressSource> entry
                : participantSources.entrySet()) {
              loaded.put(entry.getKey(), entry.getValue().loadProgress());
            }
            return loaded;
          }

          @Override
          public ArchiveProgressEnvelope loadReaderVisible() throws IOException {
            return progressFile(readerVisiblePath).load();
          }
        };
    return new ArchiveRecoveryScanner(historySource, progressSource, participants).scan();
  }

  private ArchiveProgressFile progressFile(Path path) {
    return new ArchiveProgressFile(path, progressCodec);
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
}
