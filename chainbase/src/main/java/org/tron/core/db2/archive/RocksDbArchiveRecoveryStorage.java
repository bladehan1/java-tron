package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoveryStorage;
import org.tron.core.db2.archive.RocksDbArchiveParticipant.Mutation;

/** File-history plus native RocksDB participant implementation of the H/C/D[i]/R executor. */
public final class RocksDbArchiveRecoveryStorage implements RecoveryStorage, Closeable {

  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final List<String> participants;
  private final Map<String, RocksDbArchiveParticipant> participantEngines;
  private final ParticipantReplayer replayer;
  private final HistorySegmentStore bodies;
  private final HistoryIndexStore index;
  private final HistoryCommitStore history;
  private final ArchiveRecoveryAuthorityScanner scanner;
  private final ArchiveReaderHeadPublisher readerPublisher;

  public RocksDbArchiveRecoveryStorage(Path archiveDirectory, long maxSegmentSize,
      Path checkpointPath, Map<String, RocksDbArchiveParticipant> participantEngines,
      Path readerVisiblePath, List<String> participants, ParticipantReplayer replayer)
      throws IOException {
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.maxSegmentSize = maxSegmentSize;
    this.participants = validateParticipants(participants);
    TreeMap<String, RocksDbArchiveParticipant> sorted = new TreeMap<>(
        Objects.requireNonNull(participantEngines, "participantEngines"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant engine set mismatch");
    }
    this.participantEngines = Collections.unmodifiableMap(sorted);
    this.replayer = Objects.requireNonNull(replayer, "replayer");
    new ArchiveTruncationRecovery(archiveDirectory, maxSegmentSize).recover();
    ArchiveRestartCheckpoint checkpoint = ArchiveRestartCheckpoint.load(archiveDirectory,
        new HistoryCommitMarkerCodec());
    if (checkpoint == null) {
      throw new ArchivePersistenceException("Archive restart checkpoint is missing");
    }
    HistorySegmentStore openedBodies = null;
    HistoryIndexStore openedIndex = null;
    HistoryCommitStore openedHistory = null;
    try {
      openedBodies = new HistorySegmentStore(archiveDirectory, new BlockHistoryCodec(),
          maxSegmentSize, checkpoint);
      openedIndex = new HistoryIndexStore(archiveDirectory, new HistoryIndexCodec(), checkpoint);
      openedHistory = new HistoryCommitStore(archiveDirectory, new HistoryCommitMarkerCodec(),
          checkpoint);
    } catch (IOException | RuntimeException failure) {
      close(openedIndex, failure);
      close(openedBodies, failure);
      close(openedHistory, failure);
      throw failure;
    }
    this.bodies = openedBodies;
    this.index = openedIndex;
    this.history = openedHistory;
    this.scanner = ArchiveRecoveryAuthorityScanner.forRocksDbParticipants(this.history,
        checkpointPath, this.participantEngines, readerVisiblePath, this.participants);
    this.readerPublisher = new ArchiveReaderHeadPublisher(this.history, readerVisiblePath,
        this.participants);
  }

  @Override
  public RecoverySnapshot scan() throws IOException {
    return scanner.scan();
  }

  @Override
  public void truncateHistoryAndSync(long historyHead) throws IOException {
    ArchiveTruncationIntent.prepare(archiveDirectory, history, index, bodies, historyHead,
        new HistoryCommitMarkerCodec());
    new ArchiveTruncationRecovery(archiveDirectory, maxSegmentSize).recover();
  }

  @Override
  public void replayParticipantAndSyncProgress(String participant, long firstEpoch,
      long lastEpoch) throws IOException {
    RocksDbArchiveParticipant engine = participantEngines.get(participant);
    if (engine == null) {
      throw new ArchivePersistenceException("Unknown archive recovery participant: " + participant);
    }
    HistoryCommitMarker marker = history.get(lastEpoch);
    if (marker == null || firstEpoch > lastEpoch) {
      throw new ArchivePersistenceException("Archive participant replay range is invalid");
    }
    List<Mutation> mutations = Objects.requireNonNull(
        replayer.replay(participant, firstEpoch, lastEpoch), "participant replay mutations");
    ArchiveProgressEnvelope progress = new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS,
        participant, lastEpoch, marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), participants);
    engine.apply(mutations, progress);
  }

  @Override
  public void publishReaderHeadAndSync(long readerVisibleHead) throws IOException {
    readerPublisher.publish(readerVisibleHead);
  }

  @Override
  public void close() throws IOException {
    IOException failure = null;
    try {
      index.close();
    } catch (IOException closeFailure) {
      failure = closeFailure;
    }
    try {
      bodies.close();
    } catch (IOException closeFailure) {
      failure = add(failure, closeFailure);
    }
    try {
      history.close();
    } catch (IOException closeFailure) {
      failure = add(failure, closeFailure);
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Archive participant set must not be empty");
    }
    String previous = null;
    for (String participant : copy) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Archive participants must be non-empty, unique, and sorted");
      }
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  private static IOException add(IOException current, IOException addition) {
    if (current == null) {
      return addition;
    }
    current.addSuppressed(addition);
    return current;
  }

  private static void close(Closeable resource, Exception failure) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  @FunctionalInterface
  public interface ParticipantReplayer {
    List<Mutation> replay(String participant, long firstEpoch, long lastEpoch) throws IOException;
  }
}
