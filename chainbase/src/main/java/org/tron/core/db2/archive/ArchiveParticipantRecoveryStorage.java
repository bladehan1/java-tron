package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoveryStorage;

/** File history plus mixed native participants implementing the H/C/D[i]/R executor. */
public final class ArchiveParticipantRecoveryStorage implements RecoveryStorage, Closeable {

  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final List<String> participants;
  private final Map<String, ArchiveParticipant> participantEngines;
  private final ArchiveProgressFile checkpointFile;
  private final ArchiveTargetMutationPlanFile mutationPlanFile;
  private final HistorySegmentStore bodies;
  private final HistoryIndexStore index;
  private final HistoryCommitStore history;
  private final ArchiveRecoveryAuthorityScanner scanner;
  private final ArchiveReaderPublicationGate publicationGate;
  private final ArchiveStateBarrier.ArchiveStateAction refresh;

  public ArchiveParticipantRecoveryStorage(Path archiveDirectory, long maxSegmentSize,
      Path checkpointPath, Map<String, ? extends ArchiveParticipant> participantEngines,
      Path readerVisiblePath, List<String> participants)
      throws IOException {
    this(archiveDirectory, maxSegmentSize, checkpointPath, participantEngines,
        readerVisiblePath, participants, action -> action.run(), () -> { });
  }

  public ArchiveParticipantRecoveryStorage(Path archiveDirectory, long maxSegmentSize,
      Path checkpointPath, Map<String, ? extends ArchiveParticipant> participantEngines,
      Path readerVisiblePath, List<String> participants, ArchiveStateBarrier barrier,
      ArchiveStateBarrier.ArchiveStateAction refresh)
      throws IOException {
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.maxSegmentSize = maxSegmentSize;
    this.participants = validateParticipants(participants);
    TreeMap<String, ArchiveParticipant> sorted = new TreeMap<>(
        Objects.requireNonNull(participantEngines, "participantEngines"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant engine set mismatch");
    }
    this.participantEngines = Collections.unmodifiableMap(sorted);
    ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
    this.checkpointFile = new ArchiveProgressFile(checkpointPath, progressCodec);
    this.mutationPlanFile = new ArchiveTargetMutationPlanFile(checkpointPath);
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
    this.scanner = ArchiveRecoveryAuthorityScanner.forParticipants(this.history,
        checkpointPath, this.participantEngines, readerVisiblePath, this.participants);
    this.publicationGate = new ArchiveReaderPublicationGate(this.history,
        checkpointFile::load,
        this.participantEngines, readerVisiblePath, this.participants,
        Objects.requireNonNull(barrier, "barrier"));
    this.refresh = Objects.requireNonNull(refresh, "refresh");
  }

  @Override
  public RecoverySnapshot scan() throws IOException {
    RecoverySnapshot snapshot = scanner.scan();
    ArchiveTargetMutationPlan plan = mutationPlanFile.loadIfPresent();
    boolean fixed = isFixed(snapshot);
    if (plan == null) {
      if (!authoritiesAtCheckpoint(snapshot)) {
        throw new ArchivePersistenceException("Archive recovery mutation plan is missing");
      }
      return snapshot;
    }
    long planEpoch = plan.getTarget().getEpoch();
    HistoryCommitMarker marker = history.get(planEpoch);
    if (marker != null) {
      plan.requireIdentity(marker, participants);
    }
    long checkpoint = snapshot.getCheckpointHead();
    boolean preparedOnly = planEpoch == checkpoint + 1 && authoritiesAtCheckpoint(snapshot);
    if (planEpoch != checkpoint && !preparedOnly || marker == null && !preparedOnly) {
      throw new ArchivePersistenceException("Mutation plan does not match recovery checkpoint");
    }
    if (!preparedOnly) {
      requirePlanDigest(plan, checkpointFile.load());
    }
    return snapshot;
  }

  /** Returns the committed H head observed by this startup recovery session. */
  public HistoryCommitMarker committedHead() {
    return history.head();
  }

  @Override
  public void truncateHistoryAndSync(long historyHead) throws IOException {
    ArchiveTruncationIntent.prepare(archiveDirectory, history, index, bodies, historyHead,
        new HistoryCommitMarkerCodec());
    new ArchiveTruncationRecovery(archiveDirectory, maxSegmentSize).recover();
    history.truncateAfter(historyHead);
  }

  @Override
  public void replayParticipantAndSyncProgress(String participant, long firstEpoch,
      long lastEpoch) throws IOException {
    ArchiveParticipant engine = participantEngines.get(participant);
    if (engine == null) {
      throw new ArchivePersistenceException("Unknown archive recovery participant: " + participant);
    }
    HistoryCommitMarker marker = history.get(lastEpoch);
    if (marker == null || firstEpoch > lastEpoch) {
      throw new ArchivePersistenceException("Archive participant replay range is invalid");
    }
    ArchiveTargetMutationPlan plan = mutationPlanFile.loadRequired();
    plan.requireIdentity(marker, participants);
    if (plan.getTarget().getEpoch() != lastEpoch) {
      throw new ArchivePersistenceException("Mutation plan does not cover replay range");
    }
    byte[] mutationPlanDigest = requirePlanDigest(plan, checkpointFile.load());
    List<ArchiveParticipantMutation> mutations = plan.getMutations(participant);
    ArchiveProgressEnvelope progress = new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS,
        participant, lastEpoch, marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), mutationPlanDigest, participants);
    engine.apply(mutations, progress);
  }

  @Override
  public void publishReaderHeadAndSync(long readerVisibleHead) throws IOException {
    publicationGate.publishAfterRefresh(readerVisibleHead, refresh);
  }

  @Override
  public void recoveryComplete() throws IOException {
    RecoverySnapshot snapshot = scanner.scan();
    if (!isFixed(snapshot)) {
      throw new ArchivePersistenceException("Archive recovery did not reach a fixed point");
    }
    ArchiveTargetMutationPlan plan = mutationPlanFile.loadIfPresent();
    if (plan != null) {
      long epoch = plan.getTarget().getEpoch();
      HistoryCommitMarker marker = history.get(epoch);
      if (marker != null) {
        plan.requireIdentity(marker, participants);
      }
      if (epoch != snapshot.getCheckpointHead() && epoch != snapshot.getCheckpointHead() + 1) {
        throw new ArchivePersistenceException("Completed recovery has an unrelated mutation plan");
      }
      if (epoch == snapshot.getCheckpointHead()) {
        requirePlanDigest(plan, checkpointFile.load());
      }
    }
    mutationPlanFile.retire();
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

  private static boolean isFixed(RecoverySnapshot snapshot) {
    long checkpoint = snapshot.getCheckpointHead();
    if (snapshot.getHistoryHead() != checkpoint
        || !authoritiesAtCheckpoint(snapshot)) {
      return false;
    }
    return true;
  }

  private static boolean authoritiesAtCheckpoint(RecoverySnapshot snapshot) {
    long checkpoint = snapshot.getCheckpointHead();
    if (snapshot.getReaderVisibleHead() != checkpoint) {
      return false;
    }
    for (long participant : snapshot.getParticipantHeads().values()) {
      if (participant != checkpoint) {
        return false;
      }
    }
    return true;
  }

  private static byte[] requirePlanDigest(ArchiveTargetMutationPlan plan,
      ArchiveProgressEnvelope checkpoint) {
    byte[] actual = plan.digest();
    if (!Arrays.equals(actual, checkpoint.getMutationPlanDigest())) {
      throw new ArchivePersistenceException(
          "Archive checkpoint mutation-plan digest mismatch");
    }
    return actual;
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

}
