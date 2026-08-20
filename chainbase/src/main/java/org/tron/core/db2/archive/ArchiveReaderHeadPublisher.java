package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Atomically publishes a reader-visible R identity derived from committed history. */
public final class ArchiveReaderHeadPublisher {

  private final HistoryCommitStore history;
  private final ArchiveProgressFile progressFile;
  private final List<String> participants;

  public ArchiveReaderHeadPublisher(HistoryCommitStore history, Path path,
      List<String> participants) {
    this(history, path, participants, temporary -> { });
  }

  ArchiveReaderHeadPublisher(HistoryCommitStore history, Path path, List<String> participants,
      ArchiveProgressFile.FaultHook faultHook) {
    this.history = Objects.requireNonNull(history, "history");
    this.progressFile = new ArchiveProgressFile(Objects.requireNonNull(path, "path"),
        new ArchiveProgressEnvelopeCodec(), Objects.requireNonNull(faultHook, "faultHook"));
    this.participants = validateParticipants(participants);
  }

  public void publish(long epoch) throws IOException {
    publish(epoch, null);
  }

  public void publish(long epoch, byte[] mutationPlanDigest) throws IOException {
    HistoryCommitMarker marker = history.get(epoch);
    if (marker == null || marker.getMeta().getEpoch() != epoch
        || !marker.getDatabases().equals(participants)) {
      throw new ArchivePersistenceException(
          "Missing or mismatched committed reader identity at epoch " + epoch);
    }
    progressFile.store(new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null, epoch,
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), mutationPlanDigest, participants));
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
