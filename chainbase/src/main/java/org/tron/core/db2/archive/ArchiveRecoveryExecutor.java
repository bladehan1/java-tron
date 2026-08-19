package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.ActionType;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryAction;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;

/** Executes a fresh H/C/D[i]/R plan against explicitly durable storage boundaries. */
public final class ArchiveRecoveryExecutor {

  private final RecoveryStorage storage;
  private final FaultHook faultHook;

  public ArchiveRecoveryExecutor(RecoveryStorage storage) {
    this(storage, action -> { });
  }

  ArchiveRecoveryExecutor(RecoveryStorage storage, FaultHook faultHook) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public RecoveryPlan recover() {
    try {
      RecoverySnapshot snapshot = Objects.requireNonNull(storage.scan(), "recovery snapshot");
      RecoveryPlan plan = ArchiveRecoveryPlanner.plan(snapshot.getHistoryHead(),
          snapshot.getCheckpointHead(), snapshot.getParticipantHeads(),
          snapshot.getReaderVisibleHead());
      for (RecoveryAction action : plan.getActions()) {
        execute(action);
        faultHook.afterDurableAction(action);
      }
      return plan;
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Archive recovery action failed", failure);
    }
  }

  private void execute(RecoveryAction action) throws IOException {
    ActionType type = action.getType();
    switch (type) {
      case TRUNCATE_HISTORY:
        storage.truncateHistoryAndSync(action.getLastEpoch());
        return;
      case REPLAY_PARTICIPANT:
        storage.replayParticipantAndSyncProgress(action.getParticipant(),
            action.getFirstEpoch(), action.getLastEpoch());
        return;
      case PUBLISH_READER_HEAD:
        storage.publishReaderHeadAndSync(action.getLastEpoch());
        return;
      default:
        throw new ArchivePersistenceException("Unsupported archive recovery action: " + type);
    }
  }

  /**
   * Durable recovery boundary supplied by the archive history, checkpoint and participant engines.
   *
   * <p>{@link #replayParticipantAndSyncProgress} must atomically persist the participant's business
   * mutations and D[i] progress in one sync engine batch. Returning before both are durable violates
   * the recovery contract.
   */
  public interface RecoveryStorage {
    RecoverySnapshot scan() throws IOException;

    void truncateHistoryAndSync(long historyHead) throws IOException;

    void replayParticipantAndSyncProgress(String participant, long firstEpoch, long lastEpoch)
        throws IOException;

    void publishReaderHeadAndSync(long readerVisibleHead) throws IOException;
  }

  /** Immutable result of one fresh durable H/C/D[i]/R scan. */
  public static final class RecoverySnapshot {
    private final long historyHead;
    private final long checkpointHead;
    private final SortedMap<String, Long> participantHeads;
    private final long readerVisibleHead;

    public RecoverySnapshot(long historyHead, long checkpointHead,
        Map<String, Long> participantHeads, long readerVisibleHead) {
      this.historyHead = historyHead;
      this.checkpointHead = checkpointHead;
      this.participantHeads = Collections.unmodifiableSortedMap(
          new TreeMap<>(Objects.requireNonNull(participantHeads, "participantHeads")));
      this.readerVisibleHead = readerVisibleHead;
    }

    public long getHistoryHead() {
      return historyHead;
    }

    public long getCheckpointHead() {
      return checkpointHead;
    }

    public SortedMap<String, Long> getParticipantHeads() {
      return participantHeads;
    }

    public long getReaderVisibleHead() {
      return readerVisibleHead;
    }
  }

  @FunctionalInterface
  interface FaultHook {
    void afterDurableAction(RecoveryAction action) throws IOException;
  }
}
