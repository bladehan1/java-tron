package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/** Deterministic fail-closed planner for one durable H/C/D[i]/R recovery snapshot. */
public final class ArchiveRecoveryPlanner {

  static final long MAX_REPLAY_EPOCHS_PER_ACTION = 1024;

  private ArchiveRecoveryPlanner() {
  }

  public static RecoveryPlan plan(long historyHead, long checkpointHead,
      Map<String, Long> participantHeads, long readerVisibleHead) {
    if (historyHead < 0 || checkpointHead < 0 || readerVisibleHead < 0) {
      throw new ArchivePersistenceException("Archive recovery heads must be non-negative");
    }
    if (checkpointHead > historyHead) {
      throw new ArchivePersistenceException("Archive checkpoint is ahead of history");
    }
    SortedMap<String, Long> sortedHeads = validateParticipants(participantHeads, checkpointHead);
    long safeHead = Math.min(historyHead, checkpointHead);
    for (long participantHead : sortedHeads.values()) {
      safeHead = Math.min(safeHead, participantHead);
    }
    if (readerVisibleHead > safeHead) {
      throw new ArchivePersistenceException("Reader-visible archive head is unsafe");
    }

    List<RecoveryAction> actions = new ArrayList<>();
    if (historyHead > checkpointHead) {
      actions.add(RecoveryAction.truncateHistory(checkpointHead));
    }
    sortedHeads.forEach((participant, appliedHead) -> addReplayActions(actions, participant,
        appliedHead, checkpointHead));
    if (readerVisibleHead < checkpointHead) {
      actions.add(RecoveryAction.publishReaderHead(checkpointHead));
    }
    return new RecoveryPlan(historyHead, checkpointHead, sortedHeads, readerVisibleHead,
        safeHead, actions);
  }

  private static SortedMap<String, Long> validateParticipants(Map<String, Long> participantHeads,
      long checkpointHead) {
    Objects.requireNonNull(participantHeads, "participantHeads");
    if (participantHeads.isEmpty()) {
      throw new ArchivePersistenceException("Archive recovery participant set is empty");
    }
    SortedMap<String, Long> sorted = new TreeMap<>();
    participantHeads.forEach((participant, head) -> {
      if (participant == null || participant.isEmpty() || head == null || head < 0) {
        throw new ArchivePersistenceException("Archive participant progress is invalid");
      }
      if (head > checkpointHead) {
        throw new ArchivePersistenceException(
            "Archive participant is ahead of the checkpoint: " + participant);
      }
      sorted.put(participant, head);
    });
    return sorted;
  }

  private static void addReplayActions(List<RecoveryAction> actions, String participant,
      long appliedHead, long checkpointHead) {
    if (appliedHead == checkpointHead) {
      return;
    }
    long first = appliedHead + 1;
    while (first <= checkpointHead) {
      long remaining = checkpointHead - first;
      long last = remaining >= MAX_REPLAY_EPOCHS_PER_ACTION
          ? first + MAX_REPLAY_EPOCHS_PER_ACTION - 1 : checkpointHead;
      actions.add(RecoveryAction.replayParticipant(participant, first, last));
      if (last == checkpointHead) {
        break;
      }
      first = last + 1;
    }
  }

  public enum ActionType {
    TRUNCATE_HISTORY,
    REPLAY_PARTICIPANT,
    PUBLISH_READER_HEAD
  }

  public static final class RecoveryAction {
    private final ActionType type;
    private final String participant;
    private final long firstEpoch;
    private final long lastEpoch;

    private RecoveryAction(ActionType type, String participant, long firstEpoch,
        long lastEpoch) {
      this.type = type;
      this.participant = participant;
      this.firstEpoch = firstEpoch;
      this.lastEpoch = lastEpoch;
    }

    private static RecoveryAction truncateHistory(long head) {
      return new RecoveryAction(ActionType.TRUNCATE_HISTORY, null, head, head);
    }

    private static RecoveryAction replayParticipant(String participant, long firstEpoch,
        long lastEpoch) {
      return new RecoveryAction(ActionType.REPLAY_PARTICIPANT, participant, firstEpoch,
          lastEpoch);
    }

    private static RecoveryAction publishReaderHead(long head) {
      return new RecoveryAction(ActionType.PUBLISH_READER_HEAD, null, head, head);
    }

    public ActionType getType() {
      return type;
    }

    public String getParticipant() {
      return participant;
    }

    public long getFirstEpoch() {
      return firstEpoch;
    }

    public long getLastEpoch() {
      return lastEpoch;
    }
  }

  public static final class RecoveryPlan {
    private final long historyHead;
    private final long checkpointHead;
    private final SortedMap<String, Long> participantHeads;
    private final long readerVisibleHead;
    private final long safeHeadBeforeRecovery;
    private final List<RecoveryAction> actions;

    private RecoveryPlan(long historyHead, long checkpointHead,
        SortedMap<String, Long> participantHeads, long readerVisibleHead,
        long safeHeadBeforeRecovery, List<RecoveryAction> actions) {
      this.historyHead = historyHead;
      this.checkpointHead = checkpointHead;
      this.participantHeads = Collections.unmodifiableSortedMap(new TreeMap<>(participantHeads));
      this.readerVisibleHead = readerVisibleHead;
      this.safeHeadBeforeRecovery = safeHeadBeforeRecovery;
      this.actions = Collections.unmodifiableList(new ArrayList<>(actions));
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

    public long getSafeHeadBeforeRecovery() {
      return safeHeadBeforeRecovery;
    }

    public List<RecoveryAction> getActions() {
      return actions;
    }
  }
}
