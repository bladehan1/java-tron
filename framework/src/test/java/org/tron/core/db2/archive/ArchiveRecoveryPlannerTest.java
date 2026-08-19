package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.ActionType;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryAction;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;

public class ArchiveRecoveryPlannerTest {

  @Test
  public void resumesOnlyRemainingParticipantAfterASecondCrash() {
    Map<String, Long> firstHeads = heads("storage-row", 7L, "account", 10L,
        "account-asset", 8L);
    RecoveryPlan first = ArchiveRecoveryPlanner.plan(12, 10, firstHeads, 7);

    assertEquals(7, first.getSafeHeadBeforeRecovery());
    assertActions(first.getActions(),
        action(ActionType.TRUNCATE_HISTORY, null, 10, 10),
        action(ActionType.REPLAY_PARTICIPANT, "account-asset", 9, 10),
        action(ActionType.REPLAY_PARTICIPANT, "storage-row", 8, 10),
        action(ActionType.PUBLISH_READER_HEAD, null, 10, 10));

    // The process crashes after truncating H and durably advancing only account-asset D[i].
    Map<String, Long> secondHeads = heads("storage-row", 7L, "account", 10L,
        "account-asset", 10L);
    RecoveryPlan second = ArchiveRecoveryPlanner.plan(10, 10, secondHeads, 7);

    assertActions(second.getActions(),
        action(ActionType.REPLAY_PARTICIPANT, "storage-row", 8, 10),
        action(ActionType.PUBLISH_READER_HEAD, null, 10, 10));

    Map<String, Long> recoveredHeads = heads("storage-row", 10L, "account", 10L,
        "account-asset", 10L);
    RecoveryPlan recovered = ArchiveRecoveryPlanner.plan(10, 10, recoveredHeads, 10);
    assertEquals(10, recovered.getSafeHeadBeforeRecovery());
    assertEquals(0, recovered.getActions().size());
  }

  @Test
  public void chunksEveryParticipantReplayRange() {
    RecoveryPlan plan = ArchiveRecoveryPlanner.plan(2_050, 2_050,
        heads("account", 0L), 0);

    assertActions(plan.getActions(),
        action(ActionType.REPLAY_PARTICIPANT, "account", 1, 1_024),
        action(ActionType.REPLAY_PARTICIPANT, "account", 1_025, 2_048),
        action(ActionType.REPLAY_PARTICIPANT, "account", 2_049, 2_050),
        action(ActionType.PUBLISH_READER_HEAD, null, 2_050, 2_050));
  }

  @Test
  public void rejectsEveryAheadOrUnsafeStateBeforePlanningActions() {
    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveRecoveryPlanner.plan(9, 10, heads("account", 9L), 9));
    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveRecoveryPlanner.plan(10, 10, heads("account", 11L), 10));
    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveRecoveryPlanner.plan(10, 10, heads("account", 8L), 9));
    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveRecoveryPlanner.plan(10, 10, java.util.Collections.emptyMap(), 10));
  }

  private static Map<String, Long> heads(Object... values) {
    Map<String, Long> heads = new LinkedHashMap<>();
    for (int index = 0; index < values.length; index += 2) {
      heads.put((String) values[index], (Long) values[index + 1]);
    }
    return heads;
  }

  private static ExpectedAction action(ActionType type, String participant, long first,
      long last) {
    return new ExpectedAction(type, participant, first, last);
  }

  private static void assertActions(List<RecoveryAction> actual, ExpectedAction... expected) {
    assertEquals(expected.length, actual.size());
    for (int index = 0; index < expected.length; index++) {
      ExpectedAction left = expected[index];
      RecoveryAction right = actual.get(index);
      assertEquals(left.type, right.getType());
      assertEquals(left.participant, right.getParticipant());
      assertEquals(left.firstEpoch, right.getFirstEpoch());
      assertEquals(left.lastEpoch, right.getLastEpoch());
    }
  }

  private static final class ExpectedAction {
    private final ActionType type;
    private final String participant;
    private final long firstEpoch;
    private final long lastEpoch;

    private ExpectedAction(ActionType type, String participant, long firstEpoch, long lastEpoch) {
      this.type = type;
      this.participant = participant;
      this.firstEpoch = firstEpoch;
      this.lastEpoch = lastEpoch;
    }
  }
}
