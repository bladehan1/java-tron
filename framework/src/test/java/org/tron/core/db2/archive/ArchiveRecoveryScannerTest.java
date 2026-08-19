package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.ActionType;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryAction;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;

public class ArchiveRecoveryScannerTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "account-asset");

  @Test
  public void resolvesLaggingParticipantAgainstItsOwnCommittedEpoch() throws Exception {
    TestHistory history = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    TestProgress progress = progress(envelope(Kind.APPLY_CHECKPOINT, null,
        history.markers.get(10L)), envelope(Kind.READER_VISIBLE, null,
        history.markers.get(8L)));
    progress.participantProgress.put("account",
        envelope(Kind.PARTICIPANT_PROGRESS, "account", history.markers.get(10L)));
    progress.participantProgress.put("account-asset",
        envelope(Kind.PARTICIPANT_PROGRESS, "account-asset", history.markers.get(8L)));

    RecoverySnapshot snapshot = scanner(history, progress).scan();
    assertEquals(12, snapshot.getHistoryHead());
    assertEquals(10, snapshot.getCheckpointHead());
    assertEquals(Long.valueOf(10), snapshot.getParticipantHeads().get("account"));
    assertEquals(Long.valueOf(8), snapshot.getParticipantHeads().get("account-asset"));
    assertEquals(8, snapshot.getReaderVisibleHead());

    RecoveryPlan plan = ArchiveRecoveryPlanner.plan(snapshot.getHistoryHead(),
        snapshot.getCheckpointHead(), snapshot.getParticipantHeads(),
        snapshot.getReaderVisibleHead());
    assertAction(plan.getActions().get(0), ActionType.TRUNCATE_HISTORY, null, 10, 10);
    assertAction(plan.getActions().get(1), ActionType.REPLAY_PARTICIPANT,
        "account-asset", 9, 10);
    assertAction(plan.getActions().get(2), ActionType.PUBLISH_READER_HEAD, null, 10, 10);
  }

  @Test
  public void rejectsMissingHistoryAndEveryProgressSourceGap() {
    TestHistory missingHistory = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    TestProgress valid = validProgress(missingHistory);
    missingHistory.markers.remove(8L);
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(missingHistory, valid).scan());

    TestHistory completeHistory = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    TestProgress missingCheckpoint = validProgress(completeHistory);
    missingCheckpoint.checkpoint = null;
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(completeHistory, missingCheckpoint).scan());

    TestProgress missingParticipant = validProgress(completeHistory);
    missingParticipant.participantProgress.remove("account-asset");
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(completeHistory, missingParticipant).scan());

    TestProgress unexpectedParticipant = validProgress(completeHistory);
    unexpectedParticipant.participantProgress.put("storage-row",
        unexpectedParticipant.participantProgress.get("account"));
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(completeHistory, unexpectedParticipant).scan());

    TestProgress missingReader = validProgress(completeHistory);
    missingReader.readerVisible = null;
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(completeHistory, missingReader).scan());
  }

  @Test
  public void rejectsEveryEnvelopeIdentityMismatch() {
    TestHistory history = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    HistoryCommitMarker marker = history.markers.get(8L);
    List<ArchiveProgressEnvelope> mismatches = Arrays.asList(
        new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, "account-asset", 8,
            bytes(32, 90), marker.getBatchId(), marker.getHistoryLocation().getBodyDigest(),
            PARTICIPANTS),
        new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, "account-asset", 8,
            marker.getMeta().getBlockHash(), bytes(16, 91),
            marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS),
        new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, "account-asset", 8,
            marker.getMeta().getBlockHash(), marker.getBatchId(), bytes(32, 92), PARTICIPANTS),
        new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, "account", 8,
            marker.getMeta().getBlockHash(), marker.getBatchId(),
            marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS),
        new ArchiveProgressEnvelope(Kind.APPLY_CHECKPOINT, null, 8,
            marker.getMeta().getBlockHash(), marker.getBatchId(),
            marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS),
        new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, "account-asset", 8,
            marker.getMeta().getBlockHash(), marker.getBatchId(),
            marker.getHistoryLocation().getBodyDigest(),
            Arrays.asList("account", "account-asset", "storage-row")));

    for (ArchiveProgressEnvelope mismatch : mismatches) {
      TestProgress progress = validProgress(history);
      progress.participantProgress.put("account-asset", mismatch);
      assertThrows(ArchivePersistenceException.class, () -> scanner(history, progress).scan());
    }
  }

  @Test
  public void rejectsCommittedMarkerEpochOrParticipantSetMismatch() {
    TestHistory wrongEpoch = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    TestProgress epochProgress = validProgress(wrongEpoch);
    wrongEpoch.markers.put(8L, marker(7, PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(wrongEpoch, epochProgress).scan());

    TestHistory wrongSet = history(marker(8, PARTICIPANTS), marker(10, PARTICIPANTS));
    TestProgress setProgress = validProgress(wrongSet);
    wrongSet.markers.put(8L,
        marker(8, Arrays.asList("account", "account-asset", "storage-row")));
    assertThrows(ArchivePersistenceException.class,
        () -> scanner(wrongSet, setProgress).scan());
  }

  private static ArchiveRecoveryScanner scanner(TestHistory history, TestProgress progress) {
    return new ArchiveRecoveryScanner(history, progress, PARTICIPANTS);
  }

  private static TestProgress validProgress(TestHistory history) {
    TestProgress progress = progress(envelope(Kind.APPLY_CHECKPOINT, null,
        history.markers.get(10L)), envelope(Kind.READER_VISIBLE, null,
        history.markers.get(8L)));
    progress.participantProgress.put("account",
        envelope(Kind.PARTICIPANT_PROGRESS, "account", history.markers.get(10L)));
    progress.participantProgress.put("account-asset",
        envelope(Kind.PARTICIPANT_PROGRESS, "account-asset", history.markers.get(8L)));
    return progress;
  }

  private static TestHistory history(HistoryCommitMarker... markers) {
    TestHistory history = new TestHistory();
    for (HistoryCommitMarker marker : markers) {
      history.markers.put(marker.getMeta().getEpoch(), marker);
    }
    return history;
  }

  private static TestProgress progress(ArchiveProgressEnvelope checkpoint,
      ArchiveProgressEnvelope readerVisible) {
    TestProgress progress = new TestProgress();
    progress.checkpoint = checkpoint;
    progress.readerVisible = readerVisible;
    return progress;
  }

  private static ArchiveProgressEnvelope envelope(Kind kind, String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), marker.getDatabases());
  }

  private static HistoryCommitMarker marker(long epoch, List<String> participants) {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, bytes(32, (int) epoch),
        bytes(32, (int) epoch - 1), epoch * 1_000);
    HistoryLocation body = new HistoryLocation(0, epoch * 100, 100, (int) epoch,
        bytes(32, (int) epoch + 20));
    HistoryIndexLocation index = new HistoryIndexLocation(epoch * 50, 50,
        bytes(32, (int) epoch + 30));
    return new HistoryCommitMarker(meta, epoch - 1, body, index,
        bytes(16, (int) epoch + 40), participants);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static void assertAction(RecoveryAction action, ActionType type, String participant,
      long firstEpoch, long lastEpoch) {
    assertEquals(type, action.getType());
    assertEquals(participant, action.getParticipant());
    assertEquals(firstEpoch, action.getFirstEpoch());
    assertEquals(lastEpoch, action.getLastEpoch());
  }

  private static final class TestHistory implements ArchiveRecoveryScanner.HistoryIdentitySource {
    private final Map<Long, HistoryCommitMarker> markers = new LinkedHashMap<>();

    @Override
    public long committedHeadEpoch() {
      return 12;
    }

    @Override
    public HistoryCommitMarker committedMarker(long epoch) {
      return markers.get(epoch);
    }
  }

  private static final class TestProgress implements ArchiveRecoveryScanner.ProgressIdentitySource {
    private ArchiveProgressEnvelope checkpoint;
    private final Map<String, ArchiveProgressEnvelope> participantProgress =
        new LinkedHashMap<>();
    private ArchiveProgressEnvelope readerVisible;

    @Override
    public ArchiveProgressEnvelope loadCheckpoint() {
      return checkpoint;
    }

    @Override
    public Map<String, ArchiveProgressEnvelope> loadParticipantProgress() {
      return participantProgress;
    }

    @Override
    public ArchiveProgressEnvelope loadReaderVisible() {
      return readerVisible;
    }
  }
}
