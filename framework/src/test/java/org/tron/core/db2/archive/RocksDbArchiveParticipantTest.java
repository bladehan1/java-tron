package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.RocksDbArchiveParticipant.Stage;

public class RocksDbArchiveParticipantTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void nativeBatchExposesOnlyOldOldOrNewNewAcrossFailureBoundaries() throws Exception {
    for (Stage failedStage : Stage.values()) {
      Path directory = temporaryFolder.newFolder("native-" + failedStage).toPath();
      try (RocksDbArchiveParticipant participant = new RocksDbArchiveParticipant(
          directory, "account", PARTICIPANTS)) {
        participant.apply(Collections.singletonList(
            ArchiveParticipantMutation.put(bytes("key"), bytes("old"))),
            progress(1));
      }

      try (RocksDbArchiveParticipant failing = new RocksDbArchiveParticipant(
          directory, "account", PARTICIPANTS, stage -> failAt(failedStage, stage))) {
        assertThrows(IOException.class, () -> failing.apply(
            Collections.singletonList(
                ArchiveParticipantMutation.put(bytes("key"), bytes("new"))), progress(2)));
      }

      try (RocksDbArchiveParticipant reopened = new RocksDbArchiveParticipant(
          directory, "account", PARTICIPANTS)) {
        long expectedEpoch = failedStage == Stage.BEFORE_WRITE ? 1 : 2;
        byte[] expectedValue = failedStage == Stage.BEFORE_WRITE ? bytes("old") : bytes("new");
        assertEquals(expectedEpoch, reopened.loadProgress().getEpoch());
        assertArrayEquals(expectedValue, reopened.get(bytes("key")));
      }
    }
  }

  @Test
  public void deleteAndProgressShareTheSameNativeBatch() throws Exception {
    Path directory = temporaryFolder.newFolder("native-delete").toPath();
    try (RocksDbArchiveParticipant participant = new RocksDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      participant.apply(Collections.singletonList(
          ArchiveParticipantMutation.put(bytes("key"), bytes("value"))),
          progress(1));
      participant.apply(Collections.singletonList(
          ArchiveParticipantMutation.delete(bytes("key"))), progress(2));
      assertNull(participant.get(bytes("key")));
      assertEquals(2, participant.loadProgress().getEpoch());
    }
  }

  @Test
  public void rejectsProgressForAnotherParticipantBeforeNativeWrite() throws Exception {
    Path directory = temporaryFolder.newFolder("native-identity").toPath();
    try (RocksDbArchiveParticipant participant = new RocksDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      ArchiveProgressEnvelope wrong = new ArchiveProgressEnvelope(
          ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, "account-asset", 1,
          bytes(32, 1), bytes(16, 2), bytes(32, 3), PARTICIPANTS);
      assertThrows(IllegalArgumentException.class, () -> participant.apply(
          Collections.singletonList(
              ArchiveParticipantMutation.put(bytes("key"), bytes("value"))), wrong));
      assertNull(participant.get(bytes("key")));
      assertThrows(ArchivePersistenceException.class, participant::loadProgress);
    }
  }

  @Test
  public void recoveryScannerReadsParticipantProgressFromNativeEngines() throws Exception {
    Path directory = temporaryFolder.newFolder("native-scanner").toPath();
    Path checkpointPath = directory.resolve("progress/checkpoint.progress");
    Path readerPath = directory.resolve("progress/reader.progress");
    try (HistoryCommitStore history = new HistoryCommitStore(directory,
        new HistoryCommitMarkerCodec());
        RocksDbArchiveParticipant account = new RocksDbArchiveParticipant(
            directory.resolve("account-engine"), "account", PARTICIPANTS);
        RocksDbArchiveParticipant asset = new RocksDbArchiveParticipant(
            directory.resolve("asset-engine"), "account-asset", PARTICIPANTS)) {
      HistoryCommitMarker first = marker(1);
      HistoryCommitMarker second = marker(2);
      history.commitAll(Arrays.asList(first, second));
      new ArchiveProgressFile(checkpointPath, new ArchiveProgressEnvelopeCodec())
          .store(globalProgress(ArchiveProgressEnvelope.Kind.APPLY_CHECKPOINT, second));
      new ArchiveProgressFile(readerPath, new ArchiveProgressEnvelopeCodec())
          .store(globalProgress(ArchiveProgressEnvelope.Kind.READER_VISIBLE, first));
      account.apply(Collections.emptyList(), participantProgress("account", second));
      asset.apply(Collections.emptyList(), participantProgress("account-asset", first));
      Map<String, RocksDbArchiveParticipant> engines = new LinkedHashMap<>();
      engines.put("account", account);
      engines.put("account-asset", asset);

      ArchiveRecoveryExecutor.RecoverySnapshot snapshot =
          ArchiveRecoveryAuthorityScanner.forParticipants(history, checkpointPath,
              engines, readerPath, PARTICIPANTS).scan();
      assertEquals(2, snapshot.getHistoryHead());
      assertEquals(2, snapshot.getCheckpointHead());
      assertEquals(Long.valueOf(2), snapshot.getParticipantHeads().get("account"));
      assertEquals(Long.valueOf(1), snapshot.getParticipantHeads().get("account-asset"));
      assertEquals(1, snapshot.getReaderVisibleHead());
    }
  }

  private static ArchiveProgressEnvelope progress(long epoch) {
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS,
        "account", epoch, bytes(32, (int) epoch), bytes(16, (int) epoch + 10),
        bytes(32, (int) epoch + 20), PARTICIPANTS);
  }

  private static HistoryCommitMarker marker(long epoch) {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, bytes(32, (int) epoch),
        bytes(32, (int) epoch - 1), epoch * 1_000);
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100, 80, (int) epoch, bytes(32, (int) epoch + 20)),
        new HistoryIndexLocation(epoch * 50, 50, bytes(32, (int) epoch + 30)),
        bytes(16, (int) epoch + 40), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope participantProgress(String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS,
        participant, marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(),
        marker.getBatchId(), marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope globalProgress(ArchiveProgressEnvelope.Kind kind,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static void failAt(Stage failedStage, Stage currentStage) throws IOException {
    if (currentStage == failedStage) {
      throw new IOException("injected at " + currentStage);
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
