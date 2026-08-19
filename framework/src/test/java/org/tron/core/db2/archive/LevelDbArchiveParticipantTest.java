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
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.LevelDbArchiveParticipant.Mutation;
import org.tron.core.db2.archive.LevelDbArchiveParticipant.Stage;

public class LevelDbArchiveParticipantTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void nativeBatchExposesOnlyOldOldOrNewNewAcrossFailureBoundaries() throws Exception {
    for (Stage failedStage : Stage.values()) {
      Path directory = temporaryFolder.newFolder("native-" + failedStage).toPath();
      try (LevelDbArchiveParticipant participant = new LevelDbArchiveParticipant(
          directory, "account", PARTICIPANTS)) {
        participant.apply(Collections.singletonList(Mutation.put(bytes("key"), bytes("old"))),
            progress(1));
      }

      try (LevelDbArchiveParticipant failing = new LevelDbArchiveParticipant(
          directory, "account", PARTICIPANTS, stage -> failAt(failedStage, stage))) {
        assertThrows(IOException.class, () -> failing.apply(
            Collections.singletonList(Mutation.put(bytes("key"), bytes("new"))), progress(2)));
      }

      try (LevelDbArchiveParticipant reopened = new LevelDbArchiveParticipant(
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
    try (LevelDbArchiveParticipant participant = new LevelDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      participant.apply(Collections.singletonList(Mutation.put(bytes("key"), bytes("value"))),
          progress(1));
      participant.apply(Collections.singletonList(Mutation.delete(bytes("key"))), progress(2));
      assertNull(participant.get(bytes("key")));
      assertEquals(2, participant.loadProgress().getEpoch());
    }
  }

  @Test
  public void resetClearsBusinessAndProgressBeforeAConsistentReapply() throws Exception {
    Path directory = temporaryFolder.newFolder("native-reset").toPath();
    try (LevelDbArchiveParticipant participant = new LevelDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      participant.apply(Collections.singletonList(Mutation.put(bytes("key"), bytes("old"))),
          progress(1));
      participant.reset();
      assertNull(participant.get(bytes("key")));
      assertThrows(ArchivePersistenceException.class, participant::loadProgress);

      participant.apply(Collections.singletonList(Mutation.put(bytes("key"), bytes("new"))),
          progress(2));
    }

    try (LevelDbArchiveParticipant reopened = new LevelDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      assertArrayEquals(bytes("new"), reopened.get(bytes("key")));
      assertEquals(2, reopened.loadProgress().getEpoch());
    }
  }

  @Test
  public void rejectsProgressForAnotherParticipantBeforeNativeWrite() throws Exception {
    Path directory = temporaryFolder.newFolder("native-identity").toPath();
    try (LevelDbArchiveParticipant participant = new LevelDbArchiveParticipant(
        directory, "account", PARTICIPANTS)) {
      ArchiveProgressEnvelope wrong = new ArchiveProgressEnvelope(
          ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, "account-asset", 1,
          bytes(32, 1), bytes(16, 2), bytes(32, 3), PARTICIPANTS);
      assertThrows(IllegalArgumentException.class, () -> participant.apply(
          Collections.singletonList(Mutation.put(bytes("key"), bytes("value"))), wrong));
      assertNull(participant.get(bytes("key")));
      assertThrows(ArchivePersistenceException.class, participant::loadProgress);
    }
  }

  private static ArchiveProgressEnvelope progress(long epoch) {
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS,
        "account", epoch, bytes(32, (int) epoch), bytes(16, (int) epoch + 10),
        bytes(32, (int) epoch + 20), PARTICIPANTS);
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
