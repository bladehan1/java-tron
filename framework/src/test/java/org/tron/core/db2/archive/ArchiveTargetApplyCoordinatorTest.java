package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveTargetApplyCoordinator.Stage;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

public class ArchiveTargetApplyCoordinatorTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void appliesCheckpointParticipantsRefreshAndReaderInOrder() throws Exception {
    try (Fixture fixture = fixture("normal")) {
      AtomicBoolean insideBarrier = new AtomicBoolean();
      ArchiveStateBarrier barrier = action -> {
        assertTrue(insideBarrier.compareAndSet(false, true));
        try {
          action.run();
        } finally {
          insideBarrier.set(false);
        }
      };
      try (HistoryCommitStore history = fixture.openHistory()) {
        ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
            fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS, barrier);
        coordinator.apply(1, Phase.P66_ON, plans(), () -> {
          assertTrue(insideBarrier.get());
          assertEquals(1, fixture.account.loadProgress().getEpoch());
          assertEquals(1, fixture.asset.loadProgress().getEpoch());
        });
      }

      ArchiveProgressEnvelope checkpoint = fixture.checkpoint();
      ArchiveProgressEnvelope reader = fixture.reader();
      byte[] mutationPlanDigest = checkpoint.getMutationPlanDigest();
      assertEquals(1, checkpoint.getEpoch());
      assertEquals(1, reader.getEpoch());
      assertTrue(mutationPlanDigest != null);
      assertArrayEquals(mutationPlanDigest,
          fixture.account.loadProgress().getMutationPlanDigest());
      assertArrayEquals(mutationPlanDigest,
          fixture.asset.loadProgress().getMutationPlanDigest());
      assertArrayEquals(mutationPlanDigest, reader.getMutationPlanDigest());
      assertArrayEquals(bytes("account"), fixture.account.get(bytes("normal")));
      assertArrayEquals(bytes("account-asset"), fixture.asset.get(bytes("normal")));
      assertFalse(Files.exists(
          new ArchiveTargetMutationPlanFile(fixture.checkpointPath).getPath()));
    }
  }

  @Test
  public void everyDurableStageFailureConvergesThroughFreshRecovery() throws Exception {
    for (FailurePoint point : FailurePoint.values()) {
      try (Fixture fixture = fixture(point.name().toLowerCase())) {
        AtomicInteger refreshes = new AtomicInteger();
        try (HistoryCommitStore history = fixture.openHistory()) {
          ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
              fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
              action -> action.run(),
              (stage, participant) -> failAfterStage(point, stage, participant), temporary -> {
            if (point == FailurePoint.DURING_PUBLICATION) {
              throw new IOException("injected during publication");
            }
          }, (stage, path) -> failPlanStage(point, stage));
          assertThrows(IOException.class, () -> coordinator.apply(1, Phase.P66_ON, plans(), () -> {
            refreshes.incrementAndGet();
            if (point == FailurePoint.DURING_REFRESH) {
              throw new IOException("injected during refresh");
            }
          }));
        }

        try (ArchiveParticipantRecoveryStorage recovery =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
                action -> action.run(), refreshes::incrementAndGet)) {
          new ArchiveRecoveryExecutor(recovery).recover();
        }

        long expected = point.isPlanFailure() ? 0 : 1;
        assertEquals(expected, fixture.checkpoint().getEpoch());
        assertEquals(expected, fixture.account.loadProgress().getEpoch());
        assertEquals(expected, fixture.asset.loadProgress().getEpoch());
        assertEquals(expected, fixture.reader().getEpoch());
        assertEquals(point.isPlanFailure() ? 0 : 1,
            Math.min(refreshes.get(), 1));
        assertFalse(Files.exists(
            new ArchiveTargetMutationPlanFile(fixture.checkpointPath).getPath()));

        try (ArchiveParticipantRecoveryStorage fixed =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
          assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
        }
      }
    }
  }

  private Fixture fixture(String name) throws Exception {
    return new Fixture(temporaryFolder.newFolder(name).toPath());
  }

  private static Map<String, List<ArchiveParticipantMutation>> plans() {
    Map<String, List<ArchiveParticipantMutation>> plans = new LinkedHashMap<>();
    plans.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes("normal"), bytes("account"))));
    plans.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes("normal"), bytes("account-asset"))));
    return plans;
  }

  private static void failAfterStage(FailurePoint point, Stage stage, String participant)
      throws IOException {
    if (point == FailurePoint.AFTER_CHECKPOINT && stage == Stage.AFTER_CHECKPOINT
        || point == FailurePoint.AFTER_FIRST_PARTICIPANT
        && stage == Stage.AFTER_PARTICIPANT && "account".equals(participant)
        || point == FailurePoint.AFTER_READER && stage == Stage.AFTER_READER) {
      throw new IOException("injected at " + point);
    }
  }

  private static void failPlanStage(FailurePoint point,
      ArchiveTargetMutationPlanFile.Stage stage) throws IOException {
    if (point == FailurePoint.AFTER_PLAN_TEMPORARY_FORCE
        && stage == ArchiveTargetMutationPlanFile.Stage.AFTER_TEMPORARY_FORCE
        || point == FailurePoint.AFTER_PLAN_REPLACE
        && stage == ArchiveTargetMutationPlanFile.Stage.AFTER_REPLACE) {
      throw new IOException("injected at " + point);
    }
  }

  private enum FailurePoint {
    AFTER_PLAN_TEMPORARY_FORCE,
    AFTER_PLAN_REPLACE,
    AFTER_CHECKPOINT,
    AFTER_FIRST_PARTICIPANT,
    DURING_REFRESH,
    DURING_PUBLICATION,
    AFTER_READER;

    private boolean isPlanFailure() {
      return this == AFTER_PLAN_TEMPORARY_FORCE || this == AFTER_PLAN_REPLACE;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Path archive;
    private final Path checkpointPath;
    private final Path readerPath;
    private final LevelDbArchiveParticipant account;
    private final RocksDbArchiveParticipant asset;
    private final ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();

    private Fixture(Path archive) throws Exception {
      this.archive = archive;
      List<HistoryCommitMarker> markers = initializeHistory(archive);
      checkpointPath = archive.resolve("progress/checkpoint.progress");
      readerPath = archive.resolve("progress/reader.progress");
      new ArchiveProgressFile(checkpointPath, codec).store(
          global(Kind.APPLY_CHECKPOINT, markers.get(0)));
      new ArchiveProgressFile(readerPath, codec).store(
          global(Kind.READER_VISIBLE, markers.get(0)));
      account = new LevelDbArchiveParticipant(
          archive.resolve("participants/account"), "account", PARTICIPANTS);
      asset = new RocksDbArchiveParticipant(
          archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS);
      account.apply(Collections.emptyList(), participant("account", markers.get(0)));
      asset.apply(Collections.emptyList(), participant("account-asset", markers.get(0)));
    }

    private HistoryCommitStore openHistory() throws IOException {
      return new HistoryCommitStore(archive, new HistoryCommitMarkerCodec());
    }

    private Map<String, ArchiveParticipant> engines() {
      Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();
      engines.put("account", account);
      engines.put("account-asset", asset);
      return engines;
    }

    private ArchiveProgressEnvelope checkpoint() throws IOException {
      return new ArchiveProgressFile(checkpointPath, codec).load();
    }

    private ArchiveProgressEnvelope reader() throws IOException {
      return new ArchiveProgressFile(readerPath, codec).load();
    }

    @Override
    public void close() throws IOException {
      asset.close();
      account.close();
    }
  }

  private static List<HistoryCommitMarker> initializeHistory(Path archive) throws Exception {
    List<HistoryCommitMarker> markers = new ArrayList<>();
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      for (int epoch = 0; epoch <= 1; epoch++) {
        BlockReverseDiff diff = new BlockReverseDiff(
            new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L),
            Collections.singletonList(new BlockReverseDiff.DbGroup("account",
                Collections.singletonList(new BlockReverseDiff.Entry(bytes("key-" + epoch),
                    OldValue.present(bytes("old-" + epoch)))))));
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation location = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(diff.getMeta(), epoch - 1L, body, location,
            bytes(16, epoch + 40), PARTICIPANTS));
      }
      bodies.sync();
      index.sync();
      commits.commitAll(markers);
      ArchiveRestartCheckpoint.persist(archive, commits.firstEpoch(), commits.size(),
          commits.getRecordLength(), commits.head(), new HistoryCommitMarkerCodec());
    }
    return markers;
  }

  private static ArchiveProgressEnvelope participant(String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, participant,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
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
