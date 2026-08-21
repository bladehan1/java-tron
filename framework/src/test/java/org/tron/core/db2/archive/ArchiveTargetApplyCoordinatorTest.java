package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.ActionType;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;
import org.tron.core.db2.archive.ArchiveTargetApplyCoordinator.Stage;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;

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

  @Test
  public void p66PlansRecoverOnlyRemainingNativeParticipantAfterFreshReopen() throws Exception {
    for (Phase phase : Arrays.asList(Phase.P66_ACTIVATION, Phase.P66_ON)) {
      for (boolean failAfterAccount : Arrays.asList(false, true)) {
        String boundary = failAfterAccount ? "after-account" : "after-checkpoint";
        try (Fixture fixture = fixture("p66-" + phase.name().toLowerCase() + "-" + boundary)) {
          byte[] address = accountAddress(7);
          byte[] accountValue = canonicalAccount(address,
              phase == Phase.P66_ACTIVATION ? 2_000L : 3_000L);
          byte[] assetKey = new P66AccountAssetCodec().assetPhysicalKey(address, "1000007");
          byte[] assetValue = ByteBuffer.allocate(Long.BYTES)
              .putLong(phase == Phase.P66_ACTIVATION ? 30L : 40L).array();
          Map<String, List<ArchiveParticipantMutation>> mutations =
              p66Mutations(address, accountValue, assetKey, assetValue);
          ArchiveTargetApplyCoordinator.FaultHook failure = (stage, participant) -> {
            if (!failAfterAccount && stage == Stage.AFTER_CHECKPOINT
                || failAfterAccount && stage == Stage.AFTER_PARTICIPANT
                && "account".equals(participant)) {
              throw new IOException("injected " + boundary);
            }
          };

          try (HistoryCommitStore history = fixture.openHistory()) {
            ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
                action -> action.run(), failure, temporary -> { });
            assertThrows(IOException.class,
                () -> coordinator.apply(1, phase, mutations, () -> { }));
          }

          ArchiveTargetMutationPlan durablePlan =
              new ArchiveTargetMutationPlanFile(fixture.checkpointPath).loadRequired();
          byte[] planDigest = durablePlan.digest();
          assertEquals(phase, durablePlan.getTargetPhase());
          assertArrayEquals(hash(1), durablePlan.getTarget().getBlockHash());
          assertArrayEquals(planDigest, fixture.checkpoint().getMutationPlanDigest());

          fixture.reopenParticipants();
          AtomicInteger refreshes = new AtomicInteger();
          RecoveryPlan recoveryPlan;
          try (ArchiveParticipantRecoveryStorage recovery =
              new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                  fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
                  action -> action.run(), refreshes::incrementAndGet)) {
            recoveryPlan = new ArchiveRecoveryExecutor(recovery).recover();
          }

          List<String> replayed = new ArrayList<>();
          recoveryPlan.getActions().stream()
              .filter(action -> action.getType() == ActionType.REPLAY_PARTICIPANT)
              .forEach(action -> replayed.add(action.getParticipant()));
          assertEquals(failAfterAccount
              ? Collections.singletonList("account-asset") : PARTICIPANTS, replayed);
          assertEquals(ActionType.PUBLISH_READER_HEAD,
              recoveryPlan.getActions().get(recoveryPlan.getActions().size() - 1).getType());
          assertEquals(1, refreshes.get());
          assertArrayEquals(accountValue, fixture.account.get(address));
          assertArrayEquals(assetValue, fixture.asset.get(assetKey));
          assertP66Authority(fixture, planDigest);
          assertFalse(Files.exists(
              new ArchiveTargetMutationPlanFile(fixture.checkpointPath).getPath()));

          fixture.reopenParticipants();
          try (ArchiveParticipantRecoveryStorage fixed =
              new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                  fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
            assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
          }
          assertArrayEquals(accountValue, fixture.account.get(address));
          assertArrayEquals(assetValue, fixture.asset.get(assetKey));
          assertP66Authority(fixture, planDigest);
        }
      }
    }
  }

  @Test
  public void p66ReaderDurableCrashReopenRetiresPlanWithoutBusinessReplay() throws Exception {
    for (Phase phase : Arrays.asList(Phase.P66_ACTIVATION, Phase.P66_ON)) {
      try (Fixture fixture = fixture("p66-reader-durable-" + phase.name().toLowerCase())) {
        P66Vector vector = p66Vector(phase);
        try (HistoryCommitStore history = fixture.openHistory()) {
          ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
              fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
              action -> action.run(), failAt(Stage.AFTER_READER,
                  "injected after durable reader publication"), temporary -> { });
          assertThrows(IOException.class,
              () -> coordinator.apply(1, phase, vector.mutations, () -> { }));
        }

        ArchiveTargetMutationPlan durablePlan =
            new ArchiveTargetMutationPlanFile(fixture.checkpointPath).loadRequired();
        byte[] planDigest = durablePlan.digest();
        assertEquals(phase, durablePlan.getTargetPhase());
        assertP66BusinessAndAuthority(fixture, vector, planDigest);

        fixture.reopenParticipants();
        AtomicInteger refreshes = new AtomicInteger();
        try (ArchiveParticipantRecoveryStorage recovery =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
                action -> action.run(), refreshes::incrementAndGet)) {
          assertEquals(0, new ArchiveRecoveryExecutor(recovery).recover().getActions().size());
        }
        assertEquals(0, refreshes.get());
        assertFalse(Files.exists(
            new ArchiveTargetMutationPlanFile(fixture.checkpointPath).getPath()));
        assertP66BusinessAndAuthority(fixture, vector, planDigest);

        fixture.reopenParticipants();
        try (ArchiveParticipantRecoveryStorage fixed =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
          assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
        }
        assertP66BusinessAndAuthority(fixture, vector, planDigest);
      }
    }
  }

  @Test
  public void p66RecoveryCrashReopenReplaysOnlySecondNativeParticipant() throws Exception {
    for (Phase phase : Arrays.asList(Phase.P66_ACTIVATION, Phase.P66_ON)) {
      try (Fixture fixture = fixture("p66-recovery-crash-" + phase.name().toLowerCase())) {
        P66Vector vector = p66Vector(phase);
        try (HistoryCommitStore history = fixture.openHistory()) {
          ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
              fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS,
              action -> action.run(), failAt(Stage.AFTER_CHECKPOINT,
                  "injected after checkpoint"), temporary -> { });
          assertThrows(IOException.class,
              () -> coordinator.apply(1, phase, vector.mutations, () -> { }));
        }
        ArchiveTargetMutationPlan durablePlan =
            new ArchiveTargetMutationPlanFile(fixture.checkpointPath).loadRequired();
        byte[] planDigest = durablePlan.digest();
        assertEquals(phase, durablePlan.getTargetPhase());

        fixture.reopenParticipants();
        try (ArchiveParticipantRecoveryStorage recovery =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
          assertThrows(ArchivePersistenceException.class,
              () -> new ArchiveRecoveryExecutor(recovery, action -> {
                if (action.getType() == ActionType.REPLAY_PARTICIPANT
                    && "account".equals(action.getParticipant())) {
                  throw new IOException("injected after recovered account");
                }
              }).recover());
        }
        assertArrayEquals(vector.accountValue, fixture.account.get(vector.address));
        assertNull(fixture.asset.get(vector.assetKey));
        assertEquals(1L, fixture.account.loadProgress().getEpoch());
        assertEquals(0L, fixture.asset.loadProgress().getEpoch());
        assertEquals(0L, fixture.reader().getEpoch());
        assertArrayEquals(planDigest,
            fixture.account.loadProgress().getMutationPlanDigest());

        fixture.reopenParticipants();
        RecoveryPlan recoveryPlan;
        try (ArchiveParticipantRecoveryStorage recovery =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
          recoveryPlan = new ArchiveRecoveryExecutor(recovery).recover();
        }
        assertEquals(2, recoveryPlan.getActions().size());
        assertEquals(ActionType.REPLAY_PARTICIPANT,
            recoveryPlan.getActions().get(0).getType());
        assertEquals("account-asset", recoveryPlan.getActions().get(0).getParticipant());
        assertEquals(ActionType.PUBLISH_READER_HEAD,
            recoveryPlan.getActions().get(1).getType());
        assertP66BusinessAndAuthority(fixture, vector, planDigest);
        assertFalse(Files.exists(
            new ArchiveTargetMutationPlanFile(fixture.checkpointPath).getPath()));

        fixture.reopenParticipants();
        try (ArchiveParticipantRecoveryStorage fixed =
            new ArchiveParticipantRecoveryStorage(fixture.archive, 4096,
                fixture.checkpointPath, fixture.engines(), fixture.readerPath, PARTICIPANTS)) {
          assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
        }
        assertP66BusinessAndAuthority(fixture, vector, planDigest);
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

  private static Map<String, List<ArchiveParticipantMutation>> p66Mutations(byte[] address,
      byte[] accountValue, byte[] assetKey, byte[] assetValue) {
    Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
    mutations.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(address, accountValue)));
    mutations.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.put(assetKey, assetValue)));
    return mutations;
  }

  private static byte[] accountAddress(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] canonicalAccount(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setAssetOptimized(true).setBalance(balance).build().toByteArray();
  }

  private static P66Vector p66Vector(Phase phase) {
    byte[] address = accountAddress(7);
    byte[] accountValue = canonicalAccount(address,
        phase == Phase.P66_ACTIVATION ? 2_000L : 3_000L);
    byte[] assetKey = new P66AccountAssetCodec().assetPhysicalKey(address, "1000007");
    byte[] assetValue = ByteBuffer.allocate(Long.BYTES)
        .putLong(phase == Phase.P66_ACTIVATION ? 30L : 40L).array();
    return new P66Vector(address, accountValue, assetKey, assetValue,
        p66Mutations(address, accountValue, assetKey, assetValue));
  }

  private static void assertP66BusinessAndAuthority(Fixture fixture, P66Vector vector,
      byte[] planDigest) throws IOException {
    assertArrayEquals(vector.accountValue, fixture.account.get(vector.address));
    assertArrayEquals(vector.assetValue, fixture.asset.get(vector.assetKey));
    assertP66Authority(fixture, planDigest);
  }

  private static void assertP66Authority(Fixture fixture, byte[] planDigest) throws IOException {
    ArchiveProgressEnvelope checkpoint = fixture.checkpoint();
    ArchiveProgressEnvelope reader = fixture.reader();
    assertEquals(1L, checkpoint.getEpoch());
    assertEquals(1L, reader.getEpoch());
    assertArrayEquals(hash(1), checkpoint.getBlockHash());
    assertArrayEquals(hash(1), reader.getBlockHash());
    assertArrayEquals(planDigest, checkpoint.getMutationPlanDigest());
    assertArrayEquals(planDigest, fixture.account.loadProgress().getMutationPlanDigest());
    assertArrayEquals(planDigest, fixture.asset.loadProgress().getMutationPlanDigest());
    assertArrayEquals(planDigest, reader.getMutationPlanDigest());
  }

  private static ArchiveTargetApplyCoordinator.FaultHook failAt(Stage expected,
      String message) {
    return (stage, participant) -> {
      if (stage == expected) {
        throw new IOException(message);
      }
    };
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

  private static final class P66Vector {
    private final byte[] address;
    private final byte[] accountValue;
    private final byte[] assetKey;
    private final byte[] assetValue;
    private final Map<String, List<ArchiveParticipantMutation>> mutations;

    private P66Vector(byte[] address, byte[] accountValue, byte[] assetKey, byte[] assetValue,
        Map<String, List<ArchiveParticipantMutation>> mutations) {
      this.address = address;
      this.accountValue = accountValue;
      this.assetKey = assetKey;
      this.assetValue = assetValue;
      this.mutations = mutations;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Path archive;
    private final Path checkpointPath;
    private final Path readerPath;
    private LevelDbArchiveParticipant account;
    private RocksDbArchiveParticipant asset;
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
      openParticipants();
      account.apply(Collections.emptyList(), participant("account", markers.get(0)));
      asset.apply(Collections.emptyList(), participant("account-asset", markers.get(0)));
    }

    private void reopenParticipants() throws IOException {
      closeParticipants();
      openParticipants();
    }

    private void openParticipants() throws IOException {
      account = new LevelDbArchiveParticipant(
          archive.resolve("participants/account"), "account", PARTICIPANTS);
      try {
        asset = new RocksDbArchiveParticipant(
            archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS);
      } catch (IOException | RuntimeException failure) {
        account.close();
        throw failure;
      }
    }

    private void closeParticipants() throws IOException {
      asset.close();
      account.close();
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
      closeParticipants();
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
