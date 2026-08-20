package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

public class ArchiveParticipantRecoveryStorageTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void secondRestartFinishesOnlyRemainingMixedEngineParticipant() throws Exception {
    Path archive = temporaryFolder.newFolder("mixed-native-recovery").toPath();
    Path checkpointPath = archive.resolve("progress/checkpoint.progress");
    Path readerPath = archive.resolve("progress/reader.progress");
    List<HistoryCommitMarker> markers = initializeHistory(archive, 3);
    new ArchiveProgressFile(checkpointPath, new ArchiveProgressEnvelopeCodec())
        .store(global(Kind.APPLY_CHECKPOINT, markers.get(1)));
    new ArchiveProgressFile(readerPath, new ArchiveProgressEnvelopeCodec())
        .store(global(Kind.READER_VISIBLE, markers.get(0)));

    try (LevelDbArchiveParticipant account = new LevelDbArchiveParticipant(
        archive.resolve("participants/account"), "account", PARTICIPANTS);
        RocksDbArchiveParticipant asset = new RocksDbArchiveParticipant(
            archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS)) {
      account.apply(Collections.emptyList(), participant("account", markers.get(0)));
      asset.apply(Collections.emptyList(), participant("account-asset", markers.get(0)));
      Map<String, ArchiveParticipant> engines = engines(account, asset);
      ArchiveTargetMutationPlan activePlan = storePlan(checkpointPath, markers.get(1));
      new ArchiveProgressFile(checkpointPath, new ArchiveProgressEnvelopeCodec())
          .store(global(Kind.APPLY_CHECKPOINT, markers.get(1), activePlan.digest()));

      try (ArchiveParticipantRecoveryStorage first = new ArchiveParticipantRecoveryStorage(
          archive, 4096, checkpointPath, failingEngines(account, asset), readerPath,
          PARTICIPANTS)) {
        assertThrows(ArchivePersistenceException.class,
            () -> new ArchiveRecoveryExecutor(first).recover());
      }

      assertEquals(2, account.loadProgress().getEpoch());
      assertArrayEquals(bytes("account:2-2"), account.get(bytes("replayed")));
      assertEquals(1, asset.loadProgress().getEpoch());
      assertNull(asset.get(bytes("replayed")));
      assertEquals(1, reader(readerPath).getEpoch());
      assertEquals(2, ArchiveRestartCheckpoint.load(archive,
          new HistoryCommitMarkerCodec()).getMarker().getMeta().getEpoch());
      assertFalse(Files.exists(archive.resolve("truncation.intent")));

      ArchiveTargetMutationPlanFile planFile = new ArchiveTargetMutationPlanFile(checkpointPath);
      byte[] validPlan = Files.readAllBytes(planFile.getPath());
      Files.delete(planFile.getPath());
      assertRecoveryFails(archive, checkpointPath, engines, readerPath);
      Files.write(planFile.getPath(), validPlan);
      byte[] corruptPlan = Arrays.copyOf(validPlan, validPlan.length);
      corruptPlan[corruptPlan.length - 1] ^= 1;
      Files.write(planFile.getPath(), corruptPlan);
      assertRecoveryFails(archive, checkpointPath, engines, readerPath);
      storeSubstitutedPlan(checkpointPath, markers.get(1));
      assertRecoveryFails(archive, checkpointPath, engines, readerPath);
      storePlan(checkpointPath, markers.get(0));
      assertRecoveryFails(archive, checkpointPath, engines, readerPath);
      Files.write(planFile.getPath(), validPlan);
      account.apply(Collections.emptyList(), participant("account", markers.get(1),
          bytes(32, 99)));
      assertRecoveryFails(archive, checkpointPath, engines, readerPath);
      account.apply(Collections.emptyList(), participant("account", markers.get(1),
          activePlan.digest()));

      try (ArchiveParticipantRecoveryStorage second = new ArchiveParticipantRecoveryStorage(
          archive, 4096, checkpointPath, engines, readerPath, PARTICIPANTS)) {
        assertEquals(2, new ArchiveRecoveryExecutor(second).recover().getActions().size());
      }

      assertEquals(2, account.loadProgress().getEpoch());
      assertEquals(2, asset.loadProgress().getEpoch());
      assertArrayEquals(bytes("account-asset:2-2"), asset.get(bytes("replayed")));
      assertEquals(2, reader(readerPath).getEpoch());
      assertFalse(Files.exists(planFile.getPath()));

      try (ArchiveParticipantRecoveryStorage third = new ArchiveParticipantRecoveryStorage(
          archive, 4096, checkpointPath, engines, readerPath, PARTICIPANTS)) {
        assertEquals(0, new ArchiveRecoveryExecutor(third).recover().getActions().size());
      }
    }
  }

  private static List<ArchiveParticipantMutation> mutation(String participant, long firstEpoch,
      long lastEpoch) {
    return Collections.singletonList(ArchiveParticipantMutation.put(bytes("replayed"),
        bytes(participant + ":" + firstEpoch + "-" + lastEpoch)));
  }

  private static ArchiveTargetMutationPlan storePlan(Path checkpointPath,
      HistoryCommitMarker marker)
      throws IOException {
    Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
    mutations.put("account", mutation("account", 2, 2));
    mutations.put("account-asset", mutation("account-asset", 2, 2));
    ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlan(
        global(Kind.APPLY_CHECKPOINT, marker), P66AccountAssetCodec.FORMAT_ID,
        Phase.P66_ON, mutations);
    new ArchiveTargetMutationPlanFile(checkpointPath).store(plan);
    return plan;
  }

  private static void storeSubstitutedPlan(Path checkpointPath, HistoryCommitMarker marker)
      throws IOException {
    Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
    mutations.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes("replayed"), bytes("substituted-account"))));
    mutations.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes("replayed"), bytes("substituted-asset"))));
    new ArchiveTargetMutationPlanFile(checkpointPath).store(new ArchiveTargetMutationPlan(
        global(Kind.APPLY_CHECKPOINT, marker), P66AccountAssetCodec.FORMAT_ID,
        Phase.P66_ON, mutations));
  }

  private static void assertRecoveryFails(Path archive, Path checkpointPath,
      Map<String, ArchiveParticipant> engines, Path readerPath) throws IOException {
    try (ArchiveParticipantRecoveryStorage storage = new ArchiveParticipantRecoveryStorage(
        archive, 4096, checkpointPath, engines, readerPath, PARTICIPANTS)) {
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(storage).recover());
    }
  }

  private static Map<String, ArchiveParticipant> failingEngines(
      ArchiveParticipant account, ArchiveParticipant asset) {
    Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();
    engines.put("account", account);
    engines.put("account-asset", new ArchiveParticipant() {
      @Override
      public void apply(List<ArchiveParticipantMutation> mutations,
          ArchiveProgressEnvelope progress) throws IOException {
        throw new IOException("injected second participant replay failure");
      }

      @Override
      public ArchiveProgressEnvelope loadProgress() throws IOException {
        return asset.loadProgress();
      }
    });
    return engines;
  }

  private static ArchiveProgressEnvelope reader(Path readerPath) throws IOException {
    return new ArchiveProgressFile(readerPath, new ArchiveProgressEnvelopeCodec()).load();
  }

  private static List<HistoryCommitMarker> initializeHistory(Path archive, int lastEpoch)
      throws Exception {
    List<HistoryCommitMarker> markers = new ArrayList<>();
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      for (int epoch = 1; epoch <= lastEpoch; epoch++) {
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

  private static Map<String, ArchiveParticipant> engines(
      ArchiveParticipant account, ArchiveParticipant asset) {
    Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();
    engines.put("account", account);
    engines.put("account-asset", asset);
    return engines;
  }

  private static ArchiveProgressEnvelope participant(String name,
      HistoryCommitMarker marker) {
    return participant(name, marker, null);
  }

  private static ArchiveProgressEnvelope participant(String name,
      HistoryCommitMarker marker, byte[] mutationPlanDigest) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, name,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), mutationPlanDigest, PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return global(kind, marker, null);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker,
      byte[] mutationPlanDigest) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), mutationPlanDigest, PARTICIPANTS);
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
