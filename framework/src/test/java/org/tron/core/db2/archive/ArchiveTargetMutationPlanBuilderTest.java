package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

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
import org.tron.core.db2.archive.ArchiveParticipantMutationBatch.Mutation;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

public class ArchiveTargetMutationPlanBuilderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void canonicalizesExactPhysicalMutationsAndOwnsInputBytes() {
    HistoryCommitMarker target = marker(1, participants());
    byte[] key = bytes(3, 3);
    byte[] value = bytes(2, 7);
    ArchiveParticipantMutationBatch first = new ArchiveParticipantMutationBatch(target,
        Arrays.asList(Mutation.delete("storage-row", bytes(3, 2)),
            Mutation.put("account", key, value),
            Mutation.put("account", bytes(3, 1), new byte[0])));
    key[0] = 99;
    value[0] = 99;
    ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(target, first);

    assertArrayEquals(bytes(3, 1), plan.getMutations("account").get(0).getKey());
    assertArrayEquals(new byte[0], plan.getMutations("account").get(0).getValue());
    assertArrayEquals(bytes(3, 3), plan.getMutations("account").get(1).getKey());
    assertArrayEquals(bytes(2, 7), plan.getMutations("account").get(1).getValue());
    assertNull(plan.getMutations("storage-row").get(0).getValue());
    assertEquals(participants(), new ArrayList<>(plan.getMutations().keySet()));

    ArchiveParticipantMutationBatch reordered = new ArchiveParticipantMutationBatch(target,
        Arrays.asList(Mutation.put("account", bytes(3, 1), new byte[0]),
            Mutation.put("account", bytes(3, 3), bytes(2, 7)),
            Mutation.delete("storage-row", bytes(3, 2))));
    assertArrayEquals(plan.digest(),
        new ArchiveTargetMutationPlanBuilder().build(target, reordered).digest());
  }

  @Test
  public void rejectsUnknownDerivedAndDuplicatePhysicalKeys() {
    HistoryCommitMarker target = marker(1, participants());
    assertBuildFails(target, Collections.singletonList(
        Mutation.put("unknown-db", bytes(1, 1), bytes(1, 2))));
    assertBuildFails(target, Collections.singletonList(
        Mutation.delete("accountTrie", bytes(1, 1))));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(target,
            new ArchiveParticipantMutationBatch(target, Arrays.asList(
                Mutation.put("account", bytes(1, 1), bytes(1, 2)),
                Mutation.delete("account", bytes(1, 1))))));
  }

  @Test
  public void rejectsTargetIdentityAndExactParticipantSetMismatch() {
    HistoryCommitMarker target = marker(1, participants());
    ArchiveParticipantMutationBatch batch = new ArchiveParticipantMutationBatch(target,
        Collections.emptyList());
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(marker(2, participants()), batch));

    List<String> incomplete = Arrays.asList("account", "account-asset");
    HistoryCommitMarker incompleteTarget = marker(1, incomplete);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(incompleteTarget,
            new ArchiveParticipantMutationBatch(incompleteTarget, Collections.emptyList())));

    List<String> oldExact27 = new ArrayList<>(participants());
    oldExact27.add("abi");
    Collections.sort(oldExact27);
    HistoryCommitMarker oldTarget = marker(1, oldExact27);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(oldTarget,
            new ArchiveParticipantMutationBatch(oldTarget, Collections.emptyList())));

    List<String> v2OnlyExact25 = new ArrayList<>(participants());
    v2OnlyExact25.remove("asset-issue");
    HistoryCommitMarker v2OnlyTarget = marker(1, v2OnlyExact25);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(v2OnlyTarget,
            new ArchiveParticipantMutationBatch(v2OnlyTarget, Collections.emptyList())));
  }

  @Test
  public void coordinatorConsumesImmutableBatchAndPublishesExactDigest() throws Exception {
    Path archive = temporaryFolder.newFolder("coordinator-producer").toPath();
    List<String> participants = participants();
    HistoryCommitMarker zero = marker(0, participants);
    HistoryCommitMarker one = marker(1, participants);
    Path checkpointPath = archive.resolve("progress/checkpoint.progress");
    Path readerPath = archive.resolve("progress/reader.progress");
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(checkpointPath, codec).store(global(Kind.APPLY_CHECKPOINT, zero));
    new ArchiveProgressFile(readerPath, codec).store(global(Kind.READER_VISIBLE, zero));
    Map<String, RecordingParticipant> recording = new LinkedHashMap<>();
    Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();
    for (String participant : participants) {
      RecordingParticipant engine = new RecordingParticipant(
          progress(participant, zero));
      recording.put(participant, engine);
      engines.put(participant, engine);
    }
    try (HistoryCommitStore history = new HistoryCommitStore(
        archive, new HistoryCommitMarkerCodec())) {
      history.commitAll(Arrays.asList(zero, one));
      ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
          checkpointPath, engines, readerPath, participants, action -> action.run());
      coordinator.apply(new ArchiveParticipantMutationBatch(one, Arrays.asList(
          Mutation.put("account", bytes(2, 1), new byte[0]),
          Mutation.delete("storage-row", bytes(2, 2)))), () -> { });
    }

    ArchiveProgressEnvelope checkpoint = new ArchiveProgressFile(checkpointPath, codec).load();
    ArchiveProgressEnvelope reader = new ArchiveProgressFile(readerPath, codec).load();
    assertArrayEquals(checkpoint.getMutationPlanDigest(), reader.getMutationPlanDigest());
    assertArrayEquals(checkpoint.getMutationPlanDigest(),
        recording.get("account").progress.getMutationPlanDigest());
    assertEquals(1, recording.get("account").mutations.size());
    assertArrayEquals(new byte[0], recording.get("account").mutations.get(0).getValue());
    assertNull(recording.get("storage-row").mutations.get(0).getValue());
    assertEquals(0, recording.get("witness").mutations.size());
    assertFalse(Files.exists(new ArchiveTargetMutationPlanFile(checkpointPath).getPath()));
  }

  private static void assertBuildFails(HistoryCommitMarker target, List<Mutation> mutations) {
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTargetMutationPlanBuilder().build(target,
            new ArchiveParticipantMutationBatch(target, mutations)));
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return participants;
  }

  private static HistoryCommitMarker marker(long epoch, List<String> participants) {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, hash((int) epoch),
        hash((int) epoch - 1), epoch * 1_000L);
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100, 100, (int) epoch,
            bytes(32, (int) epoch + 20)),
        new HistoryIndexLocation(epoch * 50, 50, bytes(32, (int) epoch + 30)),
        bytes(16, (int) epoch + 40), participants);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), marker.getDatabases());
  }

  private static ArchiveProgressEnvelope progress(String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, participant,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), marker.getDatabases());
  }

  private static byte[] hash(int suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static final class RecordingParticipant implements ArchiveParticipant {
    private List<ArchiveParticipantMutation> mutations = Collections.emptyList();
    private ArchiveProgressEnvelope progress;

    private RecordingParticipant(ArchiveProgressEnvelope progress) {
      this.progress = progress;
    }

    @Override
    public void apply(List<ArchiveParticipantMutation> mutations,
        ArchiveProgressEnvelope progress) {
      this.mutations = new ArrayList<>(mutations);
      this.progress = progress;
    }

    @Override
    public ArchiveProgressEnvelope loadProgress() {
      return progress;
    }
  }
}
