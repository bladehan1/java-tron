package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveTargetMutationPlanFile.Stage;

public class ArchiveTargetMutationPlanFileTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void codecRoundTripsExactPutDeleteAndEmptyValues() {
    ArchiveTargetMutationPlan plan = plan(1);
    ArchiveTargetMutationPlan decoded = new ArchiveTargetMutationPlanCodec().decode(
        new ArchiveTargetMutationPlanCodec().encode(plan));

    assertEquals(1, decoded.getTarget().getEpoch());
    assertEquals(PARTICIPANTS, decoded.getTarget().getParticipants());
    assertArrayEquals(bytes(3, 1), decoded.getMutations("account").get(0).getKey());
    assertArrayEquals(new byte[0], decoded.getMutations("account").get(0).getValue());
    assertNull(decoded.getMutations("account-asset").get(0).getValue());
    assertArrayEquals(plan.digest(), decoded.digest());

    Map<String, List<ArchiveParticipantMutation>> substituted = new LinkedHashMap<>(
        plan.getMutations());
    substituted.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 9))));
    ArchiveTargetMutationPlan replacement = new ArchiveTargetMutationPlan(
        plan.getTarget(), substituted);
    assertFalse(Arrays.equals(plan.digest(), replacement.digest()));
  }

  @Test
  public void atomicFaultExposesOnlyOldOrNewPlan() throws Exception {
    Path checkpoint = temporaryFolder.newFolder("atomic").toPath().resolve("checkpoint.progress");
    ArchiveTargetMutationPlanFile normal = new ArchiveTargetMutationPlanFile(checkpoint);
    normal.store(plan(0));

    ArchiveTargetMutationPlanFile beforeReplace = new ArchiveTargetMutationPlanFile(checkpoint,
        (stage, path) -> {
          if (stage == Stage.AFTER_TEMPORARY_FORCE) {
            throw new IOException("injected before replace");
          }
        });
    assertThrows(IOException.class, () -> beforeReplace.store(plan(1)));
    assertEquals(0, normal.loadRequired().getTarget().getEpoch());

    ArchiveTargetMutationPlanFile afterReplace = new ArchiveTargetMutationPlanFile(checkpoint,
        (stage, path) -> {
          if (stage == Stage.AFTER_REPLACE) {
            throw new IOException("injected after replace");
          }
        });
    assertThrows(IOException.class, () -> afterReplace.store(plan(1)));
    assertEquals(1, normal.loadRequired().getTarget().getEpoch());
  }

  @Test
  public void rejectsChecksumCorruptionAndTruncation() throws Exception {
    Path checkpoint = temporaryFolder.newFolder("corrupt").toPath().resolve("checkpoint.progress");
    ArchiveTargetMutationPlanFile file = new ArchiveTargetMutationPlanFile(checkpoint);
    file.store(plan(1));
    byte[] encoded = Files.readAllBytes(file.getPath());
    encoded[encoded.length - 1] ^= 1;
    Files.write(file.getPath(), encoded);
    assertThrows(ArchivePersistenceException.class, file::loadRequired);

    Files.write(file.getPath(), Arrays.copyOf(encoded, 10));
    assertThrows(ArchivePersistenceException.class, file::loadRequired);
  }

  @Test
  public void canonicalizesContainerOrderAndRejectsDuplicatePhysicalKeys() {
    ArchiveProgressEnvelope target = plan(1).getTarget();
    Map<String, List<ArchiveParticipantMutation>> first = new LinkedHashMap<>();
    first.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.delete(bytes(3, 2))));
    first.put("account", Arrays.asList(
        ArchiveParticipantMutation.put(bytes(3, 3), bytes(1, 3)),
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 1))));
    Map<String, List<ArchiveParticipantMutation>> second = new LinkedHashMap<>();
    second.put("account", Arrays.asList(
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 1)),
        ArchiveParticipantMutation.put(bytes(3, 3), bytes(1, 3))));
    second.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.delete(bytes(3, 2))));
    assertArrayEquals(new ArchiveTargetMutationPlan(target, first).digest(),
        new ArchiveTargetMutationPlan(target, second).digest());

    second.put("account", Arrays.asList(
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 1)),
        ArchiveParticipantMutation.delete(bytes(3, 1))));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveTargetMutationPlan(target, second));
  }

  private static ArchiveTargetMutationPlan plan(long epoch) {
    ArchiveProgressEnvelope target = new ArchiveProgressEnvelope(Kind.APPLY_CHECKPOINT, null,
        epoch, bytes(32, (int) epoch), bytes(16, (int) epoch + 10),
        bytes(32, (int) epoch + 20), PARTICIPANTS);
    Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
    mutations.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes(3, 1), new byte[0])));
    mutations.put("account-asset", Collections.singletonList(
        ArchiveParticipantMutation.delete(bytes(3, 2))));
    return new ArchiveTargetMutationPlan(target, mutations);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
