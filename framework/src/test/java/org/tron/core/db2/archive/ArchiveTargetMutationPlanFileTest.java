package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

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
    assertEquals(P66AccountAssetCodec.FORMAT_ID, decoded.getAccountAssetFormatId());
    assertEquals(Phase.P66_ON, decoded.getTargetPhase());
    assertEquals(PARTICIPANTS, decoded.getTarget().getParticipants());
    assertArrayEquals(bytes(3, 1), decoded.getMutations("account").get(0).getKey());
    assertArrayEquals(new byte[0], decoded.getMutations("account").get(0).getValue());
    assertNull(decoded.getMutations("account-asset").get(0).getValue());
    assertArrayEquals(plan.digest(), decoded.digest());
    assertEquals("e4bd9becc28e2afae72429f5630b3bd04bbe55fe9ae651809667bc5d5ad43bbf",
        BaseEncoding.base16().lowerCase().encode(plan.digest()));

    Map<String, List<ArchiveParticipantMutation>> substituted = new LinkedHashMap<>(
        plan.getMutations());
    substituted.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 9))));
    ArchiveTargetMutationPlan replacement = new ArchiveTargetMutationPlan(
        plan.getTarget(), P66AccountAssetCodec.FORMAT_ID, Phase.P66_ON, substituted);
    assertFalse(Arrays.equals(plan.digest(), replacement.digest()));
  }

  @Test
  public void digestBindsFormatAndPhaseAndLegacyVersionFailsClosed() {
    ArchiveTargetMutationPlan canonical = plan(1);
    ArchiveTargetMutationPlan activation = new ArchiveTargetMutationPlan(
        canonical.getTarget(), P66AccountAssetCodec.FORMAT_ID, Phase.P66_ACTIVATION,
        canonical.getMutations());
    ArchiveTargetMutationPlan substitutedFormat = new ArchiveTargetMutationPlan(
        canonical.getTarget(), "archive-state/p66-account-asset/legacy",
        Phase.P66_ON, canonical.getMutations());

    assertFalse(Arrays.equals(canonical.digest(), activation.digest()));
    assertFalse(Arrays.equals(canonical.digest(), substitutedFormat.digest()));
    ArchiveTargetMutationPlanCodec codec = new ArchiveTargetMutationPlanCodec();
    assertEquals(Phase.P66_ACTIVATION,
        codec.decode(codec.encode(activation)).getTargetPhase());
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(codec.encode(substitutedFormat)));

    byte[] legacy = codec.encode(canonical);
    ByteBuffer.wrap(legacy).putShort(Integer.BYTES, (short) 1);
    byte[] payload = Arrays.copyOf(legacy, legacy.length - Integer.BYTES);
    ByteBuffer.wrap(legacy, legacy.length - Integer.BYTES, Integer.BYTES)
        .putInt(Hashing.crc32c().hashBytes(payload).asInt());
    assertThrows(IllegalArgumentException.class, () -> codec.decode(legacy));
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
    assertArrayEquals(new ArchiveTargetMutationPlan(target, P66AccountAssetCodec.FORMAT_ID,
            Phase.P66_ON, first).digest(),
        new ArchiveTargetMutationPlan(target, P66AccountAssetCodec.FORMAT_ID,
            Phase.P66_ON, second).digest());

    second.put("account", Arrays.asList(
        ArchiveParticipantMutation.put(bytes(3, 1), bytes(1, 1)),
        ArchiveParticipantMutation.delete(bytes(3, 1))));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveTargetMutationPlan(target, P66AccountAssetCodec.FORMAT_ID,
            Phase.P66_ON, second));
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
    return new ArchiveTargetMutationPlan(target, P66AccountAssetCodec.FORMAT_ID,
        Phase.P66_ON, mutations);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
