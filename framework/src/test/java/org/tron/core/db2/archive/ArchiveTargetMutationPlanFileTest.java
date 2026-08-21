package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
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
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;

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

  @Test
  public void reopensFixedP66TransitionMutationPlanVectors() throws Exception {
    byte[] address = accountAddress(7);
    String tokenId = "1000007";
    P66AccountAssetCodec codec = new P66AccountAssetCodec();
    byte[] directKey = codec.assetPhysicalKey(address, tokenId);
    ArchiveTargetMutationPlan[] vectors = {
        transitionPlan(1, Phase.P66_OFF, address,
            account(address, false, tokenId, 20L, 1_000L), null, null),
        transitionPlan(2, Phase.P66_ACTIVATION, address,
            account(address, true, null, 0L, 2_000L), directKey,
            ByteBuffer.allocate(Long.BYTES).putLong(30L).array()),
        transitionPlan(3, Phase.P66_ON, address,
            account(address, true, null, 0L, 3_000L), directKey,
            ByteBuffer.allocate(Long.BYTES).putLong(40L).array())
    };

    for (ArchiveTargetMutationPlan expected : vectors) {
      Path checkpoint = temporaryFolder.newFolder(
          "p66-plan-" + expected.getTargetPhase().name().toLowerCase())
          .toPath().resolve("checkpoint.progress");
      new ArchiveTargetMutationPlanFile(checkpoint).store(expected);

      ArchiveTargetMutationPlan reopened =
          new ArchiveTargetMutationPlanFile(checkpoint).loadRequired();
      assertEquals(expected.getTarget().getEpoch(), reopened.getTarget().getEpoch());
      assertArrayEquals(expected.getTarget().getBlockHash(),
          reopened.getTarget().getBlockHash());
      assertArrayEquals(expected.getTarget().getBatchId(), reopened.getTarget().getBatchId());
      assertArrayEquals(expected.getTarget().getPayloadDigest(),
          reopened.getTarget().getPayloadDigest());
      assertEquals(expected.getAccountAssetFormatId(), reopened.getAccountAssetFormatId());
      assertEquals(expected.getTargetPhase(), reopened.getTargetPhase());
      assertMutationsEqual(expected.getMutations("account"),
          reopened.getMutations("account"));
      assertMutationsEqual(expected.getMutations("account-asset"),
          reopened.getMutations("account-asset"));
      assertArrayEquals(expected.digest(), reopened.digest());
    }
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

  private static ArchiveTargetMutationPlan transitionPlan(long epoch, Phase phase,
      byte[] address, byte[] accountValue, byte[] directKey, byte[] directValue) {
    ArchiveProgressEnvelope target = new ArchiveProgressEnvelope(Kind.APPLY_CHECKPOINT, null,
        epoch, bytes(32, (int) epoch), bytes(16, (int) epoch + 10),
        bytes(32, (int) epoch + 20), PARTICIPANTS);
    Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
    mutations.put("account", Collections.singletonList(
        ArchiveParticipantMutation.put(address, accountValue)));
    mutations.put("account-asset", directKey == null ? Collections.emptyList()
        : Collections.singletonList(ArchiveParticipantMutation.put(directKey, directValue)));
    return new ArchiveTargetMutationPlan(target, P66AccountAssetCodec.FORMAT_ID,
        phase, mutations);
  }

  private static byte[] accountAddress(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] account(byte[] address, boolean optimized, String tokenId,
      long assetBalance, long balance) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(optimized).setBalance(balance);
    if (tokenId != null) {
      builder.putAsset("asset-name", assetBalance).putAssetV2(tokenId, assetBalance);
    }
    return builder.build().toByteArray();
  }

  private static void assertMutationsEqual(List<ArchiveParticipantMutation> expected,
      List<ArchiveParticipantMutation> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i).getKey(), actual.get(i).getKey());
      assertArrayEquals(expected.get(i).getValue(), actual.get(i).getValue());
    }
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
