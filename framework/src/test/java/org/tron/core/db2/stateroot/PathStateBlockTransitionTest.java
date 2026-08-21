package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;

public class PathStateBlockTransitionTest {

  private static final byte[] BLOCK_HASH = bytes(1);
  private static final byte[] PARENT_HASH = bytes(33);

  @Test
  public void canonicalEvidenceIsOriginFreeAndMutationOrderInvariant() {
    PathStateMutation proposal = PathStateMutation.put("proposal", new byte[]{2}, new byte[]{3});
    PathStateMutation accountDelete = PathStateMutation.delete("account", new byte[]{1});

    PathStateBlockTransition pushed = transition(Arrays.asList(proposal, accountDelete));
    PathStateBlockTransition forkReapplied = transition(Arrays.asList(accountDelete, proposal));

    assertArrayEquals(pushed.getPayloadDigest(), forkReapplied.getPayloadDigest());
    assertEquals("account", pushed.getMutations().get(0).getDbName());
    assertEquals("proposal", pushed.getMutations().get(1).getDbName());
    assertEquals(PathStateParticipantDescriptor.SCOPE_ID, pushed.getScopeId());
    assertEquals(
        "b216f6028db74c2457f1fd625ff4218ad1c358f74d016baa4bc17828e9b4ac7a",
        ByteArray.toHexString(pushed.getPayloadDigest()));
  }

  @Test
  public void digestBindsBlockPhaseAndMutationSemantics() {
    PathStateMutation put = PathStateMutation.put("proposal", new byte[]{2}, new byte[0]);
    PathStateBlockTransition baseline = transition(Collections.singletonList(put));
    PathStateBlockTransition otherBlock = new PathStateBlockTransition(43, BLOCK_HASH, PARENT_HASH,
        1234, P66Phase.P66_ON, Collections.singletonList(put));
    PathStateBlockTransition otherPhase = new PathStateBlockTransition(42, BLOCK_HASH, PARENT_HASH,
        1234, P66Phase.P66_ACTIVATION, Collections.singletonList(put));
    PathStateBlockTransition delete = transition(Collections.singletonList(
        PathStateMutation.delete("proposal", new byte[]{2})));

    assertNotEquals(ByteArray.toHexString(baseline.getPayloadDigest()),
        ByteArray.toHexString(otherBlock.getPayloadDigest()));
    assertNotEquals(ByteArray.toHexString(baseline.getPayloadDigest()),
        ByteArray.toHexString(otherPhase.getPayloadDigest()));
    assertNotEquals(ByteArray.toHexString(baseline.getPayloadDigest()),
        ByteArray.toHexString(delete.getPayloadDigest()));
  }

  @Test
  public void noOpBlockHasDeterministicIndependentOracleDigest() throws Exception {
    PathStateBlockTransition transition = transition(Collections.emptyList());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    writeIdentity(output, "java-tron/path-state/block-transition");
    output.writeShort(1);
    writeIdentity(output, PathStateParticipantDescriptor.SCOPE_ID);
    output.writeByte(2);
    output.writeLong(42);
    output.write(BLOCK_HASH);
    output.write(PARENT_HASH);
    output.writeLong(1234);
    output.writeInt(0);

    assertArrayEquals(Hashing.sha256().hashBytes(bytes.toByteArray()).asBytes(),
        transition.getPayloadDigest());
  }

  @Test
  public void rejectsAmbiguousOrOutOfScopeMutations() {
    PathStateMutation first = PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2});
    PathStateMutation duplicate = PathStateMutation.delete("proposal", new byte[]{1});

    assertThrows(IllegalArgumentException.class,
        () -> transition(Arrays.asList(first, duplicate)));
    assertThrows(IllegalArgumentException.class,
        () -> transition(Collections.singletonList(
            PathStateMutation.put("unknown", new byte[]{1}, new byte[]{2}))));
    assertThrows(IllegalArgumentException.class,
        () -> transition(Collections.singletonList(
            PathStateMutation.put("proposal", new byte[0], new byte[]{2}))));
  }

  @Test
  public void ownsBlockAndMutationBytes() {
    byte[] blockHash = Arrays.copyOf(BLOCK_HASH, BLOCK_HASH.length);
    byte[] key = new byte[]{1};
    byte[] value = new byte[]{2};
    PathStateBlockTransition transition = new PathStateBlockTransition(42, blockHash, PARENT_HASH,
        1234, P66Phase.P66_ON,
        Collections.singletonList(PathStateMutation.put("proposal", key, value)));
    byte[] digest = transition.getPayloadDigest();

    blockHash[0] = 99;
    key[0] = 99;
    value[0] = 99;
    transition.getBlockHash()[0] = 98;
    transition.getMutations().get(0).getCanonicalKey()[0] = 98;
    transition.getPayloadDigest()[0] = 98;

    assertArrayEquals(BLOCK_HASH, transition.getBlockHash());
    assertArrayEquals(new byte[]{1}, transition.getMutations().get(0).getCanonicalKey());
    assertArrayEquals(new byte[]{2}, transition.getMutations().get(0).getCanonicalValue());
    assertArrayEquals(digest, transition.getPayloadDigest());
    assertThrows(UnsupportedOperationException.class,
        () -> transition.getMutations().add(PathStateMutation.delete("proposal", new byte[]{3})));
  }

  private static PathStateBlockTransition transition(List<PathStateMutation> mutations) {
    return new PathStateBlockTransition(42, BLOCK_HASH, PARENT_HASH, 1234,
        P66Phase.P66_ON, mutations);
  }

  private static void writeIdentity(DataOutputStream output, String value) throws Exception {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeShort(encoded.length);
    output.write(encoded);
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) (seed + i);
    }
    return value;
  }
}
