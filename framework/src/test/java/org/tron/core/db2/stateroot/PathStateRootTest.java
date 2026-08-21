package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.tron.core.trie.TrieImpl;

public class PathStateRootTest {

  private static final PathStateParticipant ABI = participant(1, "abi");
  private static final PathStateParticipant ACCOUNT = participant(4, "account");
  private static final PathStateParticipant ASSET_ISSUE = participant(6, "asset-issue");
  private static final PathStateParticipant ASSET_ISSUE_V2 = participant(7, "asset-issue-v2");
  private static final PathStateParticipant STORAGE = participant(22, "storage-row");

  @Test
  public void aggregatesEveryParticipantIntoIndependentOracleSuperRoot() {
    List<PathStateParticipant> participants = participants();
    PathStateRoot stateRoot = stateRoot(participants);
    Mutation[] mutations = {
        mutation("abi", "contract", "abi-v1"),
        mutation("asset-issue", "asset", "legacy"),
        mutation("asset-issue-v2", "asset", "v2"),
        mutation("account", "address", "account-value"),
        mutation("storage-row", "slot", "storage-value")
    };
    for (Mutation mutation : mutations) {
      stateRoot.put(mutation.dbName, mutation.key, mutation.value);
    }

    byte[] expected = referenceRoot(participants, mutations);
    assertArrayEquals(
        Hex.decode("f8d0364fdb0432016c12f9a660de2bd34513257014e35d90ac289d9024e6d216"),
        expected);
    assertArrayEquals(expected, stateRoot.rootHash());
  }

  @Test
  public void participantAndMutationOrderDoNotChangeSuperRoot() {
    List<PathStateParticipant> forwardParticipants = participants();
    List<PathStateParticipant> reverseParticipants = new ArrayList<>(forwardParticipants);
    java.util.Collections.reverse(reverseParticipants);
    Mutation[] mutations = {
        mutation("abi", "a", "1"), mutation("asset-issue", "b", "2"),
        mutation("asset-issue-v2", "c", "3"), mutation("account", "d", "4"),
        mutation("storage-row", "e", "5")
    };

    PathStateRoot forward = stateRoot(forwardParticipants);
    for (Mutation mutation : mutations) {
      forward.put(mutation.dbName, mutation.key, mutation.value);
    }
    PathStateRoot reverse = stateRoot(reverseParticipants);
    for (int i = mutations.length - 1; i >= 0; i--) {
      reverse.put(mutations[i].dbName, mutations[i].key, mutations[i].value);
    }
    assertArrayEquals(forward.rootHash(), reverse.rootHash());
  }

  @Test
  public void deleteUpdatesOnlyNamedParticipantAndSuperRoot() {
    PathStateRoot stateRoot = stateRoot(participants());
    byte[] key = bytes("asset");
    stateRoot.put("asset-issue", key, bytes("legacy"));
    stateRoot.put("asset-issue-v2", key, bytes("v2"));
    byte[] originalRoot = stateRoot.rootHash();
    byte[] v2Root = stateRoot.participantRoot("asset-issue-v2");

    stateRoot.delete("asset-issue", key);
    assertArrayEquals(v2Root, stateRoot.participantRoot("asset-issue-v2"));
    org.junit.Assert.assertFalse(Arrays.equals(originalRoot, stateRoot.rootHash()));
  }

  @Test
  public void emptyParticipantStillChangesCommittedScope() {
    PathStateRoot base = stateRoot(participants());
    List<PathStateParticipant> extended = new ArrayList<>(participants());
    extended.add(participant(21, "proposal"));
    PathStateRoot withEmptyProposal = stateRoot(extended);

    org.junit.Assert.assertFalse(Arrays.equals(base.rootHash(), withEmptyProposal.rootHash()));
  }

  @Test
  public void scopeRejectsMissingDuplicateAndUnknownParticipants() {
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateParticipantScope(Arrays.asList(ABI, ASSET_ISSUE)));
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateParticipantScope(Arrays.asList(ABI, ASSET_ISSUE, ASSET_ISSUE_V2,
            participant(1, "other"))));
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateParticipantScope(Arrays.asList(ABI, ASSET_ISSUE, ASSET_ISSUE_V2,
            participant(9, "abi"))));

    PathStateRoot stateRoot = stateRoot(participants());
    assertThrows(IllegalArgumentException.class,
        () -> stateRoot.put("unknown", bytes("key"), bytes("value")));
    assertThrows(IllegalArgumentException.class,
        () -> stateRoot.delete("unknown", bytes("key")));

    PathStateParticipantScope scope = new PathStateParticipantScope(participants());
    InMemoryPathNodeStore sharedStore = new InMemoryPathNodeStore();
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateRoot(scope, ignored -> sharedStore, new InMemoryPathNodeStore()));
    assertThrows(IllegalArgumentException.class,
        () -> new PathStateRoot(scope,
            participant -> participant.getDbName().equals("abi")
                ? sharedStore : new InMemoryPathNodeStore(), sharedStore));
  }

  @Test
  public void batchValidationRejectsPartialAndDuplicateMutationSets() {
    PathStateRoot stateRoot = stateRoot(participants());
    byte[] originalRoot = stateRoot.rootHash();
    List<PathStateMutation> partial = Arrays.asList(
        PathStateMutation.put("account", bytes("valid"), bytes("value")),
        PathStateMutation.put("unknown", bytes("invalid"), bytes("value")));
    assertThrows(IllegalArgumentException.class, () -> stateRoot.apply(partial));
    assertArrayEquals(originalRoot, stateRoot.rootHash());

    List<PathStateMutation> duplicate = Arrays.asList(
        PathStateMutation.put("abi", bytes("key"), new byte[0]),
        PathStateMutation.delete("abi", bytes("key")));
    assertThrows(IllegalArgumentException.class, () -> stateRoot.apply(duplicate));
    assertArrayEquals(originalRoot, stateRoot.rootHash());
    assertThrows(IllegalArgumentException.class,
        () -> stateRoot.apply(java.util.Collections.emptyList()));
  }

  @Test
  public void emptyAndZeroValuesProduceDifferentRoots() {
    PathStateRoot empty = stateRoot(participants());
    empty.put("abi", bytes("key"), new byte[0]);
    PathStateRoot zero = stateRoot(participants());
    zero.put("abi", bytes("key"), new byte[]{0});
    org.junit.Assert.assertFalse(Arrays.equals(empty.rootHash(), zero.rootHash()));
  }

  @Test
  public void verificationRequiresCurrentMaterializedSuperRoot() {
    PathStateRoot stateRoot = stateRoot(participants());
    assertThrows(IllegalStateException.class, stateRoot::verifyNodeStores);
    stateRoot.rootHash();
    stateRoot.verifyNodeStores();
    stateRoot.put("abi", bytes("key"), bytes("value"));
    assertThrows(IllegalStateException.class, stateRoot::verifyNodeStores);
    stateRoot.rootHash();
    stateRoot.verifyNodeStores();
  }

  @Test
  public void concurrentUniqueMutationsMatchSequentialRoot() throws Exception {
    PathStateRoot concurrent = stateRoot(participants());
    PathStateRoot sequential = stateRoot(participants());
    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 64; i++) {
        final byte[] key = bytes("key-" + i);
        final byte[] value = bytes("value-" + i);
        sequential.put("account", key, value);
        futures.add(executor.submit(() -> concurrent.put("account", key, value)));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
    assertArrayEquals(sequential.rootHash(), concurrent.rootHash());
  }

  private static byte[] referenceRoot(List<PathStateParticipant> participants,
      Mutation[] mutations) {
    Map<String, TrieImpl> stores = new LinkedHashMap<>();
    for (PathStateParticipant participant : participants) {
      stores.put(participant.getDbName(), referenceTrie());
    }
    for (Mutation mutation : mutations) {
      PathStateParticipant participant = find(participants, mutation.dbName);
      stores.get(mutation.dbName).put(
          PathStateCommitmentCodec.storeLeafKey(participant.getStoreId(), mutation.key),
          PathStateCommitmentCodec.presentLeafValue(mutation.value));
    }
    TrieImpl superTrie = referenceTrie();
    for (PathStateParticipant participant : participants) {
      superTrie.put(PathStateCommitmentCodec.superLeafKey(participant.getStoreId()),
          PathStateCommitmentCodec.superLeafValue(participant.getStoreId(),
              participant.getDbName(), participant.getStoreFormatVersion(),
              stores.get(participant.getDbName()).getRootHash()));
    }
    return superTrie.getRootHash();
  }

  private static PathStateParticipant find(List<PathStateParticipant> participants,
      String dbName) {
    for (PathStateParticipant participant : participants) {
      if (participant.getDbName().equals(dbName)) {
        return participant;
      }
    }
    throw new AssertionError("missing test participant " + dbName);
  }

  private static PathStateRoot stateRoot(List<PathStateParticipant> participants) {
    PathStateParticipantScope scope = new PathStateParticipantScope(participants);
    return new PathStateRoot(scope, ignored -> new InMemoryPathNodeStore(),
        new InMemoryPathNodeStore());
  }

  private static TrieImpl referenceTrie() {
    TrieImpl trie = new TrieImpl();
    trie.setAsync(false);
    return trie;
  }

  private static List<PathStateParticipant> participants() {
    return Arrays.asList(ABI, ACCOUNT, ASSET_ISSUE, ASSET_ISSUE_V2, STORAGE);
  }

  private static PathStateParticipant participant(int storeId, String dbName) {
    return new PathStateParticipant(storeId, dbName, 1);
  }

  private static Mutation mutation(String dbName, String key, String value) {
    return new Mutation(dbName, bytes(key), bytes(value));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static final class Mutation {

    private final String dbName;
    private final byte[] key;
    private final byte[] value;

    private Mutation(String dbName, byte[] key, byte[] value) {
      this.dbName = dbName;
      this.key = key;
      this.value = value;
    }
  }

  private static final class InMemoryPathNodeStore implements PathNodeStore {

    private final Map<String, byte[]> nodes = new LinkedHashMap<>();

    @Override
    public byte[] get(byte[] path) {
      byte[] node = nodes.get(Hex.toHexString(path));
      return node == null ? null : Arrays.copyOf(node, node.length);
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      nodes.put(Hex.toHexString(path), Arrays.copyOf(encodedNode, encodedNode.length));
    }

    @Override
    public void delete(byte[] path) {
      nodes.remove(Hex.toHexString(path));
    }
  }
}
