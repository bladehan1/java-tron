package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Current-only per-Store trie and super-trie aggregator for TASK-016.
 *
 * <p>Every participant, including an empty Store, has one super-trie leaf. Consequently the root
 * commits to the supplied participant exact-set as well as each Store's current entries. This
 * component has no block, database, history, restart, or publication lifecycle.
 */
public final class PathStateRoot {

  private static final Comparator<PreparedMutation> MUTATION_COMPARATOR = (left, right) -> {
    int participantOrder = Integer.compare(left.participant.getStoreId(),
        right.participant.getStoreId());
    return participantOrder != 0 ? participantOrder : compareUnsigned(left.secureKey,
        right.secureKey);
  };

  private final PathStateParticipantScope scope;
  private final Map<String, PathMerkleTrie> participantTries = new LinkedHashMap<>();
  private final PathMerkleTrie superTrie;
  private boolean rootMaterialized;

  public PathStateRoot(PathStateParticipantScope scope, PathNodeStoreFactory storeFactory,
      PathNodeStore superNodeStore) {
    this(scope, storeFactory, superNodeStore, null);
  }

  private PathStateRoot(PathStateParticipantScope scope, PathNodeStoreFactory storeFactory,
      PathNodeStore superNodeStore, Snapshot snapshot) {
    this.scope = Objects.requireNonNull(scope, "scope");
    PathNodeStoreFactory factory = Objects.requireNonNull(storeFactory, "storeFactory");
    Set<PathNodeStore> uniqueStores = Collections.newSetFromMap(new IdentityHashMap<>());
    for (PathStateParticipant participant : scope.getParticipants()) {
      PathNodeStore nodeStore = Objects.requireNonNull(factory.open(participant),
          "participant node store");
      if (!uniqueStores.add(nodeStore)) {
        throw new IllegalArgumentException("participant node Stores must have distinct identities");
      }
      PathMerkleTrie.Snapshot trieSnapshot = snapshot == null ? null
          : snapshot.participants.get(participant.getDbName());
      if (snapshot != null && trieSnapshot == null) {
        throw new IllegalArgumentException("path-state snapshot participant scope mismatch");
      }
      participantTries.put(participant.getDbName(), snapshot == null
          ? new PathMerkleTrie(nodeStore) : PathMerkleTrie.fromSnapshot(nodeStore, trieSnapshot));
    }
    PathNodeStore rootStore = Objects.requireNonNull(superNodeStore, "superNodeStore");
    if (!uniqueStores.add(rootStore)) {
      throw new IllegalArgumentException("super node Store must have a distinct identity");
    }
    superTrie = snapshot == null ? new PathMerkleTrie(rootStore)
        : PathMerkleTrie.fromSnapshot(rootStore, snapshot.superTrie);
    if (snapshot != null) {
      if (snapshot.participants.size() != scope.getParticipants().size()
          || !Arrays.equals(superTrie.rootHash(), snapshot.stateRoot)) {
        throw new IllegalArgumentException("path-state snapshot root or scope mismatch");
      }
      rootMaterialized = true;
    }
  }

  public synchronized void put(String dbName, byte[] canonicalKey, byte[] canonicalValue) {
    apply(Collections.singletonList(PathStateMutation.put(dbName, canonicalKey, canonicalValue)));
  }

  public synchronized void delete(String dbName, byte[] canonicalKey) {
    apply(Collections.singletonList(PathStateMutation.delete(dbName, canonicalKey)));
  }

  /** Validates a complete mutation set before changing any participant trie. */
  public synchronized void apply(Collection<PathStateMutation> mutations) {
    List<PreparedMutation> prepared = prepare(mutations);
    for (PreparedMutation mutation : prepared) {
      PathMerkleTrie trie = participantTries.get(mutation.participant.getDbName());
      if (mutation.encodedValue == null) {
        trie.delete(mutation.secureKey);
      } else {
        trie.put(mutation.secureKey, mutation.encodedValue);
      }
    }
    rootMaterialized = false;
  }

  public synchronized byte[] participantRoot(String dbName) {
    PathStateParticipant participant = scope.require(dbName);
    return participantTries.get(participant.getDbName()).rootHash();
  }

  /** Returns the super root after binding every participant identity, format, and current root. */
  public synchronized byte[] rootHash() {
    if (rootMaterialized) {
      return superTrie.rootHash();
    }
    for (PathStateParticipant participant : scope.getParticipants()) {
      byte[] storeRoot = participantTries.get(participant.getDbName()).rootHash();
      superTrie.put(PathStateCommitmentCodec.superLeafKey(participant.getStoreId()),
          PathStateCommitmentCodec.superLeafValue(participant.getStoreId(),
              participant.getDbName(), participant.getStoreFormatVersion(), storeRoot));
    }
    byte[] root = superTrie.rootHash();
    rootMaterialized = true;
    return root;
  }

  /** Verifies all current participant nodes and the already-published super-trie nodes. */
  public synchronized void verifyNodeStores() {
    if (!rootMaterialized) {
      throw new IllegalStateException("path state root is not materialized");
    }
    for (PathMerkleTrie trie : participantTries.values()) {
      trie.verifyNodeStore();
    }
    superTrie.verifyNodeStore();
  }

  synchronized List<LeafRecord> leafRecords() {
    List<LeafRecord> records = new ArrayList<>();
    for (PathStateParticipant participant : scope.getParticipants()) {
      for (PathMerkleTrie.LeafEntry entry
          : participantTries.get(participant.getDbName()).leafEntries()) {
        records.add(new LeafRecord(participant.getStoreId(), entry.getSecureKey(),
            entry.getEncodedValue()));
      }
    }
    return records;
  }

  synchronized Snapshot snapshot() {
    byte[] stateRoot = rootHash();
    Map<String, PathMerkleTrie.Snapshot> snapshots = new LinkedHashMap<>();
    for (PathStateParticipant participant : scope.getParticipants()) {
      snapshots.put(participant.getDbName(),
          participantTries.get(participant.getDbName()).snapshot());
    }
    return new Snapshot(snapshots, superTrie.snapshot(), stateRoot);
  }

  static PathStateRoot fromSnapshot(PathStateParticipantScope scope,
      PathNodeStoreFactory storeFactory, PathNodeStore superNodeStore, Snapshot snapshot) {
    return new PathStateRoot(scope, storeFactory, superNodeStore,
        Objects.requireNonNull(snapshot, "snapshot"));
  }

  synchronized void initializeLeaves(Collection<LeafRecord> records, byte[] expectedRoot) {
    restoreLeaves(records, expectedRoot, true);
  }

  synchronized void restoreLeaves(Collection<LeafRecord> records, byte[] expectedRoot) {
    restoreLeaves(records, expectedRoot, false);
  }

  private void restoreLeaves(Collection<LeafRecord> records, byte[] expectedRoot,
      boolean initialize) {
    Map<Integer, PathStateParticipant> participants = new LinkedHashMap<>();
    Map<Integer, List<PathMerkleTrie.LeafEntry>> leaves = new LinkedHashMap<>();
    for (PathStateParticipant participant : scope.getParticipants()) {
      participants.put(participant.getStoreId(), participant);
      leaves.put(participant.getStoreId(), new ArrayList<>());
    }
    for (LeafRecord record : Objects.requireNonNull(records, "records")) {
      LeafRecord present = Objects.requireNonNull(record, "record");
      if (!participants.containsKey(present.storeId)) {
        throw new IllegalArgumentException("restored leaf has unknown path-state Store ID");
      }
      leaves.get(present.storeId).add(new PathMerkleTrie.LeafEntry(
          present.secureKey, present.encodedValue));
    }

    List<PathMerkleTrie.LeafEntry> superLeaves = new ArrayList<>();
    for (PathStateParticipant participant : scope.getParticipants()) {
      PathMerkleTrie trie = participantTries.get(participant.getDbName());
      if (initialize) {
        trie.initializeLeaves(leaves.get(participant.getStoreId()));
      } else {
        trie.restoreLeaves(leaves.get(participant.getStoreId()));
      }
      byte[] storeRoot = trie.rootHash();
      superLeaves.add(new PathMerkleTrie.LeafEntry(
          PathStateCommitmentCodec.superLeafKey(participant.getStoreId()),
          PathStateCommitmentCodec.superLeafValue(participant.getStoreId(),
              participant.getDbName(), participant.getStoreFormatVersion(), storeRoot)));
    }
    if (initialize) {
      superTrie.initializeLeaves(superLeaves);
    } else {
      superTrie.restoreLeaves(superLeaves);
    }
    byte[] restoredRoot = superTrie.rootHash();
    if (!Arrays.equals(restoredRoot, Objects.requireNonNull(expectedRoot, "expectedRoot"))) {
      throw new IllegalStateException("restored path-state root differs from durable progress");
    }
    rootMaterialized = true;
    verifyNodeStores();
  }

  private List<PreparedMutation> prepare(Collection<PathStateMutation> mutations) {
    List<PathStateMutation> supplied = new ArrayList<>(
        Objects.requireNonNull(mutations, "mutations"));
    if (supplied.isEmpty()) {
      throw new IllegalArgumentException("mutation batch must not be empty");
    }
    List<PreparedMutation> prepared = new ArrayList<>(supplied.size());
    Set<MutationKey> uniqueKeys = new LinkedHashSet<>();
    for (PathStateMutation mutation : supplied) {
      PathStateMutation present = Objects.requireNonNull(mutation, "mutation");
      PathStateParticipant participant = scope.require(present.getDbName());
      byte[] secureKey = PathStateCommitmentCodec.storeLeafKey(participant.getStoreId(),
          present.getCanonicalKey());
      if (!uniqueKeys.add(new MutationKey(participant.getStoreId(), secureKey))) {
        throw new IllegalArgumentException("duplicate path-state mutation key");
      }
      byte[] encodedValue = present.isDelete() ? null
          : PathStateCommitmentCodec.presentLeafValue(present.getCanonicalValue());
      prepared.add(new PreparedMutation(participant, secureKey, encodedValue));
    }
    Collections.sort(prepared, MUTATION_COMPARATOR);
    return prepared;
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    for (int i = 0; i < Math.min(left.length, right.length); i++) {
      int result = Integer.compare(left[i] & 0xff, right[i] & 0xff);
      if (result != 0) {
        return result;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  /** Creates an independent node Store for one immutable participant identity. */
  public interface PathNodeStoreFactory {

    PathNodeStore open(PathStateParticipant participant);
  }

  public static final class Snapshot {

    private final Map<String, PathMerkleTrie.Snapshot> participants;
    private final PathMerkleTrie.Snapshot superTrie;
    private final byte[] stateRoot;

    private Snapshot(Map<String, PathMerkleTrie.Snapshot> participants,
        PathMerkleTrie.Snapshot superTrie, byte[] stateRoot) {
      this.participants = Collections.unmodifiableMap(new LinkedHashMap<>(participants));
      this.superTrie = Objects.requireNonNull(superTrie, "superTrie");
      this.stateRoot = Arrays.copyOf(stateRoot, stateRoot.length);
    }

    public byte[] getStateRoot() {
      return Arrays.copyOf(stateRoot, stateRoot.length);
    }
  }

  static final class LeafRecord {

    private final int storeId;
    private final byte[] secureKey;
    private final byte[] encodedValue;

    LeafRecord(int storeId, byte[] secureKey, byte[] encodedValue) {
      this.storeId = storeId;
      this.secureKey = Arrays.copyOf(Objects.requireNonNull(secureKey, "secureKey"),
          secureKey.length);
      this.encodedValue = Arrays.copyOf(Objects.requireNonNull(encodedValue, "encodedValue"),
          encodedValue.length);
    }

    int getStoreId() {
      return storeId;
    }

    byte[] getSecureKey() {
      return Arrays.copyOf(secureKey, secureKey.length);
    }

    byte[] getEncodedValue() {
      return Arrays.copyOf(encodedValue, encodedValue.length);
    }
  }

  private static final class PreparedMutation {

    private final PathStateParticipant participant;
    private final byte[] secureKey;
    private final byte[] encodedValue;

    private PreparedMutation(PathStateParticipant participant, byte[] secureKey,
        byte[] encodedValue) {
      this.participant = participant;
      this.secureKey = secureKey;
      this.encodedValue = encodedValue;
    }
  }

  private static final class MutationKey {

    private final int storeId;
    private final byte[] secureKey;

    private MutationKey(int storeId, byte[] secureKey) {
      this.storeId = storeId;
      this.secureKey = secureKey;
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof MutationKey
          && storeId == ((MutationKey) other).storeId
          && Arrays.equals(secureKey, ((MutationKey) other).secureKey);
    }

    @Override
    public int hashCode() {
      return 31 * storeId + Arrays.hashCode(secureKey);
    }
  }
}
