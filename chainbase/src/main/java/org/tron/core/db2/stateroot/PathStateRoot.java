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
  private final Map<String, byte[]> rebuiltParticipantRoots = new LinkedHashMap<>();
  private final Map<Integer, LinkedHashMap<MutationKey, PathStateParticipant>>
      pendingLeafMutations = new LinkedHashMap<>();
  private final PathMerkleTrie superTrie;
  private volatile boolean rootMaterialized;

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
      pendingLeafMutations.put(participant.getStoreId(), new LinkedHashMap<>());
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
    recordPendingLeafMutations(prepared);
    rootMaterialized = false;
  }

  /** Applies one rebuild batch while locking only the participant tries touched by that batch. */
  void applyRebuild(Collection<PathStateMutation> mutations) {
    List<PreparedMutation> prepared = prepare(mutations);
    for (PreparedMutation mutation : prepared) {
      PathMerkleTrie trie = participantTries.get(mutation.participant.getDbName());
      if (mutation.encodedValue == null) {
        trie.delete(mutation.secureKey);
      } else {
        trie.put(mutation.secureKey, mutation.encodedValue);
      }
    }
    recordPendingLeafMutations(prepared);
    rootMaterialized = false;
  }

  /** Records a fully streamed participant root without retaining that Store's leaves or tree. */
  void completeRebuildParticipant(String dbName, byte[] storeRoot) {
    PathStateParticipant participant = scope.require(dbName);
    byte[] root = Arrays.copyOf(Objects.requireNonNull(storeRoot, "storeRoot"),
        storeRoot.length);
    if (root.length != PathStateCommitmentCodec.ROOT_LENGTH) {
      throw new IllegalArgumentException("Store root must contain exactly 32 bytes");
    }
    synchronized (rebuiltParticipantRoots) {
      byte[] previous = rebuiltParticipantRoots.put(participant.getDbName(), root);
      if (previous != null && !Arrays.equals(previous, root)) {
        throw new IllegalStateException("path-state rebuild Store root changed");
      }
    }
    rootMaterialized = false;
  }

  void restoreRebuildParticipants(PathStateRebuildCheckpoint checkpoint) {
    for (PathStateRebuildCoordinator.StoreResult result
        : Objects.requireNonNull(checkpoint, "checkpoint").getCompletedStores()) {
      completeRebuildParticipant(result.getDbName(), result.getStoreRoot());
    }
  }

  synchronized void restoreStoredRoots(byte[] expectedRoot) {
    for (PathStateParticipant participant : scope.getParticipants()) {
      participantTries.get(participant.getDbName()).restoreRoot();
    }
    superTrie.restoreRoot(expectedRoot);
    if (!Arrays.equals(superTrie.rootHash(), expectedRoot)) {
      throw new IllegalStateException("restored path-state root differs from durable progress");
    }
    rootMaterialized = true;
  }

  synchronized void recordPendingLeafMutations(Collection<PathStateMutation> mutations) {
    recordPendingLeafMutations(prepare(mutations));
  }

  private void recordPendingLeafMutations(List<PreparedMutation> prepared) {
    for (PreparedMutation mutation : prepared) {
      MutationKey key = new MutationKey(mutation.participant.getStoreId(), mutation.secureKey);
      Map<MutationKey, PathStateParticipant> participantMutations =
          pendingLeafMutations.get(mutation.participant.getStoreId());
      synchronized (participantMutations) {
        participantMutations.put(key, mutation.participant);
      }
    }
  }

  public synchronized byte[] participantRoot(String dbName) {
    PathStateParticipant participant = scope.require(dbName);
    synchronized (rebuiltParticipantRoots) {
      byte[] rebuilt = rebuiltParticipantRoots.get(participant.getDbName());
      if (rebuilt != null) {
        return Arrays.copyOf(rebuilt, rebuilt.length);
      }
    }
    return participantTries.get(participant.getDbName()).rootHash();
  }

  /** Returns the super root after binding every participant identity, format, and current root. */
  public synchronized byte[] rootHash() {
    if (rootMaterialized) {
      return superTrie.rootHash();
    }
    for (PathStateParticipant participant : scope.getParticipants()) {
      byte[] storeRoot = participantRoot(participant.getDbName());
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

  /** Returns only leaf mutations accumulated since the last durable commit/checkpoint. */
  synchronized List<LeafMutationRecord> pendingLeafMutations() {
    List<LeafMutationRecord> mutations = new ArrayList<>();
    for (Integer storeId : pendingLeafMutations.keySet()) {
      mutations.addAll(pendingLeafMutations(storeId));
    }
    return mutations;
  }

  List<LeafMutationRecord> pendingLeafMutations(int storeId) {
    Map<MutationKey, PathStateParticipant> participantMutations =
        pendingLeafMutations.get(storeId);
    if (participantMutations == null) {
      throw new IllegalArgumentException("unknown path-state Store ID: " + storeId);
    }
    synchronized (participantMutations) {
      List<LeafMutationRecord> mutations = new ArrayList<>(participantMutations.size());
      for (Map.Entry<MutationKey, PathStateParticipant> entry
          : participantMutations.entrySet()) {
        MutationKey key = entry.getKey();
        PathStateParticipant participant = entry.getValue();
        mutations.add(new LeafMutationRecord(participant.getStoreId(), key.secureKey,
            participantTries.get(participant.getDbName()).get(key.secureKey)));
      }
      return mutations;
    }
  }

  synchronized void clearPendingLeafMutations() {
    for (Integer storeId : pendingLeafMutations.keySet()) {
      clearPendingLeafMutations(storeId);
    }
  }

  void clearPendingLeafMutations(int storeId) {
    Map<MutationKey, PathStateParticipant> participantMutations =
        pendingLeafMutations.get(storeId);
    if (participantMutations == null) {
      throw new IllegalArgumentException("unknown path-state Store ID: " + storeId);
    }
    synchronized (participantMutations) {
      participantMutations.clear();
    }
  }

  synchronized void recordPendingLeafRecords(Collection<LeafRecord> records) {
    for (LeafRecord record : Objects.requireNonNull(records, "records")) {
      LeafRecord present = Objects.requireNonNull(record, "record");
      PathStateParticipant participant = participant(present.storeId);
      Map<MutationKey, PathStateParticipant> participantMutations =
          pendingLeafMutations.get(present.storeId);
      synchronized (participantMutations) {
        participantMutations.put(new MutationKey(present.storeId, present.secureKey), participant);
      }
    }
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

  synchronized void restoreRebuildLeaves(Collection<LeafRecord> records,
      PathStateRebuildCheckpoint checkpoint) {
    restoreParticipantLeaves(records, false);
    for (PathStateRebuildCoordinator.StoreResult result : checkpoint.getCompletedStores()) {
      if (!Arrays.equals(participantRoot(result.getDbName()), result.getStoreRoot())) {
        throw new IllegalStateException(
            "restored path-state participant root differs from durable progress");
      }
    }
    rootMaterialized = false;
  }

  private void restoreLeaves(Collection<LeafRecord> records, byte[] expectedRoot,
      boolean initialize) {
    restoreParticipantLeaves(records, initialize);
    List<PathMerkleTrie.LeafEntry> superLeaves = new ArrayList<>();
    for (PathStateParticipant participant : scope.getParticipants()) {
      PathMerkleTrie trie = participantTries.get(participant.getDbName());
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

  private Map<Integer, List<PathMerkleTrie.LeafEntry>> restoreParticipantLeaves(
      Collection<LeafRecord> records, boolean initialize) {
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

    for (PathStateParticipant participant : scope.getParticipants()) {
      PathMerkleTrie trie = participantTries.get(participant.getDbName());
      if (initialize) {
        trie.initializeLeaves(leaves.get(participant.getStoreId()));
      } else {
        trie.restoreLeaves(leaves.get(participant.getStoreId()));
      }
    }
    return leaves;
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

  private PathStateParticipant participant(int storeId) {
    for (PathStateParticipant participant : scope.getParticipants()) {
      if (participant.getStoreId() == storeId) {
        return participant;
      }
    }
    throw new IllegalArgumentException("unknown path-state Store ID: " + storeId);
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

  static final class LeafMutationRecord {

    private final int storeId;
    private final byte[] secureKey;
    private final byte[] encodedValue;

    private LeafMutationRecord(int storeId, byte[] secureKey, byte[] encodedValue) {
      this.storeId = storeId;
      this.secureKey = Arrays.copyOf(Objects.requireNonNull(secureKey, "secureKey"),
          secureKey.length);
      this.encodedValue = encodedValue == null ? null
          : Arrays.copyOf(encodedValue, encodedValue.length);
    }

    int getStoreId() {
      return storeId;
    }

    byte[] getSecureKey() {
      return Arrays.copyOf(secureKey, secureKey.length);
    }

    byte[] getEncodedValue() {
      return encodedValue == null ? null : Arrays.copyOf(encodedValue, encodedValue.length);
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
