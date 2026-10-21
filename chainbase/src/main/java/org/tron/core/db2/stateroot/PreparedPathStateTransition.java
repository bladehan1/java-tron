package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.tron.common.crypto.Hash;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Immutable, memory-only candidate trie result for one exact block transition. */
public final class PreparedPathStateTransition {

  private final PathStateRootMetadata parent;
  private final PathStateBlockTransition transition;
  private final PathStateRoot.Snapshot snapshot;
  private final List<NodeMutation> nodeMutations;

  private PreparedPathStateTransition(PathStateRootMetadata parent,
      PathStateBlockTransition transition, PathStateRoot.Snapshot snapshot,
      List<NodeMutation> nodeMutations) {
    this.parent = parent;
    this.transition = transition;
    this.snapshot = snapshot;
    this.nodeMutations = Collections.unmodifiableList(new ArrayList<>(nodeMutations));
  }

  /** Computes candidate node changes without opening or writing any native path-state Store. */
  public static PreparedPathStateTransition prepare(PathStateRootMetadata parent,
      PathStateRoot.Snapshot parentSnapshot, PathStateBlockTransition transition) {
    PathStateRootMetadata admittedParent = Objects.requireNonNull(parent, "parent");
    PathStateRoot.Snapshot admittedSnapshot = Objects.requireNonNull(parentSnapshot,
        "parentSnapshot");
    PathStateBlockTransition admittedTransition = Objects.requireNonNull(transition,
        "transition");
    requireChild(admittedParent, admittedSnapshot, admittedTransition);

    Map<Integer, RecordingNodeStore> stores = new LinkedHashMap<>();
    PathStateParticipantScope scope = new PathStateCanonicalizer().participantScope();
    PathStateRoot root = PathStateRoot.fromSnapshot(scope, participant -> stores.computeIfAbsent(
        participant.getStoreId(), ignored -> new RecordingNodeStore()),
        stores.computeIfAbsent(0, ignored -> new RecordingNodeStore()), admittedSnapshot);
    if (!admittedTransition.getMutations().isEmpty()) {
      root.apply(admittedTransition.getMutations());
    }
    PathStateRoot.Snapshot candidate = root.snapshot();
    List<NodeMutation> mutations = new ArrayList<>();
    for (Map.Entry<Integer, RecordingNodeStore> store : stores.entrySet()) {
      store.getValue().appendTo(store.getKey(), mutations);
    }
    return new PreparedPathStateTransition(admittedParent, admittedTransition, candidate,
        mutations);
  }

  public byte[] getStateRoot() {
    return snapshot.getStateRoot();
  }

  public int getNodeMutationCount() {
    return nodeMutations.size();
  }

  /** Freezes this candidate as one immutable Snapshot-owned forward delta. */
  public PathStateSnapshotDelta toSnapshotDelta(BlockSnapshotMeta meta) {
    return PathStateSnapshotDelta.from(meta, this,
        new PathStateCanonicalizer().participantScope());
  }

  PathStateRootMetadata getParent() {
    return parent;
  }

  PathStateBlockTransition getTransition() {
    return transition;
  }

  PathStateRoot.Snapshot getSnapshot() {
    return snapshot;
  }

  List<NodeMutation> getNodeMutations() {
    return nodeMutations;
  }

  boolean extendsParent(PathStateRootMetadata expected) {
    return Arrays.equals(parent.encode(), expected.encode());
  }

  /** Validates the immutable candidate without reading any path-state Store. */
  void validatePreparedTransition(PathStateParticipantScope scope) {
    PathStateParticipantScope admittedScope = Objects.requireNonNull(scope, "scope");
    if (transition.getBlockNumber() != parent.getBlockNumber() + 1
        || !Arrays.equals(transition.getParentHash(), parent.getBlockHash())) {
      throw new IllegalStateException("path-state prepared transition no longer extends parent");
    }
    byte[] candidateRoot = snapshot.getStateRoot();
    if (candidateRoot.length != PathStateCommitmentCodec.ROOT_LENGTH) {
      throw new IllegalStateException("path-state prepared root must contain exactly 32 bytes");
    }
    Set<Integer> admittedStoreIds = new LinkedHashSet<>();
    admittedStoreIds.add(0);
    for (PathStateParticipant participant : admittedScope.getParticipants()) {
      admittedStoreIds.add(participant.getStoreId());
    }
    Set<NodeMutationKey> unique = new LinkedHashSet<>();
    NodeMutation superRoot = null;
    for (NodeMutation mutation : nodeMutations) {
      if (!admittedStoreIds.contains(mutation.storeId)) {
        throw new IllegalStateException("prepared path-state node has unknown Store ID");
      }
      if (mutation.path.length > PathMerkleTrie.SECURE_KEY_LENGTH * 2) {
        throw new IllegalStateException("prepared path-state node path is too long");
      }
      for (byte nibble : mutation.path) {
        if (nibble < 0 || nibble > 15) {
          throw new IllegalStateException("prepared path-state node path is not nibble encoded");
        }
      }
      if (!unique.add(new NodeMutationKey(mutation.storeId, mutation.path))) {
        throw new IllegalStateException("duplicate prepared path-state node mutation");
      }
      if (mutation.encodedNode != null && mutation.encodedNode.length == 0) {
        throw new IllegalStateException("prepared path-state node encoding is empty");
      }
      if (mutation.storeId == 0 && mutation.path.length == 0) {
        superRoot = mutation;
      }
    }
    if (!Arrays.equals(candidateRoot, parent.getStateRoot())) {
      if (superRoot == null || superRoot.encodedNode == null
          || !Arrays.equals(Hash.sha3(superRoot.encodedNode), candidateRoot)) {
        throw new IllegalStateException("prepared path-state super root mutation is inconsistent");
      }
    }
  }

  private static void requireChild(PathStateRootMetadata parent,
      PathStateRoot.Snapshot snapshot, PathStateBlockTransition transition) {
    if (!Arrays.equals(parent.getStateRoot(), snapshot.getStateRoot())) {
      throw new IllegalArgumentException("path-state prepared parent snapshot root mismatch");
    }
    if (transition.getBlockNumber() != parent.getBlockNumber() + 1
        || !Arrays.equals(transition.getParentHash(), parent.getBlockHash())) {
      throw new IllegalArgumentException("path-state prepared transition does not extend parent");
    }
  }

  static final class NodeMutation {

    private final int storeId;
    private final byte[] path;
    private final byte[] encodedNode;

    private NodeMutation(int storeId, byte[] path, byte[] encodedNode) {
      this.storeId = storeId;
      this.path = Arrays.copyOf(path, path.length);
      this.encodedNode = encodedNode == null ? null
          : Arrays.copyOf(encodedNode, encodedNode.length);
    }

    int getStoreId() {
      return storeId;
    }

    byte[] getPath() {
      return Arrays.copyOf(path, path.length);
    }

    byte[] getEncodedNode() {
      return encodedNode == null ? null : Arrays.copyOf(encodedNode, encodedNode.length);
    }
  }

  private static final class RecordingNodeStore implements PathNodeStore {

    private final Map<BytesKey, byte[]> changes = new LinkedHashMap<>();

    @Override
    public byte[] get(byte[] path) {
      byte[] value = changes.get(new BytesKey(path));
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      changes.put(new BytesKey(path), Arrays.copyOf(encodedNode, encodedNode.length));
    }

    @Override
    public void delete(byte[] path) {
      changes.put(new BytesKey(path), null);
    }

    private void appendTo(int storeId, List<NodeMutation> mutations) {
      for (Map.Entry<BytesKey, byte[]> change : changes.entrySet()) {
        mutations.add(new NodeMutation(storeId, change.getKey().bytes, change.getValue()));
      }
    }
  }

  private static final class BytesKey {

    private final byte[] bytes;

    private BytesKey(byte[] bytes) {
      this.bytes = Arrays.copyOf(Objects.requireNonNull(bytes, "path"), bytes.length);
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof BytesKey
          && Arrays.equals(bytes, ((BytesKey) other).bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  private static final class NodeMutationKey {

    private final int storeId;
    private final byte[] path;

    private NodeMutationKey(int storeId, byte[] path) {
      this.storeId = storeId;
      this.path = Arrays.copyOf(path, path.length);
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof NodeMutationKey
          && storeId == ((NodeMutationKey) other).storeId
          && Arrays.equals(path, ((NodeMutationKey) other).path);
    }

    @Override
    public int hashCode() {
      return 31 * storeId + Arrays.hashCode(path);
    }
  }
}
