package org.tron.core.db2.stateroot;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
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

  private final PathStateParticipantScope scope;
  private final Map<String, PathMerkleTrie> participantTries = new LinkedHashMap<>();
  private final PathMerkleTrie superTrie;

  public PathStateRoot(PathStateParticipantScope scope, PathNodeStoreFactory storeFactory,
      PathNodeStore superNodeStore) {
    this.scope = Objects.requireNonNull(scope, "scope");
    PathNodeStoreFactory factory = Objects.requireNonNull(storeFactory, "storeFactory");
    Set<PathNodeStore> uniqueStores = Collections.newSetFromMap(new IdentityHashMap<>());
    for (PathStateParticipant participant : scope.getParticipants()) {
      PathNodeStore nodeStore = Objects.requireNonNull(factory.open(participant),
          "participant node store");
      if (!uniqueStores.add(nodeStore)) {
        throw new IllegalArgumentException("participant node Stores must have distinct identities");
      }
      participantTries.put(participant.getDbName(), new PathMerkleTrie(nodeStore));
    }
    PathNodeStore rootStore = Objects.requireNonNull(superNodeStore, "superNodeStore");
    if (!uniqueStores.add(rootStore)) {
      throw new IllegalArgumentException("super node Store must have a distinct identity");
    }
    superTrie = new PathMerkleTrie(rootStore);
  }

  public void put(String dbName, byte[] canonicalKey, byte[] canonicalValue) {
    PathStateParticipant participant = scope.require(dbName);
    participantTries.get(participant.getDbName()).put(
        PathStateCommitmentCodec.storeLeafKey(participant.getStoreId(), canonicalKey),
        PathStateCommitmentCodec.presentLeafValue(canonicalValue));
  }

  public void delete(String dbName, byte[] canonicalKey) {
    PathStateParticipant participant = scope.require(dbName);
    participantTries.get(participant.getDbName()).delete(
        PathStateCommitmentCodec.storeLeafKey(participant.getStoreId(), canonicalKey));
  }

  public byte[] participantRoot(String dbName) {
    PathStateParticipant participant = scope.require(dbName);
    return participantTries.get(participant.getDbName()).rootHash();
  }

  /** Returns the super root after binding every participant identity, format, and current root. */
  public byte[] rootHash() {
    for (PathStateParticipant participant : scope.getParticipants()) {
      byte[] storeRoot = participantTries.get(participant.getDbName()).rootHash();
      superTrie.put(PathStateCommitmentCodec.superLeafKey(participant.getStoreId()),
          PathStateCommitmentCodec.superLeafValue(participant.getStoreId(),
              participant.getDbName(), participant.getStoreFormatVersion(), storeRoot));
    }
    return superTrie.rootHash();
  }

  /** Creates an independent node Store for one immutable participant identity. */
  public interface PathNodeStoreFactory {

    PathNodeStore open(PathStateParticipant participant);
  }
}
