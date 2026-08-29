package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Exact-27 participant and super-trie namespace views over one BASE or LAYER native database. */
public final class PathStateNodeStoreSet implements Closeable {

  public static final String NODES_DIRECTORY = "nodes";
  private static final byte[] PROGRESS_KEY = new byte[]{
      (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};
  private static final byte[] LOGICAL_BYTES_KEY = new byte[]{
      (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      'l', 'o', 'g', 'i', 'c', 'a', 'l', '-', 'b', 'y', 't', 'e', 's'};
  private static final byte[] REBUILD_CHECKPOINT_KEY = new byte[]{
      (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      'r', 'e', 'b', 'u', 'i', 'l', 'd'};
  private static final byte[] LEAF_OVERLAY_KEY = new byte[]{
      (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      'l', 'e', 'a', 'f', '-', 'o', 'v', 'e', 'r', 'l', 'a', 'y'};
  private static final byte[] LEAF_OVERLAY_VALUE = new byte[]{1};
  private static final int LEAF_DOMAIN = -2;
  private static final int NODE_TOMBSTONE_DOMAIN = -3;
  private static final int LEAF_TOMBSTONE_DOMAIN = -4;
  private static final byte[] NODE_TOMBSTONE_PREFIX = ByteBuffer.allocate(Integer.BYTES)
      .putInt(NODE_TOMBSTONE_DOMAIN).array();
  private static final byte[] TOMBSTONE_VALUE = new byte[]{1};
  private static final byte[] LEAF_PREFIX = ByteBuffer.allocate(Integer.BYTES)
      .putInt(LEAF_DOMAIN).array();
  private static final int LEAF_KEY_LENGTH = Integer.BYTES * 2 + PathMerkleTrie.SECURE_KEY_LENGTH;
  private static final byte[] LEAF_TOMBSTONE_PREFIX = ByteBuffer.allocate(Integer.BYTES)
      .putInt(LEAF_TOMBSTONE_DOMAIN).array();

  private final Path directory;
  private final PathStateParticipantScope scope;
  private final Map<String, PathNodeStore> participantStores = new LinkedHashMap<>();
  private final Map<Integer, LinkedHashMap<BytesKey, byte[]>> pending = new LinkedHashMap<>();
  private final Map<BytesKey, byte[]> localLeaves = new LinkedHashMap<>();
  private final Map<BytesKey, byte[]> persistedLeaves = new LinkedHashMap<>();
  private final Set<BytesKey> leafTombstones = new LinkedHashSet<>();
  private final PathStateNativeNodeStore nativeStore;
  private final PathStateNodeStoreSet parentStores;
  private final PathNodeStore superStore;
  private final byte[] manifestDigest;
  private final Kind kind;
  private final PathStateRootMetadata expectedMetadata;
  private final boolean sealed;
  private PathStateRootMetadata progress;
  private PathStateRebuildCheckpoint rebuildCheckpoint;
  private Long logicalBytes;
  private boolean leafOverlay;
  private PathStateRoot root;
  private boolean rootClaimed;
  private boolean closed;

  private PathStateNodeStoreSet(Path directory, PathStateStoreManifest manifest, Kind kind,
      PathStateRootMetadata expectedMetadata, PathStateNodeStoreSet parentStores)
      throws IOException {
    this.directory = directory.resolve(NODES_DIRECTORY);
    this.scope = new PathStateCanonicalizer().participantScope();
    this.manifestDigest = manifest.getIdentityDigest();
    this.kind = kind;
    this.expectedMetadata = expectedMetadata;
    this.parentStores = parentStores;
    this.sealed = Files.exists(directory.resolve(PathStateCurrentStore.METADATA_FILE),
        LinkOption.NOFOLLOW_LINKS);
    nativeStore = PathStateNativeNodeStore.open(this.directory, manifest.getEngine());
    try {
      progress = decodeProgress(nativeStore.get(PROGRESS_KEY));
      logicalBytes = decodeLogicalBytes(nativeStore.get(LOGICAL_BYTES_KEY));
      rebuildCheckpoint = decodeRebuildCheckpoint(nativeStore.get(REBUILD_CHECKPOINT_KEY));
      byte[] leafOverlayValue = nativeStore.get(LEAF_OVERLAY_KEY);
      if ((progress == null) != (logicalBytes == null)) {
        throw new IOException("path-state native progress and logical bytes marker differ");
      }
      if (progress != null && rebuildCheckpoint != null) {
        throw new IOException("path-state native progress conflicts with rebuild checkpoint");
      }
      if (progress != null) {
        requireProgressIdentity(progress);
      } else if (kind == Kind.BASE && expectedMetadata != null) {
        throw new IOException("path-state BASE metadata exists without native progress");
      }
      if (rebuildCheckpoint != null) {
        requireRebuildCheckpointIdentity(rebuildCheckpoint);
      }
      if (leafOverlayValue != null
          && (kind != Kind.LAYER || progress == null
          || !Arrays.equals(leafOverlayValue, LEAF_OVERLAY_VALUE))) {
        throw new IOException("path-state leaf overlay marker is invalid");
      }
      leafOverlay = leafOverlayValue != null
          || kind == Kind.LAYER && progress == null && parentStores != null;
      inheritParentLeaves();
      loadPersistedLeaves();
      loadLeafTombstones();
      validateNodeTombstones();
      boolean hasUnexpectedLeaves = kind == Kind.BASE
          ? !persistedLeaves.isEmpty() : !localLeaves.isEmpty();
      if (progress == null && rebuildCheckpoint == null
          && (hasUnexpectedLeaves || !leafTombstones.isEmpty())) {
        throw new IOException("path-state leaf inventory exists without native progress");
      }
      for (PathStateParticipant participant : scope.getParticipants()) {
        pending.put(participant.getStoreId(), new LinkedHashMap<>());
        participantStores.put(participant.getDbName(),
            new NamespacedNodeStore(this, participant.getStoreId()));
      }
      pending.put(0, new LinkedHashMap<>());
      superStore = new NamespacedNodeStore(this, 0);
    } catch (RuntimeException | IOException failure) {
      nativeStore.close();
      throw failure;
    }
  }

  public static PathStateNodeStoreSet openBase(PathStateStoreManifest manifest)
      throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    Path metadataPath = admitted.getBaseDirectory().resolve(PathStateCurrentStore.METADATA_FILE);
    PathStateRootMetadata metadata = Files.exists(metadataPath, LinkOption.NOFOLLOW_LINKS)
        ? PathStateMetadataFile.load(metadataPath) : null;
    return new PathStateNodeStoreSet(admitted.getBaseDirectory(), admitted, Kind.BASE, metadata,
        null);
  }

  static PathStateNodeStoreSet beginBaseAt(PathStateStoreManifest manifest, Path directory)
      throws IOException {
    return new PathStateNodeStoreSet(Objects.requireNonNull(directory, "directory"),
        Objects.requireNonNull(manifest, "manifest"), Kind.BASE, null, null);
  }

  public static PathStateNodeStoreSet openLayer(PathStateStoreManifest manifest,
      PathStateRootMetadata metadata) throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateRootMetadata layer = Objects.requireNonNull(metadata, "metadata");
    if (layer.getKind() != Kind.LAYER) {
      throw new IllegalArgumentException("path-state layer node set requires LAYER metadata");
    }
    if (!Arrays.equals(layer.getFormatDigest(), admitted.getIdentityDigest())) {
      throw new IOException("path-state layer node set manifest identity mismatch");
    }
    Path layerDirectory = admitted.getLayerDirectory(layer.getBlockNumber(), layer.getBlockHash());
    requireUnsealed(layerDirectory);
    return new PathStateNodeStoreSet(layerDirectory, admitted, Kind.LAYER, layer, null);
  }

  /** Opens the node database referenced by the verified current authority. */
  public static PathStateNodeStoreSet openCurrent(PathStateStoreManifest manifest)
      throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateRootMetadata current = new PathStateCurrentStore(admitted).current();
    return openPublished(admitted, current);
  }

  static PathStateNodeStoreSet beginLayer(PathStateStoreManifest manifest,
      PathStateRootMetadata identity, PathStateNodeStoreSet parentStores) throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateRootMetadata layer = Objects.requireNonNull(identity, "identity");
    if (layer.getKind() != Kind.LAYER
        || !Arrays.equals(layer.getFormatDigest(), admitted.getIdentityDigest())) {
      throw new IOException("path-state layer node set identity mismatch");
    }
    Path directory = admitted.getLayerDirectory(layer.getBlockNumber(), layer.getBlockHash());
    requireUnsealed(directory);
    return new PathStateNodeStoreSet(directory, admitted, Kind.LAYER, null,
        Objects.requireNonNull(parentStores, "parentStores"));
  }

  static PathStateNodeStoreSet openPublished(PathStateStoreManifest manifest,
      PathStateRootMetadata metadata) throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    PathStateRootMetadata published = Objects.requireNonNull(metadata, "metadata");
    Path owner = published.getKind() == Kind.BASE ? admitted.getBaseDirectory()
        : admitted.getLayerDirectory(published.getBlockNumber(), published.getBlockHash());
    PathStateRootMetadata stored = PathStateMetadataFile.load(
        owner.resolve(PathStateCurrentStore.METADATA_FILE));
    if (!Arrays.equals(stored.encode(), published.encode())) {
      throw new IOException("path-state published metadata differs from authority");
    }
    PathStateNodeStoreSet parent = published.getKind() == Kind.BASE ? null
        : openPublished(admitted, loadParent(admitted, published));
    try {
      return new PathStateNodeStoreSet(owner, admitted, published.getKind(), published, parent);
    } catch (IOException | RuntimeException failure) {
      closeAfterFailure(parent, failure);
      throw failure;
    }
  }

  /** Claims this database and restores its durable leaves when progress already exists. */
  public synchronized PathStateRoot createRoot() {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    PathStateRoot candidate = new PathStateRoot(scope,
        participant -> participantStores.get(participant.getDbName()),
        superStore);
    if (progress != null || rebuildCheckpoint != null) {
      if (progress == null && rebuildCheckpoint.hasIndependentStores()) {
        candidate.restoreRebuildLeaves(restoredLeafRecords(), rebuildCheckpoint);
      } else {
        byte[] expectedRoot = progress == null ? rebuildCheckpoint.getPartialRoot()
            : progress.getStateRoot();
        candidate.restoreLeaves(restoredLeafRecords(), expectedRoot);
      }
      if (hasPending()) {
        throw new IllegalStateException("path-state leaf restoration attempted to repair nodes");
      }
    }
    root = candidate;
    rootClaimed = true;
    return root;
  }

  synchronized PathStateRoot createRootFrom(List<PathStateRoot.LeafRecord> parentLeaves,
      byte[] parentRoot) {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    if (progress != null || !localLeaves.isEmpty() || !leafTombstones.isEmpty()) {
      throw new IllegalStateException("path-state layer already contains durable state");
    }
    PathStateRoot candidate = new PathStateRoot(scope,
        participant -> participantStores.get(participant.getDbName()), superStore);
    if (parentStores == null) {
      throw new IllegalStateException("path-state layer has no parent node overlay");
    }
    candidate.restoreLeaves(parentLeaves, parentRoot);
    if (hasPending()) {
      throw new IllegalStateException("path-state parent restore attempted to copy nodes");
    }
    root = candidate;
    rootClaimed = true;
    return root;
  }

  synchronized PathStateRoot createRootFrom(PathStateRoot.Snapshot snapshot,
      byte[] parentRoot) {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    if (progress != null || !localLeaves.isEmpty() || !leafTombstones.isEmpty()) {
      throw new IllegalStateException("path-state layer already contains durable state");
    }
    if (parentStores == null
        || !Arrays.equals(snapshot.getStateRoot(), Objects.requireNonNull(parentRoot,
            "parentRoot"))) {
      throw new IllegalArgumentException("path-state parent snapshot root mismatch");
    }
    PathStateRoot candidate = PathStateRoot.fromSnapshot(scope,
        participant -> participantStores.get(participant.getDbName()), superStore, snapshot);
    if (hasPending()) {
      throw new IllegalStateException("path-state snapshot fork attempted to copy nodes");
    }
    root = candidate;
    rootClaimed = true;
    return root;
  }

  synchronized PathStateRoot createRootFrom(PreparedPathStateTransition prepared) {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    if (progress != null || !localLeaves.isEmpty() || !leafTombstones.isEmpty()) {
      throw new IllegalStateException("path-state layer already contains durable state");
    }
    if (parentStores == null) {
      throw new IllegalStateException("path-state layer has no parent node overlay");
    }
    PreparedPathStateTransition candidate = Objects.requireNonNull(prepared, "prepared");
    PathStateRoot next = PathStateRoot.fromSnapshot(scope,
        participant -> participantStores.get(participant.getDbName()), superStore,
        candidate.getSnapshot());
    if (!candidate.getTransition().getMutations().isEmpty()) {
      next.recordPendingLeafMutations(candidate.getTransition().getMutations());
    }
    for (PreparedPathStateTransition.NodeMutation mutation : candidate.getNodeMutations()) {
      PathNodeStore store = nodeStore(mutation.getStoreId());
      byte[] encoded = mutation.getEncodedNode();
      if (encoded == null) {
        store.delete(mutation.getPath());
      } else {
        store.put(mutation.getPath(), encoded);
      }
    }
    next.verifyNodeStores();
    root = next;
    rootClaimed = true;
    return root;
  }

  synchronized List<PathStateRoot.LeafRecord> leafRecords() {
    requireOpen();
    if (root == null) {
      throw new IllegalStateException("path-state node database set has no trie owner");
    }
    return root.leafRecords();
  }

  synchronized PathStateRoot initializeBase(List<PathStateRoot.LeafRecord> leaves,
      byte[] expectedRoot) {
    requireOpen();
    if (kind != Kind.BASE || rootClaimed || progress != null || rebuildCheckpoint != null) {
      throw new IllegalStateException("path-state replacement BASE is not empty");
    }
    PathStateRoot candidate = new PathStateRoot(scope,
        participant -> participantStores.get(participant.getDbName()), superStore);
    List<PathStateRoot.LeafRecord> admittedLeaves = Objects.requireNonNull(leaves, "leaves");
    candidate.initializeLeaves(admittedLeaves, expectedRoot);
    candidate.recordPendingLeafRecords(admittedLeaves);
    root = candidate;
    rootClaimed = true;
    return candidate;
  }

  /** Atomically persists all pending path nodes and their exact root progress. */
  public synchronized void commit(PathStateRootMetadata metadata) throws IOException {
    requireOpen();
    if (sealed) {
      throw new IOException("path-state node database set is sealed by immutable metadata");
    }
    if (root == null) {
      throw new IllegalStateException("path-state node database set has no trie owner");
    }
    PathStateRootMetadata next = Objects.requireNonNull(metadata, "metadata");
    long nextLogicalBytes = projectedLogicalBytes(next);
    List<PathStateRoot.LeafMutationRecord> leafMutations = root.pendingLeafMutations();
    List<PathStateNativeNodeStore.BatchMutation> mutations =
        new ArrayList<>(pendingSize() + leafMutations.size() + 4);
    appendPendingMutations(mutations);
    appendLeafMutations(mutations, leafMutations);
    mutations.add(PathStateNativeNodeStore.BatchMutation.put(PROGRESS_KEY, next.encode()));
    if (leafOverlay) {
      mutations.add(PathStateNativeNodeStore.BatchMutation.put(
          LEAF_OVERLAY_KEY, LEAF_OVERLAY_VALUE));
    }
    mutations.add(PathStateNativeNodeStore.BatchMutation.put(LOGICAL_BYTES_KEY,
        ByteBuffer.allocate(Long.BYTES).putLong(nextLogicalBytes).array()));
    if (rebuildCheckpoint != null) {
      mutations.add(PathStateNativeNodeStore.BatchMutation.delete(REBUILD_CHECKPOINT_KEY));
    }
    nativeStore.writeBatch(mutations);
    clearPending();
    recordCommittedLeaves(leafMutations);
    root.clearPendingLeafMutations();
    progress = next;
    rebuildCheckpoint = null;
    logicalBytes = nextLogicalBytes;
  }

  /** Persists one more completed rebuild Store without creating BASE authority. */
  synchronized void checkpointRebuild(PathStateRebuildCheckpoint checkpoint,
      Collection<Integer> participantStoreIds) throws IOException {
    requireOpen();
    if (kind != Kind.BASE || sealed || progress != null || root == null) {
      throw new IOException("path-state rebuild checkpoint is not admissible");
    }
    PathStateRebuildCheckpoint next = Objects.requireNonNull(checkpoint, "checkpoint");
    requireRebuildCheckpointIdentity(next);
    int previousCount = rebuildCheckpoint == null ? 0
        : rebuildCheckpoint.getCompletedStores().size();
    if (next.getCompletedStores().size() != previousCount + 1) {
      throw new IOException("path-state rebuild checkpoint must advance one Store");
    }
    if (rebuildCheckpoint != null) {
      for (PathStateRebuildCoordinator.StoreResult previous
          : rebuildCheckpoint.getCompletedStores()) {
        boolean retained = false;
        for (PathStateRebuildCoordinator.StoreResult advanced : next.getCompletedStores()) {
          if (sameStoreResult(previous, advanced)) {
            retained = true;
            break;
          }
        }
        if (!retained) {
          throw new IOException("path-state rebuild checkpoint rewrites completed Store");
        }
      }
    }
    Set<Integer> storeIds = new LinkedHashSet<>(Objects.requireNonNull(participantStoreIds,
        "participantStoreIds"));
    if (storeIds.isEmpty() || storeIds.contains(0)) {
      throw new IOException("path-state rebuild checkpoint Store ownership is invalid");
    }
    List<PathStateRoot.LeafMutationRecord> leafMutations = new ArrayList<>();
    for (Integer storeId : storeIds) {
      requireParticipant(storeId);
      leafMutations.addAll(root.pendingLeafMutations(storeId));
    }
    List<PathStateNativeNodeStore.BatchMutation> mutations =
        durableStateMutations(next.encode(), leafMutations, storeIds);
    nativeStore.writeBatch(mutations);
    for (Integer storeId : storeIds) {
      clearPending(storeId);
      root.clearPendingLeafMutations(storeId);
    }
    recordCommittedLeaves(leafMutations);
    rebuildCheckpoint = next;
  }

  PathStateRebuildCheckpoint getRebuildCheckpoint() {
    requireOpen();
    return rebuildCheckpoint;
  }

  synchronized long projectedLogicalBytes(PathStateRootMetadata metadata) throws IOException {
    requireOpen();
    if (root == null) {
      throw new IllegalStateException("path-state node database set has no trie owner");
    }
    PathStateRootMetadata next = Objects.requireNonNull(metadata, "metadata");
    requireProgressIdentity(next);
    if (!Arrays.equals(root.rootHash(), next.getStateRoot())) {
      throw new IllegalArgumentException("path-state progress root does not match trie root");
    }
    long total = rebuildCheckpoint == null ? (logicalBytes == null ? 0 : logicalBytes)
        : rebuildLogicalBytes();
    total = projectedPendingBytes(total);
    total = projectedLeafBytes(total, root.pendingLeafMutations());
    if (leafOverlay) {
      total = replaceLogicalEntry(total, LEAF_OVERLAY_KEY,
          nativeStore.get(LEAF_OVERLAY_KEY), LEAF_OVERLAY_VALUE);
    }
    total = replaceLogicalEntry(total, PROGRESS_KEY,
        progress == null ? null : progress.encode(), next.encode());
    return rebuildCheckpoint == null ? total : replaceLogicalEntry(total,
        REBUILD_CHECKPOINT_KEY, rebuildCheckpoint.encode(), null);
  }

  public synchronized PathStateRootMetadata getProgress() {
    requireOpen();
    return progress;
  }

  static PathStateRootMetadata loadProgress(Path ownerDirectory,
      PathStateStoreManifest manifest) throws IOException {
    Path nodes = Objects.requireNonNull(ownerDirectory, "ownerDirectory").resolve(NODES_DIRECTORY);
    if (!Files.exists(nodes, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    if (!Files.isDirectory(nodes, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(nodes)) {
      throw new IOException("path-state node database is not a direct directory: " + nodes);
    }
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(nodes,
        Objects.requireNonNull(manifest, "manifest").getEngine())) {
      return decodeProgress(store.get(PROGRESS_KEY));
    }
  }

  static Long loadLogicalBytes(Path ownerDirectory, PathStateStoreManifest manifest)
      throws IOException {
    Path nodes = Objects.requireNonNull(ownerDirectory, "ownerDirectory").resolve(NODES_DIRECTORY);
    if (!Files.exists(nodes, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    if (!Files.isDirectory(nodes, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(nodes)) {
      throw new IOException("path-state node database is not a direct directory: " + nodes);
    }
    try (PathStateNativeNodeStore store = PathStateNativeNodeStore.open(nodes,
        Objects.requireNonNull(manifest, "manifest").getEngine())) {
      Long expected = decodeLogicalBytes(store.get(LOGICAL_BYTES_KEY));
      if (expected == null) {
        return null;
      }
      long[] actual = new long[]{0};
      try {
        store.scanAll(entry -> {
          byte[] key = entry.getKey();
          if (!Arrays.equals(key, LOGICAL_BYTES_KEY)) {
            actual[0] = Math.addExact(actual[0],
                Math.addExact(key.length, entry.getValue().length));
          }
        });
      } catch (ArithmeticException overflow) {
        throw new IOException("path-state logical bytes verification overflow", overflow);
      }
      if (actual[0] != expected) {
        throw new IOException("path-state logical bytes marker does not match native entries");
      }
      return expected;
    }
  }

  public Path getDirectory() {
    return directory;
  }

  /** Releases inherited read handles after the child root has been frozen for publication. */
  synchronized void releaseParentReadHandles() throws IOException {
    requireOpen();
    if (parentStores != null) {
      parentStores.close();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      nativeStore.close();
    } catch (IOException closeFailure) {
      failure = closeFailure;
    }
    if (parentStores != null) {
      try {
        parentStores.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("path-state node database set is closed: " + directory);
    }
  }

  private byte[] get(byte[] key) {
    BytesKey ownedKey = new BytesKey(key);
    Map<BytesKey, byte[]> participantPending = pending(key);
    synchronized (participantPending) {
      if (participantPending.containsKey(ownedKey)) {
        byte[] value = participantPending.get(ownedKey);
        return value == null ? null : Arrays.copyOf(value, value.length);
      }
    }
    byte[] owned = ownedKey.copy();
    byte[] local = nativeStore.get(owned);
    if (local != null) {
      return local;
    }
    if (kind == Kind.LAYER && nativeStore.get(tombstoneKey(owned)) != null) {
      return null;
    }
    return parentStores == null ? null : parentStores.get(owned);
  }

  private void put(byte[] key, byte[] value) {
    Map<BytesKey, byte[]> participantPending = pending(key);
    synchronized (participantPending) {
      participantPending.put(new BytesKey(key), Arrays.copyOf(value, value.length));
    }
  }

  private void delete(byte[] key) {
    Map<BytesKey, byte[]> participantPending = pending(key);
    synchronized (participantPending) {
      participantPending.put(new BytesKey(key), null);
    }
  }

  private PathNodeStore nodeStore(int storeId) {
    if (storeId == 0) {
      return superStore;
    }
    for (PathStateParticipant participant : scope.getParticipants()) {
      if (participant.getStoreId() == storeId) {
        return participantStores.get(participant.getDbName());
      }
    }
    throw new IllegalArgumentException("unknown prepared path-state Store ID: " + storeId);
  }

  private void requireProgressIdentity(PathStateRootMetadata metadata) throws IOException {
    if (metadata.getKind() != kind
        || !Arrays.equals(metadata.getFormatDigest(), manifestDigest)) {
      throw new IOException("path-state native progress identity mismatch");
    }
    if (expectedMetadata != null
        && !Arrays.equals(metadata.encode(), expectedMetadata.encode())) {
      throw new IOException("path-state native progress differs from immutable metadata");
    }
  }

  private void loadPersistedLeaves() throws IOException {
    nativeStore.scanPrefix(LEAF_PREFIX, entry -> {
      byte[] key = entry.getKey();
      if (key.length != LEAF_KEY_LENGTH || ByteBuffer.wrap(key).getInt() != LEAF_DOMAIN) {
        throw new IOException("path-state durable leaf key is malformed");
      }
      int storeId = ByteBuffer.wrap(key, Integer.BYTES, Integer.BYTES).getInt();
      requireParticipant(storeId);
      BytesKey leafKey = new BytesKey(key);
      byte[] value = entry.getValue();
      if (kind == Kind.LAYER) {
        if (localLeaves.put(leafKey, value) != null) {
          throw new IOException("duplicate path-state durable leaf key");
        }
        persistedLeaves.put(leafKey, value);
      } else if (persistedLeaves.put(leafKey, value) != null) {
        throw new IOException("duplicate path-state durable leaf key");
      }
    });
  }

  private void inheritParentLeaves() {
    if (!leafOverlay || parentStores == null) {
      return;
    }
    for (Map.Entry<BytesKey, byte[]> entry : parentStores.persistedLeaves.entrySet()) {
      persistedLeaves.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
    }
  }

  private void loadLeafTombstones() throws IOException {
    nativeStore.scanPrefix(LEAF_TOMBSTONE_PREFIX, entry -> {
      if (!leafOverlay || kind != Kind.LAYER || progress == null) {
        throw new IOException("path-state leaf tombstones require durable LAYER progress");
      }
      byte[] key = entry.getKey();
      if (key.length != LEAF_KEY_LENGTH
          || ByteBuffer.wrap(key).getInt() != LEAF_TOMBSTONE_DOMAIN
          || !Arrays.equals(entry.getValue(), TOMBSTONE_VALUE)) {
        throw new IOException("path-state leaf tombstone is malformed");
      }
      int storeId = ByteBuffer.wrap(key, Integer.BYTES, Integer.BYTES).getInt();
      requireParticipant(storeId);
      BytesKey leafKey = new BytesKey(leafKeyFromTombstone(key));
      if (localLeaves.containsKey(leafKey)) {
        throw new IOException("path-state leaf and tombstone coexist");
      }
      if (!persistedLeaves.containsKey(leafKey)) {
        throw new IOException("path-state leaf tombstone does not mask a parent leaf");
      }
      leafTombstones.add(leafKey);
      persistedLeaves.remove(leafKey);
    });
  }

  private void validateNodeTombstones() throws IOException {
    nativeStore.scanPrefix(NODE_TOMBSTONE_PREFIX, entry -> {
      if (kind != Kind.LAYER || progress == null) {
        throw new IOException("path-state node tombstones require durable LAYER progress");
      }
      byte[] key = entry.getKey();
      if (key.length < Integer.BYTES * 2
          || ByteBuffer.wrap(key).getInt() != NODE_TOMBSTONE_DOMAIN
          || !Arrays.equals(entry.getValue(), TOMBSTONE_VALUE)) {
        throw new IOException("path-state node tombstone is malformed");
      }
      byte[] nodeKey = Arrays.copyOfRange(key, Integer.BYTES, key.length);
      int storeId = ByteBuffer.wrap(nodeKey).getInt();
      if (storeId < 0 || storeId > scope.getParticipants().size()) {
        throw new IOException("path-state node tombstone has an unknown Store ID");
      }
      for (int index = Integer.BYTES; index < nodeKey.length; index++) {
        if (nodeKey[index] < 0 || nodeKey[index] > 15) {
          throw new IOException("path-state node tombstone contains a non-nibble path");
        }
      }
      if (nativeStore.get(nodeKey) != null) {
        throw new IOException("path-state node and tombstone coexist");
      }
    });
  }

  private List<PathStateNativeNodeStore.BatchMutation> durableStateMutations(
      byte[] rebuildValue, List<PathStateRoot.LeafMutationRecord> leafMutations,
      Collection<Integer> storeIds) {
    List<PathStateNativeNodeStore.BatchMutation> mutations =
        new ArrayList<>(pendingSize(storeIds) + leafMutations.size() + 1);
    appendPendingMutations(mutations, storeIds);
    appendLeafMutations(mutations, leafMutations);
    mutations.add(PathStateNativeNodeStore.BatchMutation.put(REBUILD_CHECKPOINT_KEY,
        rebuildValue));
    return mutations;
  }

  private long rebuildLogicalBytes() throws IOException {
    long[] total = new long[]{0};
    try {
      nativeStore.scanAll(entry -> {
        byte[] key = entry.getKey();
        if (!Arrays.equals(key, LOGICAL_BYTES_KEY)) {
          total[0] = Math.addExact(total[0],
              Math.addExact(key.length, entry.getValue().length));
        }
      });
      return total[0];
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state rebuild logical bytes overflow", overflow);
    }
  }

  private void requireRebuildCheckpointIdentity(PathStateRebuildCheckpoint checkpoint)
      throws IOException {
    if (kind != Kind.BASE || expectedMetadata != null
        || !Arrays.equals(checkpoint.getManifestDigest(), manifestDigest)) {
      throw new IOException("path-state rebuild checkpoint identity mismatch");
    }
  }

  private static boolean sameStoreResult(PathStateRebuildCoordinator.StoreResult left,
      PathStateRebuildCoordinator.StoreResult right) {
    return left.getStoreId() == right.getStoreId()
        && left.getDbName().equals(right.getDbName())
        && left.getEntryCount() == right.getEntryCount()
        && Arrays.equals(left.getInputDigest(), right.getInputDigest())
        && Arrays.equals(left.getStoreRoot(), right.getStoreRoot());
  }

  private List<PathStateRoot.LeafRecord> restoredLeafRecords() {
    List<PathStateRoot.LeafRecord> records = new ArrayList<>(persistedLeaves.size());
    for (Map.Entry<BytesKey, byte[]> entry : persistedLeaves.entrySet()) {
      byte[] key = entry.getKey().copy();
      int storeId = ByteBuffer.wrap(key, Integer.BYTES, Integer.BYTES).getInt();
      byte[] secureKey = Arrays.copyOfRange(key, Integer.BYTES * 2, key.length);
      records.add(new PathStateRoot.LeafRecord(storeId, secureKey, entry.getValue()));
    }
    return records;
  }

  private PathStateParticipant requireParticipant(int storeId) {
    for (PathStateParticipant participant : scope.getParticipants()) {
      if (participant.getStoreId() == storeId) {
        return participant;
      }
    }
    throw new IllegalArgumentException("unknown path-state durable leaf Store ID: " + storeId);
  }

  private static PathStateRootMetadata decodeProgress(byte[] encoded) throws IOException {
    if (encoded == null) {
      return null;
    }
    try {
      return PathStateRootMetadata.decode(encoded);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("path-state native progress is corrupt", invalid);
    }
  }

  private static Long decodeLogicalBytes(byte[] encoded) throws IOException {
    if (encoded == null) {
      return null;
    }
    if (encoded.length != Long.BYTES) {
      throw new IOException("path-state logical bytes marker is corrupt");
    }
    long value = ByteBuffer.wrap(encoded).getLong();
    if (value < 0) {
      throw new IOException("path-state logical bytes marker is negative");
    }
    return value;
  }

  private static PathStateRebuildCheckpoint decodeRebuildCheckpoint(byte[] encoded)
      throws IOException {
    return encoded == null ? null : PathStateRebuildCheckpoint.decode(encoded);
  }

  private static long replaceLogicalEntry(long total, byte[] key, byte[] previous, byte[] next)
      throws IOException {
    try {
      long adjusted = previous == null ? total
          : Math.subtractExact(total, Math.addExact(key.length, previous.length));
      return next == null ? adjusted
          : Math.addExact(adjusted, Math.addExact(key.length, next.length));
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state logical bytes overflow", overflow);
    }
  }

  private void appendPendingMutations(
      List<PathStateNativeNodeStore.BatchMutation> mutations) {
    appendPendingMutations(mutations, pending.keySet());
  }

  private void appendPendingMutations(List<PathStateNativeNodeStore.BatchMutation> mutations,
      Collection<Integer> storeIds) {
    for (Integer storeId : storeIds) {
      Map<BytesKey, byte[]> participantPending = pending.get(storeId);
      synchronized (participantPending) {
        appendPendingMutations(mutations, participantPending);
      }
    }
  }

  private void appendPendingMutations(List<PathStateNativeNodeStore.BatchMutation> mutations,
      Map<BytesKey, byte[]> participantPending) {
    for (Map.Entry<BytesKey, byte[]> entry : participantPending.entrySet()) {
      byte[] key = entry.getKey().copy();
      byte[] value = entry.getValue();
      if (kind == Kind.LAYER) {
        mutations.add(value == null
            ? PathStateNativeNodeStore.BatchMutation.delete(key)
            : PathStateNativeNodeStore.BatchMutation.put(key, value));
        byte[] tombstone = tombstoneKey(key);
        mutations.add(value == null
            ? PathStateNativeNodeStore.BatchMutation.put(tombstone, TOMBSTONE_VALUE)
            : PathStateNativeNodeStore.BatchMutation.delete(tombstone));
      } else {
        mutations.add(value == null
            ? PathStateNativeNodeStore.BatchMutation.delete(key)
            : PathStateNativeNodeStore.BatchMutation.put(key, value));
      }
    }
  }

  private void appendLeafMutations(List<PathStateNativeNodeStore.BatchMutation> mutations,
      List<PathStateRoot.LeafMutationRecord> leafMutations) {
    for (PathStateRoot.LeafMutationRecord mutation : leafMutations) {
      byte[] key = leafKey(mutation);
      byte[] value = mutation.getEncodedValue();
      if (!Arrays.equals(persistedLeaves.get(new BytesKey(key)), value)) {
        appendLeafMutation(mutations, key, value);
      }
    }
  }

  private void appendLeafMutation(List<PathStateNativeNodeStore.BatchMutation> mutations,
      byte[] key, byte[] value) {
    mutations.add(value == null
        ? PathStateNativeNodeStore.BatchMutation.delete(key)
        : PathStateNativeNodeStore.BatchMutation.put(key, value));
    if (leafOverlay) {
      byte[] tombstone = leafTombstoneKey(key);
      mutations.add(value == null
          ? PathStateNativeNodeStore.BatchMutation.put(tombstone, TOMBSTONE_VALUE)
          : PathStateNativeNodeStore.BatchMutation.delete(tombstone));
    }
  }

  private long projectedLeafBytes(long total,
      List<PathStateRoot.LeafMutationRecord> leafMutations)
      throws IOException {
    long projected = total;
    for (PathStateRoot.LeafMutationRecord mutation : leafMutations) {
      byte[] key = leafKey(mutation);
      byte[] value = mutation.getEncodedValue();
      if (!Arrays.equals(persistedLeaves.get(new BytesKey(key)), value)) {
        projected = projectedLeafMutation(projected, key, value);
      }
    }
    return projected;
  }

  private long projectedLeafMutation(long total, byte[] key, byte[] value) throws IOException {
    long projected = replaceLogicalEntry(total, key, nativeStore.get(key), value);
    if (leafOverlay) {
      byte[] tombstone = leafTombstoneKey(key);
      projected = replaceLogicalEntry(projected, tombstone, nativeStore.get(tombstone),
          value == null ? TOMBSTONE_VALUE : null);
    }
    return projected;
  }

  private void recordCommittedLeaves(List<PathStateRoot.LeafMutationRecord> leafMutations) {
    for (PathStateRoot.LeafMutationRecord mutation : leafMutations) {
      BytesKey key = new BytesKey(leafKey(mutation));
      byte[] value = mutation.getEncodedValue();
      if (Arrays.equals(persistedLeaves.get(key), value)) {
        continue;
      }
      if (value == null) {
        persistedLeaves.remove(key);
        if (kind == Kind.LAYER) {
          localLeaves.remove(key);
        }
        if (leafOverlay) {
          leafTombstones.add(key);
        }
      } else if (!Arrays.equals(persistedLeaves.get(key), value)) {
        byte[] owned = Arrays.copyOf(value, value.length);
        persistedLeaves.put(key, owned);
        if (kind == Kind.LAYER) {
          localLeaves.put(key, owned);
        }
        leafTombstones.remove(key);
      }
    }
  }

  private static byte[] leafKey(PathStateRoot.LeafMutationRecord mutation) {
    return ByteBuffer.allocate(LEAF_KEY_LENGTH)
        .putInt(LEAF_DOMAIN)
        .putInt(mutation.getStoreId())
        .put(mutation.getSecureKey())
        .array();
  }

  private long projectedPendingBytes(long total) throws IOException {
    long projected = total;
    for (Map<BytesKey, byte[]> participantPending : pending.values()) {
      synchronized (participantPending) {
        projected = projectedPendingBytes(projected, participantPending);
      }
    }
    return projected;
  }

  private long projectedPendingBytes(long total, Map<BytesKey, byte[]> participantPending)
      throws IOException {
    long projected = total;
    for (Map.Entry<BytesKey, byte[]> entry : participantPending.entrySet()) {
      byte[] key = entry.getKey().copy();
      byte[] value = entry.getValue();
      projected = replaceLogicalEntry(projected, key, nativeStore.get(key), value);
      if (kind == Kind.LAYER) {
        byte[] tombstone = tombstoneKey(key);
        projected = replaceLogicalEntry(projected, tombstone, nativeStore.get(tombstone),
            value == null ? TOMBSTONE_VALUE : null);
      }
    }
    return projected;
  }

  private Map<BytesKey, byte[]> pending(byte[] key) {
    if (key.length < Integer.BYTES) {
      throw new IllegalArgumentException("path-state namespaced key is too short");
    }
    int storeId = ByteBuffer.wrap(key).getInt();
    Map<BytesKey, byte[]> participantPending = pending.get(storeId);
    if (participantPending == null) {
      throw new IllegalArgumentException("unknown path-state node Store ID: " + storeId);
    }
    return participantPending;
  }

  private boolean hasPending() {
    return pendingSize() != 0;
  }

  private int pendingSize() {
    return pendingSize(pending.keySet());
  }

  private int pendingSize(Collection<Integer> storeIds) {
    int size = 0;
    for (Integer storeId : storeIds) {
      Map<BytesKey, byte[]> participantPending = pending.get(storeId);
      synchronized (participantPending) {
        size = Math.addExact(size, participantPending.size());
      }
    }
    return size;
  }

  private void clearPending() {
    for (Integer storeId : pending.keySet()) {
      clearPending(storeId);
    }
  }

  private void clearPending(int storeId) {
    Map<BytesKey, byte[]> participantPending = pending.get(storeId);
    synchronized (participantPending) {
      participantPending.clear();
    }
  }

  private static byte[] tombstoneKey(byte[] nodeKey) {
    return ByteBuffer.allocate(Integer.BYTES + nodeKey.length)
        .putInt(NODE_TOMBSTONE_DOMAIN)
        .put(nodeKey)
        .array();
  }

  private static byte[] leafTombstoneKey(byte[] leafKey) {
    byte[] tombstone = Arrays.copyOf(leafKey, leafKey.length);
    ByteBuffer.wrap(tombstone).putInt(LEAF_TOMBSTONE_DOMAIN);
    return tombstone;
  }

  private static byte[] leafKeyFromTombstone(byte[] tombstoneKey) {
    byte[] leafKey = Arrays.copyOf(tombstoneKey, tombstoneKey.length);
    ByteBuffer.wrap(leafKey).putInt(LEAF_DOMAIN);
    return leafKey;
  }

  private static PathStateRootMetadata loadParent(PathStateStoreManifest manifest,
      PathStateRootMetadata child) throws IOException {
    if (child.getKind() != Kind.LAYER || child.getBlockNumber() == 0) {
      throw new IOException("path-state node overlay has an invalid child identity");
    }
    PathStateRootMetadata base = PathStateMetadataFile.load(
        manifest.getBaseDirectory().resolve(PathStateCurrentStore.METADATA_FILE));
    PathStateRootMetadata parent = base.getBlockNumber() == child.getBlockNumber() - 1
        ? base : PathStateMetadataFile.load(manifest.getLayerDirectory(
            child.getBlockNumber() - 1, child.getParentHash())
            .resolve(PathStateCurrentStore.METADATA_FILE));
    if (child.getBlockNumber() != parent.getBlockNumber() + 1
        || !Arrays.equals(child.getParentHash(), parent.getBlockHash())
        || !Arrays.equals(child.getParentStateRoot(), parent.getStateRoot())
        || !Arrays.equals(parent.getFormatDigest(), manifest.getIdentityDigest())) {
      throw new IOException("path-state node overlay parent identity mismatch");
    }
    return parent;
  }

  private static void closeAfterFailure(PathStateNodeStoreSet stores, Throwable failure) {
    if (stores == null) {
      return;
    }
    try {
      stores.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static void requireUnsealed(Path directory) throws IOException {
    Path metadata = directory.resolve(PathStateCurrentStore.METADATA_FILE);
    if (Files.exists(metadata, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state node database set is sealed by immutable metadata: "
          + directory);
    }
  }

  private static final class NamespacedNodeStore implements PathNodeStore {

    private final PathStateNodeStoreSet owner;
    private final int storeId;

    private NamespacedNodeStore(PathStateNodeStoreSet owner, int storeId) {
      this.owner = owner;
      this.storeId = storeId;
    }

    @Override
    public byte[] get(byte[] path) {
      return owner.get(key(path));
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      byte[] value = Arrays.copyOf(Objects.requireNonNull(encodedNode, "encodedNode"),
          encodedNode.length);
      if (value.length == 0) {
        throw new IllegalArgumentException("encodedNode must not be empty");
      }
      owner.put(key(path), value);
    }

    @Override
    public void delete(byte[] path) {
      owner.delete(key(path));
    }

    private byte[] key(byte[] path) {
      byte[] ownedPath = Arrays.copyOf(Objects.requireNonNull(path, "path"), path.length);
      for (byte nibble : ownedPath) {
        if (nibble < 0 || nibble > 15) {
          throw new IllegalArgumentException("path-state node path contains a non-nibble byte");
        }
      }
      return ByteBuffer.allocate(Integer.BYTES + ownedPath.length)
          .putInt(storeId)
          .put(ownedPath)
          .array();
    }
  }

  private static final class BytesKey {

    private final byte[] bytes;

    private BytesKey(byte[] bytes) {
      this.bytes = Arrays.copyOf(Objects.requireNonNull(bytes, "bytes"), bytes.length);
    }

    private byte[] copy() {
      return Arrays.copyOf(bytes, bytes.length);
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
}
