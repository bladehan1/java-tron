package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Exact-27 participant and super-trie namespace views over one BASE or LAYER native database. */
public final class PathStateNodeStoreSet implements Closeable {

  public static final String NODES_DIRECTORY = "nodes";
  private static final byte[] PROGRESS_KEY = new byte[]{
      (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};

  private final Path directory;
  private final PathStateParticipantScope scope;
  private final Map<String, PathNodeStore> participantStores = new LinkedHashMap<>();
  private final Map<BytesKey, byte[]> pending = new LinkedHashMap<>();
  private final PathStateNativeNodeStore nativeStore;
  private final PathNodeStore superStore;
  private final byte[] manifestDigest;
  private final Kind kind;
  private final PathStateRootMetadata expectedLayer;
  private PathStateRootMetadata progress;
  private PathStateRoot root;
  private boolean rootClaimed;
  private boolean closed;

  private PathStateNodeStoreSet(Path directory, PathStateStoreManifest manifest, Kind kind,
      PathStateRootMetadata expectedLayer)
      throws IOException {
    this.directory = directory.resolve(NODES_DIRECTORY);
    this.scope = new PathStateCanonicalizer().participantScope();
    this.manifestDigest = manifest.getIdentityDigest();
    this.kind = kind;
    this.expectedLayer = expectedLayer;
    nativeStore = PathStateNativeNodeStore.open(this.directory, manifest.getEngine());
    try {
      progress = decodeProgress(nativeStore.get(PROGRESS_KEY));
      if (progress != null) {
        requireProgressIdentity(progress);
      }
      for (PathStateParticipant participant : scope.getParticipants()) {
        participantStores.put(participant.getDbName(),
            new NamespacedNodeStore(this, participant.getStoreId()));
      }
      superStore = new NamespacedNodeStore(this, 0);
    } catch (RuntimeException | IOException failure) {
      nativeStore.close();
      throw failure;
    }
  }

  public static PathStateNodeStoreSet openBase(PathStateStoreManifest manifest)
      throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    requireUnsealed(admitted.getBaseDirectory());
    return new PathStateNodeStoreSet(admitted.getBaseDirectory(), admitted, Kind.BASE, null);
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
    return new PathStateNodeStoreSet(layerDirectory, admitted, Kind.LAYER, layer);
  }

  /** Claims this empty or in-process database for one trie owner. */
  public synchronized PathStateRoot createRoot() {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    if (progress != null) {
      throw new IllegalStateException("path-state persisted root requires leaf restoration");
    }
    rootClaimed = true;
    root = new PathStateRoot(scope,
        participant -> participantStores.get(participant.getDbName()),
        superStore);
    return root;
  }

  /** Atomically persists all pending path nodes and their exact root progress. */
  public synchronized void commit(PathStateRootMetadata metadata) throws IOException {
    requireOpen();
    if (root == null) {
      throw new IllegalStateException("path-state node database set has no trie owner");
    }
    PathStateRootMetadata next = Objects.requireNonNull(metadata, "metadata");
    requireProgressIdentity(next);
    byte[] currentRoot = root.rootHash();
    if (!Arrays.equals(currentRoot, next.getStateRoot())) {
      throw new IllegalArgumentException("path-state progress root does not match trie root");
    }
    java.util.List<PathStateNativeNodeStore.BatchMutation> mutations =
        new java.util.ArrayList<>(pending.size() + 1);
    for (Map.Entry<BytesKey, byte[]> entry : pending.entrySet()) {
      byte[] value = entry.getValue();
      mutations.add(value == null
          ? PathStateNativeNodeStore.BatchMutation.delete(entry.getKey().copy())
          : PathStateNativeNodeStore.BatchMutation.put(entry.getKey().copy(), value));
    }
    mutations.add(PathStateNativeNodeStore.BatchMutation.put(PROGRESS_KEY, next.encode()));
    nativeStore.writeBatch(mutations);
    pending.clear();
    progress = next;
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

  public Path getDirectory() {
    return directory;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    nativeStore.close();
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("path-state node database set is closed: " + directory);
    }
  }

  private synchronized byte[] get(byte[] key) {
    BytesKey ownedKey = new BytesKey(key);
    if (pending.containsKey(ownedKey)) {
      byte[] value = pending.get(ownedKey);
      return value == null ? null : Arrays.copyOf(value, value.length);
    }
    return nativeStore.get(ownedKey.copy());
  }

  private synchronized void put(byte[] key, byte[] value) {
    pending.put(new BytesKey(key), Arrays.copyOf(value, value.length));
  }

  private synchronized void delete(byte[] key) {
    pending.put(new BytesKey(key), null);
  }

  private void requireProgressIdentity(PathStateRootMetadata metadata) throws IOException {
    if (metadata.getKind() != kind
        || !Arrays.equals(metadata.getFormatDigest(), manifestDigest)) {
      throw new IOException("path-state native progress identity mismatch");
    }
    if (expectedLayer != null && !Arrays.equals(metadata.encode(), expectedLayer.encode())) {
      throw new IOException("path-state native layer progress mismatch");
    }
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
