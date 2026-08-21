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

  private final Path directory;
  private final PathStateParticipantScope scope;
  private final Map<String, PathNodeStore> participantStores = new LinkedHashMap<>();
  private final PathStateNativeNodeStore nativeStore;
  private final PathNodeStore superStore;
  private boolean rootClaimed;
  private boolean closed;

  private PathStateNodeStoreSet(Path directory, PathStateStoreManifest manifest)
      throws IOException {
    this.directory = directory.resolve(NODES_DIRECTORY);
    this.scope = new PathStateCanonicalizer().participantScope();
    nativeStore = PathStateNativeNodeStore.open(this.directory, manifest.getEngine());
    for (PathStateParticipant participant : scope.getParticipants()) {
      participantStores.put(participant.getDbName(),
          new NamespacedNodeStore(nativeStore, participant.getStoreId()));
    }
    superStore = new NamespacedNodeStore(nativeStore, 0);
  }

  public static PathStateNodeStoreSet openBase(PathStateStoreManifest manifest)
      throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    requireUnsealed(admitted.getBaseDirectory());
    return new PathStateNodeStoreSet(admitted.getBaseDirectory(), admitted);
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
    return new PathStateNodeStoreSet(layerDirectory, admitted);
  }

  /** Claims these databases for one in-process trie owner. */
  public synchronized PathStateRoot createRoot() {
    requireOpen();
    if (rootClaimed) {
      throw new IllegalStateException("path-state node database set already has a trie owner");
    }
    rootClaimed = true;
    return new PathStateRoot(scope, participant -> participantStores.get(participant.getDbName()),
        superStore);
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

  private static void requireUnsealed(Path directory) throws IOException {
    Path metadata = directory.resolve(PathStateCurrentStore.METADATA_FILE);
    if (Files.exists(metadata, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state node database set is sealed by immutable metadata: "
          + directory);
    }
  }

  private static final class NamespacedNodeStore implements PathNodeStore {

    private final PathStateNativeNodeStore nativeStore;
    private final int storeId;

    private NamespacedNodeStore(PathStateNativeNodeStore nativeStore, int storeId) {
      this.nativeStore = nativeStore;
      this.storeId = storeId;
    }

    @Override
    public byte[] get(byte[] path) {
      return nativeStore.get(key(path));
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      byte[] value = Arrays.copyOf(Objects.requireNonNull(encodedNode, "encodedNode"),
          encodedNode.length);
      if (value.length == 0) {
        throw new IllegalArgumentException("encodedNode must not be empty");
      }
      nativeStore.put(key(path), value);
    }

    @Override
    public void delete(byte[] path) {
      nativeStore.delete(key(path));
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
}
