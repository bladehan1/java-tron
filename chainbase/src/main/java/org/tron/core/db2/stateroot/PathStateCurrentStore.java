package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/**
 * Current-head authority over one durable base and its immutable reversible metadata chain.
 *
 * <p>This component intentionally exposes only the verified current record. Traversal of older
 * layers is an internal startup integrity check, not a historical-root query API.
 */
public final class PathStateCurrentStore {

  public static final String METADATA_FILE = "METADATA";
  public static final String CURRENT_FILE = "CURRENT";

  private static final int MAX_VALIDATION_LAYERS = 65_536;

  private final PathStateStoreManifest manifest;
  private final Path currentPath;

  public PathStateCurrentStore(PathStateStoreManifest manifest) {
    this.manifest = Objects.requireNonNull(manifest, "manifest");
    this.currentPath = manifest.getDirectory().resolve(CURRENT_FILE);
  }

  public boolean isInitialized() {
    return Files.exists(currentPath, LinkOption.NOFOLLOW_LINKS);
  }

  /** Publishes the first durable base and then atomically makes it current. */
  public synchronized PathStateRootMetadata publishBase(PathStateRootMetadata base)
      throws IOException {
    PathStateRootMetadata metadata = requireKind(base, Kind.BASE);
    requireFormat(metadata);
    if (isInitialized()) {
      PathStateRootMetadata current = current();
      if (!Arrays.equals(current.encode(), metadata.encode())) {
        throw new IOException("path-state CURRENT already identifies another root");
      }
      return current;
    }
    PathStateMetadataFile.publishImmutable(basePath(), metadata);
    PathStateMetadataFile.replaceCurrent(currentPath, metadata);
    return current();
  }

  /** Publishes one immutable child layer before atomically advancing CURRENT. */
  public synchronized PathStateRootMetadata appendLayer(PathStateRootMetadata layer)
      throws IOException {
    PathStateRootMetadata child = requireKind(layer, Kind.LAYER);
    requireFormat(child);
    PathStateRootMetadata parent = current();
    if (same(parent, child)) {
      return parent;
    }
    requireChild(parent, child);
    PathStateMetadataFile.publishImmutable(layerPath(child), child);
    PathStateMetadataFile.replaceCurrent(currentPath, child);
    return current();
  }

  /** Loads CURRENT and verifies that every referenced layer reaches the single durable base. */
  public synchronized PathStateRootMetadata current() throws IOException {
    PathStateRootMetadata base = requireKind(PathStateMetadataFile.load(basePath()), Kind.BASE);
    requireFormat(base);
    PathStateRootMetadata head = PathStateMetadataFile.load(currentPath);
    requireFormat(head);
    PathStateRootMetadata storedHead = PathStateMetadataFile.load(metadataPath(head));
    requireFormat(storedHead);
    requireSame(head, storedHead, "CURRENT metadata differs from its immutable record");

    PathStateRootMetadata child = head;
    int layers = 0;
    while (child.getKind() == Kind.LAYER) {
      if (++layers > MAX_VALIDATION_LAYERS) {
        throw new IOException("path-state layer chain exceeds the validation safety limit");
      }
      if (child.getBlockNumber() == 0) {
        throw new IOException("path-state layer zero cannot have a parent");
      }
      PathStateRootMetadata parent;
      if (base.getBlockNumber() == child.getBlockNumber() - 1) {
        parent = base;
      } else {
        parent = requireKind(PathStateMetadataFile.load(
            layerPath(child.getBlockNumber() - 1, child.getParentHash())), Kind.LAYER);
        requireFormat(parent);
      }
      requireChild(parent, child);
      child = parent;
    }
    requireSame(base, child, "path-state layer chain does not terminate at the durable base");
    return head;
  }

  private Path metadataPath(PathStateRootMetadata metadata) {
    return metadata.getKind() == Kind.BASE ? basePath() : layerPath(metadata);
  }

  private Path basePath() {
    return manifest.getBaseDirectory().resolve(METADATA_FILE);
  }

  private Path layerPath(PathStateRootMetadata metadata) {
    return layerPath(metadata.getBlockNumber(), metadata.getBlockHash());
  }

  private Path layerPath(long blockNumber, byte[] blockHash) {
    String directory = String.format(Locale.ROOT, "%020d-%s", blockNumber, hex(blockHash));
    return manifest.getLayersDirectory().resolve(directory).resolve(METADATA_FILE);
  }

  private static PathStateRootMetadata requireKind(PathStateRootMetadata metadata, Kind kind) {
    PathStateRootMetadata present = Objects.requireNonNull(metadata, "metadata");
    if (present.getKind() != kind) {
      throw new IllegalArgumentException("expected path-state " + kind + " metadata");
    }
    return present;
  }

  private void requireFormat(PathStateRootMetadata metadata) throws IOException {
    if (!Arrays.equals(metadata.getFormatDigest(), manifest.getIdentityDigest())) {
      throw new IOException("path-state metadata manifest identity mismatch");
    }
  }

  private static void requireChild(PathStateRootMetadata parent, PathStateRootMetadata child)
      throws IOException {
    if (child.getBlockNumber() != parent.getBlockNumber() + 1
        || !Arrays.equals(child.getParentHash(), parent.getBlockHash())
        || !Arrays.equals(child.getParentStateRoot(), parent.getStateRoot())) {
      throw new IOException("path-state layer does not extend CURRENT");
    }
  }

  private static void requireSame(PathStateRootMetadata expected,
      PathStateRootMetadata actual, String error) throws IOException {
    if (!same(expected, actual)) {
      throw new IOException(error);
    }
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return Arrays.equals(left.encode(), right.encode());
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte current : value) {
      encoded.append(Character.forDigit(current >>> 4 & 0xf, 16));
      encoded.append(Character.forDigit(current & 0xf, 16));
    }
    return encoded.toString();
  }
}
