package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

  static final int MAX_VALIDATION_LAYERS = 65_536;

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

  /** Atomically switches CURRENT to an exact durable ancestor inside the reversible window. */
  synchronized PathStateRootMetadata switchToAncestor(PathStateRootMetadata target,
      PathStateLayerLimits limits) throws IOException {
    return switchToAncestor(target, limits, temporary -> { });
  }

  synchronized PathStateRootMetadata switchToAncestor(PathStateRootMetadata target,
      PathStateLayerLimits limits, PathStateMetadataFile.FaultHook faultHook) throws IOException {
    List<PathStateRootMetadata> suffix = layersAboveAncestor(target, limits);
    PathStateRootMetadata admittedTarget = Objects.requireNonNull(target, "target");
    if (suffix.isEmpty()) {
      return current();
    }
    PathStateMetadataFile.replaceCurrent(currentPath, admittedTarget,
        Objects.requireNonNull(faultHook, "faultHook"));
    return current();
  }

  synchronized List<PathStateRootMetadata> layersAboveAncestor(PathStateRootMetadata target,
      PathStateLayerLimits limits) throws IOException {
    PathStateRootMetadata admittedTarget = Objects.requireNonNull(target, "target");
    PathStateLayerLimits admittedLimits = Objects.requireNonNull(limits, "limits");
    requireFormat(admittedTarget);
    PathStateRootMetadata head = current();
    if (same(head, admittedTarget)) {
      verifyTargetState(admittedTarget);
      return Collections.emptyList();
    }
    if (admittedTarget.getBlockNumber() >= head.getBlockNumber()) {
      throw new IOException("path-state canonical switch target is not an ancestor");
    }

    PathStateRootMetadata base = requireKind(PathStateMetadataFile.load(basePath()), Kind.BASE);
    requireFormat(base);
    List<PathStateRootMetadata> suffix = new ArrayList<>();
    PathStateRootMetadata cursor = head;
    for (int depth = 1; depth <= admittedLimits.getMaxLayers(); depth++) {
      suffix.add(cursor);
      cursor = parentOf(cursor, base);
      if (same(cursor, admittedTarget)) {
        verifyTargetState(admittedTarget);
        return Collections.unmodifiableList(suffix);
      }
      if (cursor.getKind() == Kind.BASE) {
        break;
      }
    }
    throw new IOException("path-state canonical switch exceeds the reversible window");
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
    return manifest.getLayerDirectory(blockNumber, blockHash).resolve(METADATA_FILE);
  }

  private PathStateRootMetadata parentOf(PathStateRootMetadata child,
      PathStateRootMetadata base) throws IOException {
    if (child.getKind() != Kind.LAYER || child.getBlockNumber() == 0) {
      throw new IOException("path-state canonical switch reached an invalid parent boundary");
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
    return parent;
  }

  private void verifyTargetState(PathStateRootMetadata target) throws IOException {
    Path owner = target.getKind() == Kind.BASE ? manifest.getBaseDirectory()
        : manifest.getLayerDirectory(target.getBlockNumber(), target.getBlockHash());
    PathStateRootMetadata progress = PathStateNodeStoreSet.loadProgress(owner, manifest);
    if (progress == null || !same(target, progress)) {
      throw new IOException("path-state canonical switch target has invalid native progress");
    }
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openPublished(manifest, target)) {
      PathStateRoot root = stores.createRoot();
      root.verifyNodeStores();
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new IOException("path-state canonical switch target is corrupt", e);
    }
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

}
