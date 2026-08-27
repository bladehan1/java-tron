package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Crash-recoverable materialization of a canonical layer prefix into the single durable BASE. */
public final class PathStateBaseCompaction {

  public static final String INTENT_FILE = "BASE_FLUSH_INTENT";
  static final String NEXT_DIRECTORY = "base.next";
  static final String PREVIOUS_DIRECTORY = "base.previous";

  private final PathStateStoreManifest manifest;
  private final PathStateLayerLimits limits;
  private final FaultHook faultHook;
  private final Path root;
  private final Path intentPath;
  private final Path basePath;
  private final Path nextPath;
  private final Path previousPath;

  public PathStateBaseCompaction(PathStateStoreManifest manifest,
      PathStateLayerLimits limits) {
    this(manifest, limits, stage -> { });
  }

  PathStateBaseCompaction(PathStateStoreManifest manifest, PathStateLayerLimits limits,
      FaultHook faultHook) {
    this.manifest = Objects.requireNonNull(manifest, "manifest");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    this.root = manifest.getDirectory();
    this.intentPath = root.resolve(INTENT_FILE);
    this.basePath = manifest.getBaseDirectory();
    this.nextPath = root.resolve(NEXT_DIRECTORY);
    this.previousPath = root.resolve(PREVIOUS_DIRECTORY);
  }

  /** Materializes canonical layers through an exact non-head target, one prefix layer at a time. */
  public synchronized PathStateRootMetadata compactThrough(long blockNumber, byte[] blockHash)
      throws IOException {
    byte[] targetHash = Arrays.copyOf(Objects.requireNonNull(blockHash, "blockHash"),
        blockHash.length);
    recover();
    while (true) {
      PathStateCurrentStore currentStore = new PathStateCurrentStore(manifest);
      PathStateRootMetadata base = loadBase();
      if (base.getBlockNumber() == blockNumber
          && Arrays.equals(base.getBlockHash(), targetHash)) {
        return base;
      }
      PathStateRootMetadata head = currentStore.current();
      PathStateRootMetadata target = currentStore.findAncestor(blockNumber, targetHash, limits);
      if (target.getKind() != Kind.LAYER || target.getBlockNumber() >= head.getBlockNumber()) {
        throw new IOException("path-state base flush must retain a newer reversible head");
      }
      PathStateRootMetadata first = currentStore.firstLayerAfterBaseToward(target, limits);
      PathStateRootMetadata replacement = asBase(first);
      PathStateMetadataFile.publishImmutable(intentPath, replacement);
      faultHook.after(Stage.AFTER_INTENT);
      finish(replacement);
    }
  }

  /** Completes an interrupted one-layer BASE replacement and becomes a zero-action retry. */
  public synchronized RecoveryAction recover() throws IOException {
    if (!Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)) {
      if (Files.exists(nextPath, LinkOption.NOFOLLOW_LINKS)
          || Files.exists(previousPath, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("path-state base flush has orphan replacement directories");
      }
      return RecoveryAction.NONE;
    }
    PathStateRootMetadata replacement = requireBase(PathStateMetadataFile.load(intentPath));
    finish(replacement);
    return RecoveryAction.COMPLETED_COMPACTION;
  }

  private void finish(PathStateRootMetadata replacement) throws IOException {
    PathStateRootMetadata installed = metadataIfPresent(basePath);
    if (installed == null || !same(installed, replacement)) {
      if (Files.exists(previousPath, LinkOption.NOFOLLOW_LINKS)) {
        if (Files.exists(basePath, LinkOption.NOFOLLOW_LINKS)) {
          throw new IOException("path-state base flush has ambiguous directory authority");
        }
        verifyPreparedNext(replacement);
      } else {
        if (installed == null) {
          throw new IOException("path-state base flush lost the previous BASE");
        }
        deleteDirectory(nextPath);
        buildNext(replacement);
        faultHook.after(Stage.AFTER_NEXT);
        moveDirectory(basePath, previousPath);
        faultHook.after(Stage.AFTER_OLD_BASE);
      }
      moveDirectory(nextPath, basePath);
      faultHook.after(Stage.AFTER_BASE);
    }
    verifyInstalledBase(replacement);
    deleteDirectory(manifest.getLayerDirectory(
        replacement.getBlockNumber(), replacement.getBlockHash()));
    faultHook.after(Stage.AFTER_LAYER_RETIRE);
    deleteDirectory(previousPath);
    faultHook.after(Stage.AFTER_PREVIOUS_RETIRE);
    PathStateMetadataFile.deleteDurable(intentPath);
    faultHook.after(Stage.AFTER_RETIRE);
    new PathStateCurrentStore(manifest).current();
  }

  private void buildNext(PathStateRootMetadata replacement) throws IOException {
    Path layerPath = manifest.getLayerDirectory(
        replacement.getBlockNumber(), replacement.getBlockHash());
    PathStateRootMetadata layer = PathStateMetadataFile.load(
        layerPath.resolve(PathStateCurrentStore.METADATA_FILE));
    if (layer.getKind() != Kind.LAYER
        || layer.getBlockNumber() != replacement.getBlockNumber()
        || !Arrays.equals(layer.getBlockHash(), replacement.getBlockHash())
        || !Arrays.equals(layer.getStateRoot(), replacement.getStateRoot())) {
      throw new IOException("path-state base flush source layer identity mismatch");
    }
    Files.createDirectory(nextPath);
    try (PathStateNodeStoreSet source = PathStateNodeStoreSet.openPublished(manifest, layer);
        PathStateNodeStoreSet destination = PathStateNodeStoreSet.beginBaseAt(manifest, nextPath)) {
      PathStateRoot sourceRoot = source.createRoot();
      sourceRoot.verifyNodeStores();
      List<PathStateRoot.LeafRecord> leaves = new ArrayList<>(source.leafRecords());
      PathStateRoot destinationRoot = destination.initializeBase(leaves, layer.getStateRoot());
      if (!Arrays.equals(destinationRoot.rootHash(), replacement.getStateRoot())) {
        throw new IOException("path-state replacement BASE root mismatch");
      }
      destination.commit(replacement);
    }
    PathStateMetadataFile.publishImmutable(
        nextPath.resolve(PathStateCurrentStore.METADATA_FILE), replacement);
    verifyPreparedNext(replacement);
  }

  private void verifyPreparedNext(PathStateRootMetadata replacement) throws IOException {
    PathStateMetadataFile.requireExact(
        nextPath.resolve(PathStateCurrentStore.METADATA_FILE), replacement);
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.beginBaseAt(manifest, nextPath)) {
      PathStateRoot restored = stores.createRoot();
      if (!Arrays.equals(restored.rootHash(), replacement.getStateRoot())) {
        throw new IOException("path-state prepared BASE root mismatch");
      }
      restored.verifyNodeStores();
    }
  }

  private void verifyInstalledBase(PathStateRootMetadata replacement) throws IOException {
    PathStateMetadataFile.requireExact(
        basePath.resolve(PathStateCurrentStore.METADATA_FILE), replacement);
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot restored = stores.createRoot();
      if (!Arrays.equals(restored.rootHash(), replacement.getStateRoot())) {
        throw new IOException("path-state installed BASE root mismatch");
      }
      restored.verifyNodeStores();
    }
  }

  private PathStateRootMetadata loadBase() throws IOException {
    return requireBase(PathStateMetadataFile.load(
        basePath.resolve(PathStateCurrentStore.METADATA_FILE)));
  }

  private PathStateRootMetadata metadataIfPresent(Path directory) throws IOException {
    Path metadata = directory.resolve(PathStateCurrentStore.METADATA_FILE);
    return Files.exists(metadata, LinkOption.NOFOLLOW_LINKS)
        ? PathStateMetadataFile.load(metadata) : null;
  }

  private PathStateRootMetadata asBase(PathStateRootMetadata layer) {
    return PathStateRootMetadata.base(layer.getBlockNumber(), layer.getBlockHash(),
        layer.getParentHash(), layer.getTimestamp(), layer.getPhase(),
        layer.getFormatDigest(), layer.getStateRoot(), layer.getPayloadDigest());
  }

  private PathStateRootMetadata requireBase(PathStateRootMetadata metadata) throws IOException {
    if (metadata.getKind() != Kind.BASE
        || !Arrays.equals(metadata.getFormatDigest(), manifest.getIdentityDigest())) {
      throw new IOException("path-state base flush metadata identity mismatch");
    }
    return metadata;
  }

  private void moveDirectory(Path source, Path target) throws IOException {
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new IOException("path-state base flush requires atomic directory move", unsupported);
    }
    PathStateMetadataFile.syncDirectory(root);
  }

  private void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
        || Files.isSymbolicLink(directory)) {
      throw new IOException("path-state base flush refuses non-directory: " + directory);
    }
    List<Path> entries = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.forEach(entries::add);
    }
    for (Path entry : entries) {
      if (Files.isSymbolicLink(entry)) {
        throw new IOException("path-state base flush refuses symbolic links: " + entry);
      }
    }
    entries.sort(Comparator.reverseOrder());
    for (Path entry : entries) {
      Files.deleteIfExists(entry);
    }
    PathStateMetadataFile.syncDirectory(root);
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return Arrays.equals(left.encode(), right.encode());
  }

  public enum RecoveryAction {
    NONE,
    COMPLETED_COMPACTION
  }

  enum Stage {
    AFTER_INTENT,
    AFTER_NEXT,
    AFTER_OLD_BASE,
    AFTER_BASE,
    AFTER_LAYER_RETIRE,
    AFTER_PREVIOUS_RETIRE,
    AFTER_RETIRE
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage) throws IOException;
  }
}
