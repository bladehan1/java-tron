package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Crash-recoverable publication boundary for one immutable reversible path-state layer. */
public final class PathStateLayerPublication {

  public static final String INTENT_FILE = "INTENT";

  private final PathStateStoreManifest manifest;
  private final PathStateCurrentStore currentStore;
  private final PathStateLayerLimits limits;
  private final FaultHook faultHook;

  public PathStateLayerPublication(PathStateStoreManifest manifest) {
    this(manifest, PathStateLayerLimits.defaults());
  }

  public PathStateLayerPublication(PathStateStoreManifest manifest,
      PathStateLayerLimits limits) {
    this(manifest, limits, stage -> { });
  }

  PathStateLayerPublication(PathStateStoreManifest manifest, FaultHook faultHook) {
    this(manifest, PathStateLayerLimits.defaults(), faultHook);
  }

  PathStateLayerPublication(PathStateStoreManifest manifest, PathStateLayerLimits limits,
      FaultHook faultHook) {
    this.manifest = Objects.requireNonNull(manifest, "manifest");
    this.currentStore = new PathStateCurrentStore(manifest);
    this.limits = Objects.requireNonNull(limits, "limits");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  /** Publishes one layer through intent, native progress, metadata, CURRENT, and retire stages. */
  public synchronized PathStateRootMetadata publish(PathStateNodeStoreSet stores,
      PathStateRootMetadata metadata) throws IOException {
    PathStateRootMetadata layer = requireLayer(metadata);
    Path directory = layerDirectory(layer);
    Path expectedNodes = directory.resolve(PathStateNodeStoreSet.NODES_DIRECTORY)
        .toAbsolutePath().normalize();
    PathStateNodeStoreSet nodeStores = Objects.requireNonNull(stores, "stores");
    if (!expectedNodes.equals(nodeStores.getDirectory().toAbsolutePath().normalize())) {
      throw new IllegalArgumentException("path-state LAYER node database directory mismatch");
    }
    requireCurrentParentOrChild(layer);
    nodeStores.resolvePendingLeafValues();
    nodeStores.releaseParentReadHandles();
    limits.verifyAdmission(manifest, directory, layer,
        nodeStores.projectedLogicalBytes(layer));

    Path intent = directory.resolve(INTENT_FILE);
    PathStateMetadataFile.publishImmutable(intent, layer);
    faultHook.after(Stage.AFTER_INTENT);
    nodeStores.commit(layer);
    faultHook.after(Stage.AFTER_NODE_PROGRESS);
    PathStateMetadataFile.publishImmutable(
        directory.resolve(PathStateCurrentStore.METADATA_FILE), layer);
    faultHook.after(Stage.AFTER_METADATA);
    PathStateRootMetadata current = currentStore.current();
    if (!same(current, layer)) {
      current = currentStore.appendLayer(layer);
    }
    faultHook.after(Stage.AFTER_CURRENT);
    PathStateMetadataFile.deleteDurable(intent);
    faultHook.after(Stage.AFTER_RETIRE);
    return current;
  }

  /** Reconciles the sole unfinished layer intent and verifies all settled layer authorities. */
  public synchronized RecoveryAction recover() throws IOException {
    limits.verifyExisting(manifest);
    List<LayerState> layers = scanLayers();
    LayerState pending = null;
    for (LayerState layer : layers) {
      layer.verifySettledOrIntent();
      if (layer.intent != null) {
        if (pending != null) {
          throw new IOException("multiple unfinished path-state layer intents");
        }
        pending = layer;
      }
    }
    if (pending == null) {
      verifyCurrentProgress();
      return RecoveryAction.NONE;
    }

    PathStateRootMetadata intent = pending.intent;
    PathStateRootMetadata current = currentStore.current();
    if (!same(current, intent) && !isParent(current, intent)) {
      throw new IOException("path-state layer intent no longer extends CURRENT");
    }
    if (pending.progress == null) {
      if (same(current, intent)) {
        throw new IOException("path-state CURRENT layer exists without native progress");
      }
      PathStateMetadataFile.deleteDurable(pending.intentPath);
      verifyCurrentProgress();
      limits.verifyExisting(manifest);
      return RecoveryAction.ROLLED_BACK_INTENT;
    }

    requireSame(intent, pending.progress,
        "path-state layer intent and native progress differ");
    if (!same(current, intent)) {
      currentStore.appendLayer(intent);
    }
    PathStateMetadataFile.deleteDurable(pending.intentPath);
    verifyCurrentProgress();
    limits.verifyExisting(manifest);
    return RecoveryAction.COMPLETED_PUBLICATION;
  }

  private List<LayerState> scanLayers() throws IOException {
    List<Path> entries = new ArrayList<>();
    try (Stream<Path> paths = Files.list(manifest.getLayersDirectory())) {
      paths.sorted(Comparator.comparing(path -> path.getFileName().toString()))
          .forEach(entries::add);
    }
    List<LayerState> layers = new ArrayList<>(entries.size());
    for (Path entry : entries) {
      if (!Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(entry)) {
        throw new IOException("path-state layer entry is not a direct directory: " + entry);
      }
      layers.add(new LayerState(entry));
    }
    return layers;
  }

  private void verifyCurrentProgress() throws IOException {
    PathStateRootMetadata current = currentStore.current();
    Path owner = current.getKind() == Kind.BASE ? manifest.getBaseDirectory()
        : layerDirectory(current);
    PathStateRootMetadata progress = PathStateNodeStoreSet.loadProgress(owner, manifest);
    if (progress == null) {
      throw new IOException("path-state CURRENT has no native progress");
    }
    requireSame(current, progress, "path-state CURRENT and native progress differ");
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openPublished(manifest, current)) {
      stores.createRoot();
    }
  }

  private void requireCurrentParentOrChild(PathStateRootMetadata layer) throws IOException {
    PathStateRootMetadata current = currentStore.current();
    if (!same(current, layer) && !isParent(current, layer)) {
      throw new IOException("path-state LAYER publication does not extend CURRENT");
    }
  }

  private PathStateRootMetadata requireLayer(PathStateRootMetadata metadata) throws IOException {
    PathStateRootMetadata layer = Objects.requireNonNull(metadata, "metadata");
    if (layer.getKind() != Kind.LAYER
        || !Arrays.equals(layer.getFormatDigest(), manifest.getIdentityDigest())) {
      throw new IOException("path-state LAYER publication identity mismatch");
    }
    return layer;
  }

  private Path layerDirectory(PathStateRootMetadata metadata) {
    return manifest.getLayerDirectory(metadata.getBlockNumber(), metadata.getBlockHash());
  }

  private static boolean isParent(PathStateRootMetadata parent, PathStateRootMetadata child) {
    return child.getBlockNumber() == parent.getBlockNumber() + 1
        && Arrays.equals(child.getParentHash(), parent.getBlockHash())
        && Arrays.equals(child.getParentStateRoot(), parent.getStateRoot());
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return Arrays.equals(left.encode(), right.encode());
  }

  private static void requireSame(PathStateRootMetadata expected,
      PathStateRootMetadata actual, String error) throws IOException {
    if (!same(expected, actual)) {
      throw new IOException(error);
    }
  }

  public enum RecoveryAction {
    NONE,
    ROLLED_BACK_INTENT,
    COMPLETED_PUBLICATION
  }

  enum Stage {
    AFTER_INTENT,
    AFTER_NODE_PROGRESS,
    AFTER_METADATA,
    AFTER_CURRENT,
    AFTER_RETIRE
  }

  @FunctionalInterface
  interface FaultHook {

    void after(Stage stage) throws IOException;
  }

  private final class LayerState {

    private final Path directory;
    private final Path intentPath;
    private final PathStateRootMetadata intent;
    private final PathStateRootMetadata metadata;
    private final PathStateRootMetadata progress;

    private LayerState(Path directory) throws IOException {
      this.directory = directory;
      this.intentPath = directory.resolve(INTENT_FILE);
      Path metadataPath = directory.resolve(PathStateCurrentStore.METADATA_FILE);
      this.intent = Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)
          ? requireLayer(PathStateMetadataFile.load(intentPath)) : null;
      this.metadata = Files.exists(metadataPath, LinkOption.NOFOLLOW_LINKS)
          ? requireLayer(PathStateMetadataFile.load(metadataPath)) : null;
      this.progress = PathStateNodeStoreSet.loadProgress(directory, manifest);
    }

    private void verifySettledOrIntent() throws IOException {
      PathStateRootMetadata identity = intent != null ? intent : metadata;
      if (identity != null && !directory.equals(layerDirectory(identity))) {
        throw new IOException("path-state layer record is in a noncanonical directory");
      }
      if (metadata != null) {
        if (progress == null) {
          throw new IOException("path-state layer metadata exists without native progress");
        }
        requireSame(metadata, progress,
            "path-state layer metadata and native progress differ");
      } else if (progress != null && intent == null) {
        throw new IOException("path-state layer has orphaned native progress");
      }
      if (intent != null && metadata != null) {
        requireSame(intent, metadata,
            "path-state layer intent and metadata differ");
      }
    }
  }
}
