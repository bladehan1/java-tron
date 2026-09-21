package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Crash-recoverable publication boundary for the first durable path-state BASE. */
public final class PathStateBasePublication {

  public static final String INTENT_FILE = "INTENT";

  private final PathStateStoreManifest manifest;
  private final PathStateCurrentStore currentStore;
  private final Path baseDirectory;
  private final Path intentPath;
  private final Path metadataPath;
  private final FaultHook faultHook;

  public PathStateBasePublication(PathStateStoreManifest manifest) {
    this(manifest, stage -> { });
  }

  PathStateBasePublication(PathStateStoreManifest manifest, FaultHook faultHook) {
    this.manifest = Objects.requireNonNull(manifest, "manifest");
    this.currentStore = new PathStateCurrentStore(manifest);
    this.baseDirectory = manifest.getBaseDirectory();
    this.intentPath = baseDirectory.resolve(INTENT_FILE);
    this.metadataPath = baseDirectory.resolve(PathStateCurrentStore.METADATA_FILE);
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  /** Publishes a first BASE through intent, native marker, metadata, CURRENT, and retire stages. */
  public synchronized PathStateRootMetadata publish(PathStateNodeStoreSet stores,
      PathStateRootMetadata metadata) throws IOException {
    PathStateNodeStoreSet nodeStores = Objects.requireNonNull(stores, "stores");
    Path expectedNodes = baseDirectory.resolve(PathStateNodeStoreSet.NODES_DIRECTORY)
        .toAbsolutePath().normalize();
    if (!expectedNodes.equals(nodeStores.getDirectory().toAbsolutePath().normalize())) {
      throw new IllegalArgumentException("path-state BASE node database directory mismatch");
    }
    PathStateRootMetadata base = requireBase(metadata);
    if (currentStore.isInitialized()) {
      throw new IOException("path-state initial BASE is already published");
    }
    PathStateMetadataFile.publishImmutable(intentPath, base);
    faultHook.after(Stage.AFTER_INTENT);
    nodeStores.commit(base);
    faultHook.after(Stage.AFTER_NODE_PROGRESS);
    PathStateMetadataFile.publishImmutable(metadataPath, base);
    faultHook.after(Stage.AFTER_METADATA);
    PathStateRootMetadata current = currentStore.publishBase(base);
    faultHook.after(Stage.AFTER_CURRENT);
    PathStateMetadataFile.deleteDurable(intentPath);
    faultHook.after(Stage.AFTER_RETIRE);
    return current;
  }

  /** Reconciles one interrupted first-BASE publication and returns the durable action taken. */
  public synchronized RecoveryAction recover() throws IOException {
    if (!Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)) {
      verifySettledState();
      return RecoveryAction.NONE;
    }
    PathStateRootMetadata intent = requireBase(PathStateMetadataFile.load(intentPath));
    PathStateRootMetadata progress = PathStateNodeStoreSet.loadProgress(baseDirectory, manifest);
    if (progress == null) {
      if (Files.exists(metadataPath, LinkOption.NOFOLLOW_LINKS) || currentStore.isInitialized()) {
        throw new IOException("path-state BASE authority exists without native progress");
      }
      PathStateMetadataFile.deleteDurable(intentPath);
      return RecoveryAction.ROLLED_BACK_INTENT;
    }
    requireSame(intent, progress, "path-state BASE intent and native progress differ");
    currentStore.publishBase(intent);
    PathStateMetadataFile.deleteDurable(intentPath);
    verifySettledState();
    return RecoveryAction.COMPLETED_PUBLICATION;
  }

  private void verifySettledState() throws IOException {
    PathStateRootMetadata progress = PathStateNodeStoreSet.loadProgress(baseDirectory, manifest);
    boolean metadataExists = Files.exists(metadataPath, LinkOption.NOFOLLOW_LINKS);
    if (!currentStore.isInitialized()) {
      if (metadataExists || progress != null) {
        throw new IOException("path-state BASE has orphaned authority without CURRENT");
      }
      return;
    }
    currentStore.current();
    PathStateRootMetadata metadata = requireBase(PathStateMetadataFile.load(metadataPath));
    if (progress == null) {
      throw new IOException("path-state BASE native progress is missing");
    }
    requireSame(metadata, progress, "path-state BASE metadata and native progress differ");
  }

  private PathStateRootMetadata requireBase(PathStateRootMetadata metadata) throws IOException {
    PathStateRootMetadata base = Objects.requireNonNull(metadata, "metadata");
    if (base.getKind() != Kind.BASE
        || !Arrays.equals(base.getFormatDigest(), manifest.getIdentityDigest())) {
      throw new IOException("path-state BASE publication identity mismatch");
    }
    return base;
  }

  private static void requireSame(PathStateRootMetadata expected,
      PathStateRootMetadata actual, String error) throws IOException {
    if (!Arrays.equals(expected.encode(), actual.encode())) {
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
}
