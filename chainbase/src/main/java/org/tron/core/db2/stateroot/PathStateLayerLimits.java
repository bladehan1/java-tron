package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/** Explicit count and logical-byte admission limits for current-only reversible layers. */
public final class PathStateLayerLimits {

  public static final int DEFAULT_MAX_LAYERS = 128;
  public static final long DEFAULT_MAX_LOGICAL_BYTES = 1L << 40;

  private final int maxLayers;
  private final long maxLogicalBytes;

  public PathStateLayerLimits(int maxLayers, long maxLogicalBytes) {
    if (maxLayers <= 0 || maxLogicalBytes <= 0) {
      throw new IllegalArgumentException("path-state layer limits must be positive");
    }
    this.maxLayers = maxLayers;
    this.maxLogicalBytes = maxLogicalBytes;
  }

  public static PathStateLayerLimits defaults() {
    return new PathStateLayerLimits(DEFAULT_MAX_LAYERS, DEFAULT_MAX_LOGICAL_BYTES);
  }

  void verifyCanBegin(PathStateStoreManifest manifest, Path candidate) throws IOException {
    Usage usage = usageExcluding(manifest, candidate);
    try {
      requireWithin(Math.addExact(usage.layers, 1), usage.logicalBytes);
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state layer count overflow", overflow);
    }
  }

  void verifyAdmission(PathStateStoreManifest manifest, Path candidate,
      PathStateRootMetadata metadata, long nativeLogicalBytes) throws IOException {
    Usage usage = usageExcluding(manifest, candidate);
    long candidateBytes;
    try {
      candidateBytes = Math.addExact(nativeLogicalBytes, metadata.encode().length);
      requireWithin(Math.addExact(usage.layers, 1),
          Math.addExact(usage.logicalBytes, candidateBytes));
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state layer limit accounting overflow", overflow);
    }
  }

  void verifyExisting(PathStateStoreManifest manifest) throws IOException {
    Usage usage = usageExcluding(manifest, null);
    requireWithin(usage.layers, usage.logicalBytes);
  }

  public int getMaxLayers() {
    return maxLayers;
  }

  public long getMaxLogicalBytes() {
    return maxLogicalBytes;
  }

  private Usage usageExcluding(PathStateStoreManifest manifest, Path excluded)
      throws IOException {
    PathStateStoreManifest admitted = Objects.requireNonNull(manifest, "manifest");
    Path omitted = excluded == null ? null : excluded.toAbsolutePath().normalize();
    int layers = 0;
    long logicalBytes = 0;
    try (Stream<Path> paths = Files.list(admitted.getLayersDirectory())) {
      for (Path entry : (Iterable<Path>) paths::iterator) {
        if (!Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(entry)) {
          throw new IOException("path-state layer entry is not a direct directory: " + entry);
        }
        if (omitted != null && omitted.equals(entry.toAbsolutePath().normalize())) {
          continue;
        }
        Path metadataPath = entry.resolve(PathStateCurrentStore.METADATA_FILE);
        Path intentPath = entry.resolve(PathStateLayerPublication.INTENT_FILE);
        PathStateRootMetadata metadata = Files.exists(metadataPath, LinkOption.NOFOLLOW_LINKS)
            ? requireLayer(admitted, entry, PathStateMetadataFile.load(metadataPath)) : null;
        PathStateRootMetadata intent = Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)
            ? requireLayer(admitted, entry, PathStateMetadataFile.load(intentPath)) : null;
        PathStateRootMetadata progress = PathStateNodeStoreSet.loadProgress(entry, admitted);
        Long nativeBytes = PathStateNodeStoreSet.loadLogicalBytes(entry, admitted);
        if ((progress == null) != (nativeBytes == null)) {
          throw new IOException("path-state layer progress and logical bytes marker differ");
        }
        if (metadata != null) {
          requireSame(metadata, progress,
              "path-state layer metadata and native progress differ");
          layers = Math.addExact(layers, 1);
          logicalBytes = Math.addExact(logicalBytes,
              Math.addExact(nativeBytes, metadata.encode().length));
        } else if (intent != null && progress != null) {
          requireSame(intent, progress,
              "path-state layer intent and native progress differ");
          layers = Math.addExact(layers, 1);
          logicalBytes = Math.addExact(logicalBytes,
              Math.addExact(nativeBytes, intent.encode().length));
        } else if (progress != null) {
          throw new IOException("path-state layer has orphaned native progress");
        }
      }
    } catch (ArithmeticException overflow) {
      throw new IOException("path-state layer limit accounting overflow", overflow);
    }
    return new Usage(layers, logicalBytes);
  }

  private void requireWithin(int layers, long logicalBytes) throws IOException {
    if (layers > maxLayers) {
      throw new IOException("path-state layer count limit exceeded: " + layers + " > "
          + maxLayers);
    }
    if (logicalBytes > maxLogicalBytes) {
      throw new IOException("path-state layer logical bytes limit exceeded: " + logicalBytes
          + " > " + maxLogicalBytes);
    }
  }

  private static PathStateRootMetadata requireLayer(PathStateStoreManifest manifest,
      Path directory, PathStateRootMetadata metadata) throws IOException {
    if (metadata.getKind() != Kind.LAYER
        || !Arrays.equals(metadata.getFormatDigest(), manifest.getIdentityDigest())
        || !directory.equals(manifest.getLayerDirectory(
        metadata.getBlockNumber(), metadata.getBlockHash()))) {
      throw new IOException("path-state layer limit record identity mismatch");
    }
    return metadata;
  }

  private static void requireSame(PathStateRootMetadata expected,
      PathStateRootMetadata actual, String error) throws IOException {
    if (actual == null || !Arrays.equals(expected.encode(), actual.encode())) {
      throw new IOException(error);
    }
  }

  private static final class Usage {

    private final int layers;
    private final long logicalBytes;

    private Usage(int layers, long logicalBytes) {
      this.layers = layers;
      this.logicalBytes = logicalBytes;
    }
  }
}
