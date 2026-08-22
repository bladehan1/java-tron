package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

/** Crash-recoverable removal of the old canonical suffix after an ancestor switch. */
public final class PathStateLayerRetirement {

  public static final String INTENT_FILE = "RETIRE_INTENT";

  private static final int MAGIC = 0x50535254; // PSRT
  private static final short VERSION = 1;
  private static final int MAX_PLAN_LENGTH = 32 * 1024 * 1024;

  private final PathStateStoreManifest manifest;
  private final PathStateCurrentStore currentStore;
  private final PathStateLayerLimits limits;
  private final FaultHook faultHook;
  private final Path intentPath;

  public PathStateLayerRetirement(PathStateStoreManifest manifest,
      PathStateLayerLimits limits) {
    this(manifest, limits, stage -> { });
  }

  PathStateLayerRetirement(PathStateStoreManifest manifest, PathStateLayerLimits limits,
      FaultHook faultHook) {
    this.manifest = Objects.requireNonNull(manifest, "manifest");
    this.currentStore = new PathStateCurrentStore(manifest);
    this.limits = Objects.requireNonNull(limits, "limits");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    this.intentPath = manifest.getDirectory().resolve(INTENT_FILE);
  }

  /** Switches to an exact ancestor and durably removes only the old canonical suffix. */
  public synchronized PathStateRootMetadata switchToAncestor(PathStateRootMetadata target)
      throws IOException {
    if (Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("unfinished path-state layer retirement requires recovery");
    }
    List<PathStateRootMetadata> victims = currentStore.layersAboveAncestor(target, limits);
    if (victims.isEmpty()) {
      return currentStore.current();
    }
    RetirementPlan plan = new RetirementPlan(target, victims);
    plan.verify(manifest);
    PathStateMetadataFile.publishImmutableBytes(intentPath, plan.encode());
    faultHook.after(Stage.AFTER_INTENT);
    PathStateRootMetadata current = currentStore.switchToAncestor(target, limits);
    faultHook.after(Stage.AFTER_CURRENT);
    retire(plan);
    PathStateMetadataFile.deleteDurable(intentPath);
    faultHook.after(Stage.AFTER_RETIRE);
    limits.verifyExisting(manifest);
    return current;
  }

  /** Completes one durable switch/retire plan and becomes a zero-action retry. */
  public synchronized RecoveryAction recover() throws IOException {
    if (!Files.exists(intentPath, LinkOption.NOFOLLOW_LINKS)) {
      limits.verifyExisting(manifest);
      return RecoveryAction.NONE;
    }
    RetirementPlan plan = RetirementPlan.decode(
        PathStateMetadataFile.loadImmutableBytes(intentPath, MAX_PLAN_LENGTH));
    plan.verify(manifest);
    PathStateRootMetadata current = currentStore.current();
    if (same(current, plan.victims.get(0))) {
      currentStore.switchToAncestor(plan.target, limits);
    } else if (!same(current, plan.target)) {
      throw new IOException("path-state retirement plan does not own CURRENT");
    }
    retire(plan);
    PathStateMetadataFile.deleteDurable(intentPath);
    limits.verifyExisting(manifest);
    return RecoveryAction.COMPLETED_RETIREMENT;
  }

  private void retire(RetirementPlan plan) throws IOException {
    for (PathStateRootMetadata victim : plan.victims) {
      deleteLayer(victim);
      faultHook.after(Stage.AFTER_LAYER_RETIRE);
    }
  }

  private void deleteLayer(PathStateRootMetadata victim) throws IOException {
    Path directory = manifest.getLayerDirectory(victim.getBlockNumber(), victim.getBlockHash());
    if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
        || Files.isSymbolicLink(directory)) {
      throw new IOException("path-state retirement target is not a direct directory");
    }
    Path metadata = directory.resolve(PathStateCurrentStore.METADATA_FILE);
    if (Files.exists(metadata, LinkOption.NOFOLLOW_LINKS)) {
      PathStateMetadataFile.requireExact(metadata, victim);
    }
    List<Path> entries = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.forEach(entries::add);
    }
    for (Path entry : entries) {
      if (Files.isSymbolicLink(entry)) {
        throw new IOException("path-state retirement refuses symbolic links: " + entry);
      }
    }
    entries.sort(Comparator.reverseOrder());
    for (Path entry : entries) {
      Files.deleteIfExists(entry);
      faultHook.after(Stage.AFTER_LAYER_ENTRY_DELETE);
    }
    PathStateMetadataFile.syncDirectory(manifest.getLayersDirectory());
  }

  public enum RecoveryAction {
    NONE,
    COMPLETED_RETIREMENT
  }

  enum Stage {
    AFTER_INTENT,
    AFTER_CURRENT,
    AFTER_LAYER_ENTRY_DELETE,
    AFTER_LAYER_RETIRE,
    AFTER_RETIRE
  }

  @FunctionalInterface
  interface FaultHook {

    void after(Stage stage) throws IOException;
  }

  private static final class RetirementPlan {

    private final PathStateRootMetadata target;
    private final List<PathStateRootMetadata> victims;

    private RetirementPlan(PathStateRootMetadata target,
        List<PathStateRootMetadata> victims) {
      this.target = Objects.requireNonNull(target, "target");
      this.victims = new ArrayList<>(Objects.requireNonNull(victims, "victims"));
      if (this.victims.isEmpty()) {
        throw new IllegalArgumentException("path-state retirement plan requires victims");
      }
    }

    private void verify(PathStateStoreManifest manifest) throws IOException {
      requireFormat(target, manifest);
      PathStateRootMetadata child = null;
      for (PathStateRootMetadata victim : victims) {
        requireFormat(victim, manifest);
        if (victim.getKind() != Kind.LAYER) {
          throw new IOException("path-state retirement victim is not a layer");
        }
        if (child != null && !isParent(victim, child)) {
          throw new IOException("path-state retirement suffix is not contiguous");
        }
        child = victim;
      }
      if (!isParent(target, victims.get(victims.size() - 1))) {
        throw new IOException("path-state retirement target does not precede its suffix");
      }
    }

    private byte[] encode() {
      try {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(MAGIC);
        output.writeShort(VERSION);
        output.writeShort(0);
        output.writeInt(0);
        output.writeInt(victims.size());
        writeMetadata(output, target);
        for (PathStateRootMetadata victim : victims) {
          writeMetadata(output, victim);
        }
        output.flush();
        byte[] payload = bytes.toByteArray();
        ByteBuffer.wrap(payload).putInt(8, payload.length + Integer.BYTES);
        bytes.reset();
        output = new DataOutputStream(bytes);
        output.write(payload);
        output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
        output.flush();
        byte[] encoded = bytes.toByteArray();
        if (encoded.length > MAX_PLAN_LENGTH) {
          throw new IllegalArgumentException("path-state retirement plan is too large");
        }
        return encoded;
      } catch (IOException impossible) {
        throw new IllegalStateException("in-memory retirement plan encoding failed", impossible);
      }
    }

    private static RetirementPlan decode(byte[] encoded) throws IOException {
      byte[] value = Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length);
      if (value.length <= Integer.BYTES || value.length > MAX_PLAN_LENGTH) {
        throw new IOException("path-state retirement plan length is invalid");
      }
      byte[] payload = Arrays.copyOf(value, value.length - Integer.BYTES);
      int checksum = ByteBuffer.wrap(value, payload.length, Integer.BYTES).getInt();
      if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
        throw new IOException("path-state retirement plan checksum mismatch");
      }
      try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(value))) {
        if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
            || input.readInt() != value.length) {
          throw new IOException("unsupported path-state retirement plan header");
        }
        int count = input.readInt();
        if (count <= 0 || count > PathStateCurrentStore.MAX_VALIDATION_LAYERS) {
          throw new IOException("path-state retirement plan count is invalid");
        }
        PathStateRootMetadata target = readMetadata(input);
        List<PathStateRootMetadata> victims = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
          victims.add(readMetadata(input));
        }
        if (input.available() != Integer.BYTES) {
          throw new IOException("path-state retirement plan payload mismatch");
        }
        return new RetirementPlan(target, victims);
      } catch (IllegalArgumentException invalid) {
        throw new IOException("path-state retirement plan metadata is corrupt", invalid);
      }
    }

    private static void writeMetadata(DataOutputStream output, PathStateRootMetadata metadata)
        throws IOException {
      byte[] encoded = metadata.encode();
      output.writeInt(encoded.length);
      output.write(encoded);
    }

    private static PathStateRootMetadata readMetadata(DataInputStream input) throws IOException {
      int length = input.readInt();
      if (length <= 0 || length > input.available() - Integer.BYTES) {
        throw new IOException("path-state retirement metadata length is invalid");
      }
      byte[] encoded = new byte[length];
      input.readFully(encoded);
      return PathStateRootMetadata.decode(encoded);
    }

    private static void requireFormat(PathStateRootMetadata metadata,
        PathStateStoreManifest manifest) throws IOException {
      if (!Arrays.equals(metadata.getFormatDigest(), manifest.getIdentityDigest())) {
        throw new IOException("path-state retirement metadata identity mismatch");
      }
    }

    private static boolean isParent(PathStateRootMetadata parent,
        PathStateRootMetadata child) {
      return child.getBlockNumber() == parent.getBlockNumber() + 1
          && Arrays.equals(child.getParentHash(), parent.getBlockHash())
          && Arrays.equals(child.getParentStateRoot(), parent.getStateRoot());
    }
  }

  private static boolean same(PathStateRootMetadata left, PathStateRootMetadata right) {
    return Arrays.equals(left.encode(), right.encode());
  }
}
