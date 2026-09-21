package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Atomic durable authority for one in-flight target mutation plan. */
final class ArchiveTargetMutationPlanFile {

  private static final String FILE_NAME = "target.mutation-plan";
  private final Path path;
  private final Path temporary;
  private final ArchiveTargetMutationPlanCodec codec = new ArchiveTargetMutationPlanCodec();
  private final FaultHook faultHook;

  ArchiveTargetMutationPlanFile(Path checkpointPath) {
    this(checkpointPath, (stage, path) -> { });
  }

  ArchiveTargetMutationPlanFile(Path checkpointPath, FaultHook faultHook) {
    Path directory = Objects.requireNonNull(checkpointPath, "checkpointPath").getParent();
    if (directory == null) {
      throw new IllegalArgumentException("Checkpoint path must have a parent");
    }
    this.path = directory.resolve(FILE_NAME);
    this.temporary = directory.resolve(FILE_NAME + ".tmp");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  void store(ArchiveTargetMutationPlan plan) throws IOException {
    byte[] encoded = codec.encode(plan);
    Files.createDirectories(path.getParent());
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
    faultHook.after(Stage.AFTER_TEMPORARY_FORCE, temporary);
    try {
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Archive filesystem does not support atomic mutation-plan replacement", unsupported);
    }
    HistorySegmentStore.syncDirectory(path.getParent());
    faultHook.after(Stage.AFTER_REPLACE, path);
  }

  ArchiveTargetMutationPlan loadRequired() throws IOException {
    if (!Files.exists(path)) {
      throw new ArchivePersistenceException("Archive target mutation plan is missing");
    }
    long size = Files.size(path);
    if (size <= 0 || size > ArchiveTargetMutationPlanCodec.MAX_ENCODED_LENGTH) {
      throw new ArchivePersistenceException("Archive target mutation-plan length is invalid");
    }
    try {
      return codec.decode(Files.readAllBytes(path));
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive target mutation plan is corrupt", invalid);
    }
  }

  ArchiveTargetMutationPlan loadIfPresent() throws IOException {
    return Files.exists(path) ? loadRequired() : null;
  }

  void retire() throws IOException {
    Files.deleteIfExists(path);
    Files.deleteIfExists(temporary);
    HistorySegmentStore.syncDirectory(path.getParent());
  }

  Path getPath() {
    return path;
  }

  enum Stage {
    AFTER_TEMPORARY_FORCE,
    AFTER_REPLACE
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage, Path path) throws IOException;
  }
}
