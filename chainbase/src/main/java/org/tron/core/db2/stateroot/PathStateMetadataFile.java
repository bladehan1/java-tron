package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Atomic-file boundary for immutable BASE/LAYER metadata and the replaceable CURRENT authority. */
final class PathStateMetadataFile {

  private PathStateMetadataFile() {
  }

  static PathStateRootMetadata load(Path path) throws IOException {
    Path target = Objects.requireNonNull(path, "path");
    if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state metadata is missing or not a regular file: " + target);
    }
    try {
      return PathStateRootMetadata.decode(Files.readAllBytes(target));
    } catch (IllegalArgumentException invalid) {
      throw new IOException("path-state metadata is corrupt: " + target, invalid);
    }
  }

  /** Publishes once; an exact existing record is an idempotent retry, not a rewrite. */
  static void publishImmutable(Path path, PathStateRootMetadata metadata) throws IOException {
    Path target = Objects.requireNonNull(path, "path");
    byte[] encoded = Objects.requireNonNull(metadata, "metadata").encode();
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      requireExact(target, encoded);
      return;
    }
    publish(target, encoded, false, temporary -> { });
  }

  static void replaceCurrent(Path path, PathStateRootMetadata metadata) throws IOException {
    replaceCurrent(path, metadata, temporary -> { });
  }

  static void replaceCurrent(Path path, PathStateRootMetadata metadata, FaultHook faultHook)
      throws IOException {
    publish(Objects.requireNonNull(path, "path"),
        Objects.requireNonNull(metadata, "metadata").encode(), true,
        Objects.requireNonNull(faultHook, "faultHook"));
  }

  static void requireExact(Path path, PathStateRootMetadata metadata) throws IOException {
    requireExact(path, Objects.requireNonNull(metadata, "metadata").encode());
  }

  static void deleteDurable(Path path) throws IOException {
    Path target = Objects.requireNonNull(path, "path");
    Path directory = Objects.requireNonNull(target.getParent(), "metadata directory");
    if (Files.deleteIfExists(target)) {
      syncDirectory(directory);
    }
  }

  private static void requireExact(Path path, byte[] expected) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state metadata is not a regular file: " + path);
    }
    byte[] actual = Files.readAllBytes(path);
    try {
      PathStateRootMetadata.decode(actual);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("path-state metadata is corrupt: " + path, invalid);
    }
    if (!Arrays.equals(expected, actual)) {
      throw new IOException("immutable path-state metadata identity mismatch: " + path);
    }
  }

  private static void publish(Path target, byte[] encoded, boolean replace, FaultHook faultHook)
      throws IOException {
    Path directory = Objects.requireNonNull(target.getParent(), "metadata directory");
    Files.createDirectories(directory);
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
        || Files.isSymbolicLink(directory)) {
      throw new IOException("path-state metadata parent is not a direct directory: " + directory);
    }
    Path temporary = directory.resolve("." + target.getFileName() + "-" + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        writeFully(channel, ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      faultHook.afterTemporaryForce(temporary);
      try {
        if (replace) {
          Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } else {
          Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        }
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("path-state metadata requires atomic publication", unsupported);
      }
      syncDirectory(directory);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  @FunctionalInterface
  interface FaultHook {

    void afterTemporaryForce(Path temporary) throws IOException;
  }
}
