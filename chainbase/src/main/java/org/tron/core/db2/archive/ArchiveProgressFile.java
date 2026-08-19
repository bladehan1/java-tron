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

/** Atomic-file prototype for one C or D[i] envelope. */
final class ArchiveProgressFile {

  private final Path path;
  private final Path temporary;
  private final ArchiveProgressEnvelopeCodec codec;
  private final FaultHook faultHook;

  ArchiveProgressFile(Path path, ArchiveProgressEnvelopeCodec codec) {
    this(path, codec, temporary -> { });
  }

  ArchiveProgressFile(Path path, ArchiveProgressEnvelopeCodec codec, FaultHook faultHook) {
    this.path = Objects.requireNonNull(path, "path");
    this.temporary = path.resolveSibling(path.getFileName() + ".tmp");
    this.codec = Objects.requireNonNull(codec, "codec");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  ArchiveProgressEnvelope load() throws IOException {
    if (!Files.exists(path)) {
      throw new ArchivePersistenceException("Archive progress envelope is missing: " + path);
    }
    long size = Files.size(path);
    if (size <= 0 || size > ArchiveProgressEnvelopeCodec.MAX_ENCODED_LENGTH) {
      throw new ArchivePersistenceException("Archive progress envelope file length is invalid");
    }
    try {
      return codec.decode(Files.readAllBytes(path));
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive progress envelope is corrupt", invalid);
    }
  }

  void store(ArchiveProgressEnvelope envelope) throws IOException {
    byte[] encoded = codec.encode(envelope);
    Path directory = Objects.requireNonNull(path.getParent(), "progress directory");
    Files.createDirectories(directory);
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
    faultHook.afterTemporaryForce(temporary);
    try {
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Archive progress filesystem does not support atomic replacement", unsupported);
    }
    HistorySegmentStore.syncDirectory(directory);
  }

  Path getTemporaryPath() {
    return temporary;
  }

  @FunctionalInterface
  interface FaultHook {
    void afterTemporaryForce(Path temporary) throws IOException;
  }
}
