package org.tron.core.db2.archive.checkpoint.storage;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
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
import java.util.Optional;
import java.util.UUID;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.history.io.ArchiveFileDurability;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;

/** Shared checkpoint target codec and forced atomic file-publication protocol. */
public final class ArchiveCheckpointFiles {
  public static final String READABLE_FILE = "READABLE";
  private static final int TARGET_MAGIC = 0x53414354; // SACT
  private static final short TARGET_VERSION = 2;
  private static final int DIGEST_LENGTH = 32;
  private static final int META_LENGTH = 3 * Long.BYTES + 2 * DIGEST_LENGTH;
  private static final int TARGET_LENGTH = Integer.BYTES + 2 * Short.BYTES
      + 4 * DIGEST_LENGTH + 2 * META_LENGTH + DIGEST_LENGTH;

  private ArchiveCheckpointFiles() {
  }

  public static byte[] encodeTarget(CommonCheckpointTarget target) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(TARGET_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(TARGET_MAGIC);
      output.writeShort(TARGET_VERSION);
      output.writeShort(0);
      output.write(target.getFormatIdentity());
      output.write(target.getPayloadDigest());
      writeMeta(output, target.getFirstBlock());
      writeMeta(output, target.getLastBlock());
      output.write(target.getParentStateRoot());
      output.write(target.getStateRoot());
      output.flush();
      byte[] body = bytes.toByteArray();
      output.write(Hashing.sha256().hashBytes(body).asBytes());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory State Archive target encoding failed", impossible);
    }
  }

  public static TargetMarker loadTarget(Path path) throws IOException {
    byte[] encoded = readBounded(path, TARGET_LENGTH);
    if (encoded.length != TARGET_LENGTH) {
      throw new IOException("State Archive checkpoint target length is invalid");
    }
    int bodyLength = encoded.length - DIGEST_LENGTH;
    byte[] body = Arrays.copyOf(encoded, bodyLength);
    byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
    if (!Arrays.equals(checksum, Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IOException("State Archive checkpoint target checksum differs");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      if (input.readInt() != TARGET_MAGIC || input.readShort() != TARGET_VERSION
          || input.readShort() != 0) {
        throw new IOException("State Archive checkpoint target format is unsupported");
      }
      byte[] formatIdentity = readDigest(input);
      byte[] payloadDigest = readDigest(input);
      BlockSnapshotMeta first = readMeta(input);
      BlockSnapshotMeta last = readMeta(input);
      byte[] parentStateRoot = readDigest(input);
      byte[] stateRoot = readDigest(input);
      CommonCheckpointTarget target;
      try {
        target = CommonCheckpointTarget.restore(formatIdentity, payloadDigest, first, last,
            parentStateRoot, stateRoot);
      } catch (IllegalArgumentException invalid) {
        throw new IOException("State Archive checkpoint target identity is invalid", invalid);
      }
      return new TargetMarker(encoded, target);
    } catch (EOFException truncated) {
      throw new IOException("State Archive checkpoint target is truncated", truncated);
    }
  }

  public static Optional<CommonCheckpointTarget> loadReadableTargetIfPresent(Path directory)
      throws IOException {
    Path readable = Objects.requireNonNull(directory, "directory").resolve(READABLE_FILE);
    return Files.exists(readable, LinkOption.NOFOLLOW_LINKS)
        ? Optional.of(loadTarget(readable).target) : Optional.empty();
  }

  public static void publishReadableTarget(Path directory, CommonCheckpointTarget target)
      throws IOException {
    replace(Objects.requireNonNull(directory, "directory").resolve(READABLE_FILE),
        encodeTarget(Objects.requireNonNull(target, "target")));
  }

  public static void publishImmutable(Path path, byte[] bytes) throws IOException {
    createDirectory(path.getParent());
    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      if (!Arrays.equals(readBounded(path, bytes.length), bytes)) {
        throw new IOException("State Archive immutable checkpoint file differs");
      }
      return;
    }
    Path temporary = path.resolveSibling(path.getFileName() + ".tmp-" + UUID.randomUUID());
    try {
      writeForced(temporary, bytes);
      try {
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("State Archive filesystem lacks atomic immutable publication",
            unsupported);
      } catch (java.nio.file.FileAlreadyExistsException raced) {
        if (!Arrays.equals(readBounded(path, bytes.length), bytes)) {
          throw new IOException("State Archive immutable checkpoint publication raced", raced);
        }
      }
      ArchiveFileDurability.syncDirectory(path.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  public static void replace(Path path, byte[] bytes) throws IOException {
    createDirectory(path.getParent());
    Path temporary = path.resolveSibling(path.getFileName() + ".tmp-" + UUID.randomUUID());
    try {
      writeForced(temporary, bytes);
      try {
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("State Archive filesystem lacks atomic READABLE replacement",
            unsupported);
      }
      ArchiveFileDurability.syncDirectory(path.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void writeForced(Path path, byte[] bytes) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
  }

  public static byte[] readBounded(Path path, long maximum) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State Archive checkpoint path is not a regular file");
    }
    long size = Files.size(path);
    if (size <= 0 || size > maximum) {
      throw new IOException("State Archive checkpoint file length is invalid");
    }
    return Files.readAllBytes(path);
  }

  private static byte[] readDigest(DataInputStream input) throws IOException {
    byte[] value = new byte[DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static void writeMeta(DataOutputStream output, BlockSnapshotMeta meta)
      throws IOException {
    output.writeLong(meta.getEpoch());
    output.writeLong(meta.getBlockNumber());
    output.write(meta.getBlockHash());
    output.write(meta.getParentHash());
    output.writeLong(meta.getTimestamp());
  }

  private static BlockSnapshotMeta readMeta(DataInputStream input) throws IOException {
    return new BlockSnapshotMeta(input.readLong(), input.readLong(), readDigest(input),
        readDigest(input), input.readLong());
  }

  private static void createDirectory(Path path) throws IOException {
    Files.createDirectories(path);
    if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State Archive checkpoint path is not a directory");
    }
  }

  public static final class TargetMarker {

    private final byte[] encoded;
    private final CommonCheckpointTarget target;

    public byte[] getEncoded() {
      return Arrays.copyOf(encoded, encoded.length);
    }

    public CommonCheckpointTarget getTarget() {
      return target;
    }

    private TargetMarker(byte[] encoded, CommonCheckpointTarget target) {
      this.encoded = encoded;
      this.target = target;
    }
  }
}
