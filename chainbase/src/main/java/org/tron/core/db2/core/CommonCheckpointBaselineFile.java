package org.tron.core.db2.core;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
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
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Immutable, forced admission record for the first common-checkpoint parent. */
public final class CommonCheckpointBaselineFile {

  public static final String FILE_NAME = "COMMON_BASELINE";
  public static final String BOOTSTRAP_INTENT_FILE = "COMMON_BOOTSTRAP_INTENT";
  private static final int MAGIC = 0x43424c4e; // CBLN
  private static final short VERSION = 1;
  private static final int DIGEST_LENGTH = 32;
  private static final int LENGTH = Integer.BYTES + 2 * Short.BYTES + DIGEST_LENGTH
      + 3 * Long.BYTES + 3 * DIGEST_LENGTH + DIGEST_LENGTH;

  private final Path directory;
  private final Path path;

  public CommonCheckpointBaselineFile(Path directory) {
    this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    this.path = this.directory.resolve(FILE_NAME);
  }

  /** Publishes the supplied baseline once, or returns the exact previously admitted baseline. */
  public synchronized CommonCheckpointBaseline openOrCreate(CommonCheckpointBaseline supplied)
      throws IOException {
    Objects.requireNonNull(supplied, "supplied");
    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      CommonCheckpointBaseline loaded = load();
      if (!Arrays.equals(encode(loaded), encode(supplied))) {
        throw new IOException("Common checkpoint baseline differs from canonical startup state");
      }
      return loaded;
    }
    Files.createDirectories(directory);
    Path temporary = directory.resolve(FILE_NAME + ".tmp-" + UUID.randomUUID());
    byte[] encoded = encode(supplied);
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(encoded);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      try {
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("Common checkpoint baseline requires atomic rename", unsupported);
      }
      syncDirectory(directory);
      return supplied;
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  /** Durably marks that a missing PathState directory is being built for this format. */
  public synchronized void beginBootstrap(byte[] formatIdentity) throws IOException {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(formatIdentity, "formatIdentity"),
        formatIdentity.length);
    if (admitted.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException("formatIdentity must contain exactly 32 bytes");
    }
    Files.createDirectories(directory);
    Path intent = directory.resolve(BOOTSTRAP_INTENT_FILE);
    if (Files.exists(intent, LinkOption.NOFOLLOW_LINKS)) {
      if (!Arrays.equals(Files.readAllBytes(intent), admitted)) {
        throw new IOException("Common checkpoint bootstrap intent format differs");
      }
      return;
    }
    publishBytes(intent, admitted);
  }

  public synchronized boolean hasBootstrapIntent(byte[] formatIdentity) throws IOException {
    Path intent = directory.resolve(BOOTSTRAP_INTENT_FILE);
    return Files.isRegularFile(intent, LinkOption.NOFOLLOW_LINKS)
        && Arrays.equals(Files.readAllBytes(intent), formatIdentity);
  }

  public synchronized void retireBootstrapIntent() throws IOException {
    if (Files.deleteIfExists(directory.resolve(BOOTSTRAP_INTENT_FILE))) {
      syncDirectory(directory);
    }
  }

  public synchronized CommonCheckpointBaseline load() throws IOException {
    byte[] encoded = Files.readAllBytes(path);
    if (encoded.length != LENGTH) {
      throw new IOException("Common checkpoint baseline length is invalid");
    }
    int bodyLength = encoded.length - DIGEST_LENGTH;
    byte[] body = Arrays.copyOf(encoded, bodyLength);
    if (!Arrays.equals(Arrays.copyOfRange(encoded, bodyLength, encoded.length),
        Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IOException("Common checkpoint baseline checksum differs");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new IOException("Common checkpoint baseline format is unsupported");
      }
      byte[] format = readDigest(input);
      long epoch = input.readLong();
      long number = input.readLong();
      long timestamp = input.readLong();
      byte[] hash = readDigest(input);
      byte[] parentHash = readDigest(input);
      byte[] stateRoot = readDigest(input);
      return new CommonCheckpointBaseline(format,
          new BlockSnapshotMeta(epoch, number, hash, parentHash, timestamp), stateRoot);
    } catch (EOFException truncated) {
      throw new IOException("Common checkpoint baseline is truncated", truncated);
    }
  }

  private static byte[] encode(CommonCheckpointBaseline baseline) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      BlockSnapshotMeta head = baseline.getHead();
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.write(baseline.getFormatIdentity());
      output.writeLong(head.getEpoch());
      output.writeLong(head.getBlockNumber());
      output.writeLong(head.getTimestamp());
      output.write(head.getBlockHash());
      output.write(head.getParentHash());
      output.write(baseline.getStateRoot());
      output.flush();
      byte[] body = bytes.toByteArray();
      output.write(Hashing.sha256().hashBytes(body).asBytes());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("In-memory common baseline encoding failed", impossible);
    }
  }

  private void publishBytes(Path target, byte[] encoded) throws IOException {
    Path temporary = directory.resolve(target.getFileName() + ".tmp-" + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(encoded);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      try {
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("Common checkpoint bootstrap requires atomic rename", unsupported);
      }
      syncDirectory(directory);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static byte[] readDigest(DataInputStream input) throws IOException {
    byte[] value = new byte[DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }
}
