package org.tron.common.storage;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

/** Durable engine identity used to bind archive generations across process restarts. */
public final class EngineSourceIdentityFile {

  private static final int MAGIC = 0x454e4749; // ENGI
  private static final short VERSION = 1;
  private static final int MAX_SIZE = 4096;
  private static final String FILE_NAME = ".archive-engine.identity";
  private static final String LOCK_NAME = ".archive-engine.identity.lock";

  private EngineSourceIdentityFile() {
  }

  public static synchronized String loadOrCreate(Path databaseDirectory, String engine,
      String dbName)
      throws IOException {
    if (databaseDirectory == null) {
      throw new IllegalArgumentException("databaseDirectory must not be null");
    }
    String normalizedEngine = normalizeEngine(engine);
    String normalizedDbName = normalizeDbName(dbName);
    Files.createDirectories(databaseDirectory);
    Path identityFile = databaseDirectory.resolve(FILE_NAME);
    try (FileChannel lockChannel = FileChannel.open(databaseDirectory.resolve(LOCK_NAME),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock ignored = lockChannel.lock()) {
      if (!Files.exists(identityFile)) {
        Identity created = new Identity(normalizedEngine, normalizedDbName, UUID.randomUUID());
        persistNew(databaseDirectory, identityFile, created);
      }
    }
    Identity identity = load(identityFile);
    if (!normalizedEngine.equals(identity.engine) || !normalizedDbName.equals(identity.dbName)) {
      throw new IOException("Archive engine source identity does not match engine/dbName");
    }
    return identity.engine.toLowerCase(Locale.ROOT) + ":" + identity.dbName + ":"
        + identity.uuid;
  }

  static Path identityPath(Path databaseDirectory) {
    return databaseDirectory.resolve(FILE_NAME);
  }

  private static void persistNew(Path directory, Path destination, Identity identity)
      throws IOException {
    byte[] encoded = encode(identity);
    Path temporary = directory.resolve(FILE_NAME + ".tmp." + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      try {
        Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
      } catch (FileAlreadyExistsException raced) {
        // Another opener established the immutable identity first; validate it below.
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("Engine identity filesystem does not support atomic create",
            unsupported);
      }
      syncDirectory(directory);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static Identity load(Path identityFile) throws IOException {
    if (!Files.isRegularFile(identityFile)) {
      throw new IOException("Archive engine source identity is missing or not a regular file");
    }
    byte[] encoded = Files.readAllBytes(identityFile);
    if (encoded.length < 32 || encoded.length > MAX_SIZE) {
      throw new IOException("Archive engine source identity length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IOException("Archive engine source identity checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new IOException("Unsupported archive engine source identity");
      }
      String engine = normalizeEngine(input.readUTF());
      String dbName = normalizeDbName(input.readUTF());
      UUID uuid = new UUID(input.readLong(), input.readLong());
      if (input.available() != Integer.BYTES) {
        throw new IOException("Archive engine source identity payload mismatch");
      }
      return new Identity(engine, dbName, uuid);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("Archive engine source identity fields are invalid", invalid);
    }
  }

  private static byte[] encode(Identity identity) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeUTF(identity.engine);
      output.writeUTF(identity.dbName);
      output.writeLong(identity.uuid.getMostSignificantBits());
      output.writeLong(identity.uuid.getLeastSignificantBits());
      output.flush();
      byte[] payload = bytes.toByteArray();
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected engine identity encoding failure", impossible);
    }
  }

  private static String normalizeEngine(String engine) {
    if (engine == null) {
      throw new IllegalArgumentException("engine must not be null");
    }
    String normalized = engine.trim().toUpperCase(Locale.ROOT);
    if (!"LEVELDB".equals(normalized) && !"ROCKSDB".equals(normalized)) {
      throw new IllegalArgumentException("Unsupported archive engine identity: " + engine);
    }
    return normalized;
  }

  private static String normalizeDbName(String dbName) {
    if (dbName == null) {
      throw new IllegalArgumentException("dbName must not be null");
    }
    String normalized = Normalizer.normalize(dbName, Normalizer.Form.NFC);
    if (normalized.isEmpty() || !normalized.equals(dbName)) {
      throw new IllegalArgumentException("dbName must be non-empty canonical NFC");
    }
    return normalized;
  }

  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static final class Identity {
    private final String engine;
    private final String dbName;
    private final UUID uuid;

    private Identity(String engine, String dbName, UUID uuid) {
      this.engine = engine;
      this.dbName = dbName;
      this.uuid = uuid;
    }
  }
}
