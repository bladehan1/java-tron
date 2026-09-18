package org.tron.core.db2.archive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Durable engine identity that prevents a checkpoint serving index from changing engine. */
final class StateArchiveIndexEngineManifest {

  static final String FILE = "ENGINE";
  private static final String TEMP = "ENGINE.tmp";
  private static final int MAGIC = 0x53414945; // SAIE
  private static final short LEGACY_SHA_VERSION = 1;
  private static final short VERSION = 2;
  private static final int BODY_LENGTH = Integer.BYTES + 2 * Short.BYTES;
  private static final int CHECKSUM_LENGTH = Integer.BYTES;
  private static final int LEGACY_DIGEST_LENGTH = 32;
  private static final int ENCODED_LENGTH = BODY_LENGTH + CHECKSUM_LENGTH;

  private StateArchiveIndexEngineManifest() {
  }

  static void openOrCreate(Path directory, Engine engine) throws IOException {
    openOrCreate(directory, engine, false);
  }

  static void openOrCreateLegacy(Path directory, Engine engine) throws IOException {
    openOrCreate(directory, engine, true);
  }

  private static void openOrCreate(Path directory, Engine engine, boolean admitLegacyRocks)
      throws IOException {
    Path root = Objects.requireNonNull(directory, "directory");
    Engine selected = Objects.requireNonNull(engine, "engine");
    Files.createDirectories(root);
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State Archive serving-index path is not a directory");
    }
    Path manifest = root.resolve(FILE);
    if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS)) {
      require(root, selected);
      return;
    }
    Path database = root.resolve("keys");
    if (Files.exists(database, LinkOption.NOFOLLOW_LINKS)) {
      if (!admitLegacyRocks || selected != Engine.ROCKSDB) {
        throw new IOException("State Archive serving index has no engine identity");
      }
    }
    byte[] encoded = encode(selected);
    Path temporary = root.resolve(TEMP);
    try {
      Files.write(temporary, encoded, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
          StandardOpenOption.SYNC);
      try {
        Files.move(temporary, manifest, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException failure) {
        throw new IOException("State Archive serving engine publication must be atomic", failure);
      }
      HistorySegmentStore.syncDirectory(root);
    } catch (IOException | RuntimeException failure) {
      Files.deleteIfExists(temporary);
      throw failure;
    }
  }

  static void require(Path directory, Engine engine) throws IOException {
    Engine expected = Objects.requireNonNull(engine, "engine");
    if (load(directory) != expected) {
      throw new IOException("State Archive serving index engine differs: expected " + expected);
    }
  }

  static Engine load(Path directory) throws IOException {
    Path manifest = Objects.requireNonNull(directory, "directory").resolve(FILE);
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State Archive serving engine identity is missing");
    }
    byte[] encoded = Files.readAllBytes(manifest);
    if (encoded.length != ENCODED_LENGTH
        && encoded.length != BODY_LENGTH + LEGACY_DIGEST_LENGTH) {
      throw new IOException("State Archive serving engine identity length is invalid");
    }
    byte[] body = Arrays.copyOf(encoded, BODY_LENGTH);
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      int magic = input.readInt();
      short version = input.readShort();
      if (magic != MAGIC || version != VERSION && version != LEGACY_SHA_VERSION) {
        throw new IOException("State Archive serving engine identity is unsupported");
      }
      if (version == VERSION) {
        if (encoded.length != ENCODED_LENGTH
            || java.nio.ByteBuffer.wrap(encoded, BODY_LENGTH, CHECKSUM_LENGTH).getInt()
            != com.google.common.hash.Hashing.crc32c().hashBytes(body).asInt()) {
          throw new IOException("State Archive serving engine identity checksum differs");
        }
      } else if (encoded.length != BODY_LENGTH + LEGACY_DIGEST_LENGTH
          || !Arrays.equals(Arrays.copyOfRange(encoded, BODY_LENGTH, encoded.length),
          com.google.common.hash.Hashing.sha256().hashBytes(body).asBytes())) {
        throw new IOException("State Archive serving engine identity checksum differs");
      }
      int tag = input.readUnsignedShort();
      if (tag == tag(Engine.LEVELDB)) {
        return Engine.LEVELDB;
      }
      if (tag == tag(Engine.ROCKSDB)) {
        return Engine.ROCKSDB;
      }
      throw new IOException("State Archive serving engine identity tag is unsupported");
    }
  }

  private static byte[] encode(Engine engine) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(ENCODED_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(tag(engine));
      output.flush();
      byte[] body = bytes.toByteArray();
      output.writeInt(com.google.common.hash.Hashing.crc32c().hashBytes(body).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory serving engine encoding failed", impossible);
    }
  }

  private static int tag(Engine engine) {
    switch (engine) {
      case LEVELDB:
        return 1;
      case ROCKSDB:
        return 2;
      default:
        throw new IllegalArgumentException("Unsupported State Archive serving engine: " + engine);
    }
  }
}
