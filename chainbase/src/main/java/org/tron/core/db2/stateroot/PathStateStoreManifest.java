package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import org.tron.core.db2.stateroot.PathStateParticipantDescriptor.StoreIdentity;

/** Durable format admission anchor for an enabled current path-state backend. */
public final class PathStateStoreManifest {

  public static final String MANIFEST_FILE = "MANIFEST";
  public static final String BASE_DIRECTORY = "base";
  public static final String LAYERS_DIRECTORY = "layers";

  private static final int MAGIC = 0x50534d46; // PSMF
  private static final short VERSION = 1;
  private static final int MAX_LENGTH = 1024 * 1024;

  private final Path directory;
  private final Engine engine;
  private final byte[] identityDigest;

  private PathStateStoreManifest(Path directory, Engine engine) {
    this.directory = directory;
    this.engine = engine;
    this.identityDigest = Hashing.sha256().hashBytes(encode(engine)).asBytes();
  }

  /** Creates a new exact-format manifest or validates an existing one without rewriting it. */
  public static PathStateStoreManifest createOrOpen(Path directory, Engine engine)
      throws IOException {
    Path root = directory.toAbsolutePath().normalize();
    Engine selected = requireEngine(engine);
    rejectSymbolicLink(root);
    Files.createDirectories(root);
    requireDirectory(root, "path-state root");

    byte[] expected = encode(selected);
    Path manifest = root.resolve(MANIFEST_FILE);
    if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS)) {
      validateExisting(manifest, expected);
    } else {
      publish(manifest, expected);
    }
    ensureChildDirectory(root.resolve(BASE_DIRECTORY));
    ensureChildDirectory(root.resolve(LAYERS_DIRECTORY));
    return new PathStateStoreManifest(root, selected);
  }

  /** Validates an existing manifest without creating or modifying filesystem entries. */
  public static PathStateStoreManifest validateExisting(Path directory, Engine engine)
      throws IOException {
    Path root = directory.toAbsolutePath().normalize();
    Engine selected = requireEngine(engine);
    rejectSymbolicLink(root);
    requireDirectory(root, "path-state root");
    Path manifest = root.resolve(MANIFEST_FILE);
    validateExisting(manifest, encode(selected));
    Path base = root.resolve(BASE_DIRECTORY);
    if (!Files.isDirectory(base, LinkOption.NOFOLLOW_LINKS)) {
      requireBaseReplacementRecoveryLayout(root);
    }
    requireDirectory(root.resolve(LAYERS_DIRECTORY), "path-state layers");
    return new PathStateStoreManifest(root, selected);
  }

  private static void requireBaseReplacementRecoveryLayout(Path root) throws IOException {
    Path intent = root.resolve(PathStateBaseCompaction.INTENT_FILE);
    if (!Files.isRegularFile(intent, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state base is missing outside a durable replacement");
    }
    requireDirectory(root.resolve(PathStateBaseCompaction.NEXT_DIRECTORY),
        "path-state next base");
    requireDirectory(root.resolve(PathStateBaseCompaction.PREVIOUS_DIRECTORY),
        "path-state previous base");
  }

  public Path getDirectory() {
    return directory;
  }

  public Path getBaseDirectory() {
    return directory.resolve(BASE_DIRECTORY);
  }

  public Path getLayersDirectory() {
    return directory.resolve(LAYERS_DIRECTORY);
  }

  public Path getLayerDirectory(long blockNumber, byte[] blockHash) {
    if (blockNumber < 0) {
      throw new IllegalArgumentException("blockNumber must not be negative");
    }
    byte[] hash = Arrays.copyOf(blockHash, blockHash.length);
    if (hash.length != PathStateRootMetadata.DIGEST_LENGTH) {
      throw new IllegalArgumentException("blockHash must be exactly 32 bytes");
    }
    return getLayersDirectory().resolve(String.format(Locale.ROOT, "%020d-%s",
        blockNumber, hex(hash)));
  }

  public Engine getEngine() {
    return engine;
  }

  public byte[] getIdentityDigest() {
    return Arrays.copyOf(identityDigest, identityDigest.length);
  }

  private static byte[] encode(Engine engine) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      writeString(output, PathStateParticipantDescriptor.SCOPE_ID);
      output.writeByte(engine.tag);
      output.writeShort(PathStateCommitmentCodec.FORMAT_VERSION);
      PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
      PathStateCanonicalizer canonicalizer = new PathStateCanonicalizer();
      output.writeInt(descriptor.getStores().size());
      for (StoreIdentity store : descriptor.getStores()) {
        PathStateCanonicalizer.StoreFormat format =
            canonicalizer.requireFormat(store.getDbName());
        output.writeInt(store.getStoreId());
        writeString(output, store.getDbName());
        writeString(output, store.getComparatorId());
        output.writeInt(format.getStoreFormatVersion());
        writeString(output, format.getCodecId());
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = payload.length + Integer.BYTES;
      if (length > MAX_LENGTH) {
        throw new IllegalArgumentException("path-state manifest is too large");
      }
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory path-state manifest encoding failed", impossible);
    }
  }

  private static void validateExisting(Path manifest, byte[] expected) throws IOException {
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state manifest is missing or not a regular file");
    }
    long size = Files.size(manifest);
    if (size <= Integer.BYTES || size > MAX_LENGTH) {
      throw new IOException("path-state manifest length is invalid");
    }
    byte[] actual = Files.readAllBytes(manifest);
    decodeHeader(actual);
    if (!Arrays.equals(expected, actual)) {
      throw new IOException("path-state manifest identity mismatch");
    }
  }

  private static void decodeHeader(byte[] encoded) throws IOException {
    if (encoded.length <= Integer.BYTES || encoded.length > MAX_LENGTH) {
      throw new IOException("path-state manifest length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IOException("path-state manifest checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IOException("unsupported path-state manifest header");
      }
    }
  }

  private static void publish(Path manifest, byte[] encoded) throws IOException {
    Path directory = manifest.getParent();
    Path temporary = directory.resolve(".MANIFEST-" + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        writeFully(channel, ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      try {
        Files.move(temporary, manifest, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("path-state manifest requires atomic publication", unsupported);
      }
      syncDirectory(directory);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void ensureChildDirectory(Path path) throws IOException {
    rejectSymbolicLink(path);
    Files.createDirectories(path);
    requireDirectory(path, "path-state child");
  }

  private static void rejectSymbolicLink(Path path) throws IOException {
    if (Files.isSymbolicLink(path)) {
      throw new IOException("path-state path must not be a symbolic link: " + path);
    }
  }

  private static void requireDirectory(Path path, String name) throws IOException {
    if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException(name + " is missing or not a directory: " + path);
    }
  }

  private static Engine requireEngine(Engine engine) {
    if (engine == null) {
      throw new IllegalArgumentException("path-state engine must not be null");
    }
    return engine;
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    if (encoded.length == 0 || encoded.length > 1024) {
      throw new IllegalArgumentException("path-state manifest string is invalid");
    }
    output.writeShort(encoded.length);
    output.write(encoded);
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

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte current : value) {
      encoded.append(Character.forDigit(current >>> 4 & 0xf, 16));
      encoded.append(Character.forDigit(current & 0xf, 16));
    }
    return encoded.toString();
  }

  public enum Engine {
    LEVELDB(1),
    ROCKSDB(2);

    private final int tag;

    Engine(int tag) {
      this.tag = tag;
    }
  }
}
