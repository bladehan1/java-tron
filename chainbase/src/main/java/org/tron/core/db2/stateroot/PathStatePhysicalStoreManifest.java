package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
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
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Immutable format identity for the TASK-018 physical 27+1 layout. */
public final class PathStatePhysicalStoreManifest {

  private static final String FILE = "MANIFEST";
  private static final int MAGIC = 0x50535046; // PSPF
  private static final short VERSION = 1;
  private static final int HEADER_LENGTH = Integer.BYTES + Short.BYTES + Short.BYTES
      + Short.BYTES + Integer.BYTES;

  private final Path directory;
  private final Engine engine;
  private final byte[] identityDigest;

  private PathStatePhysicalStoreManifest(Path directory, Engine engine, byte[] encoded) {
    this.directory = directory;
    this.engine = engine;
    identityDigest = Hashing.sha256().hashBytes(encoded).asBytes();
  }

  /** Creates a fresh manifest or requires byte-for-byte identity with the existing manifest. */
  public static PathStatePhysicalStoreManifest createOrOpen(Path directory, Engine engine)
      throws IOException {
    Path root = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    Engine selected = Objects.requireNonNull(engine, "engine");
    if (Files.isSymbolicLink(root)) {
      throw new IOException("path-state physical root must not be a symbolic link: " + root);
    }
    Files.createDirectories(root);
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state physical root is not a directory: " + root);
    }
    byte[] expected = encode(selected);
    Path manifest = root.resolve(FILE);
    if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS)) {
      byte[] actual = Files.readAllBytes(manifest);
      if (!Arrays.equals(expected, actual)) {
        throw new IOException("TASK-018 physical manifest identity mismatch");
      }
    } else {
      publish(manifest, expected);
    }
    return new PathStatePhysicalStoreManifest(root, selected, expected);
  }

  /** Validates a previously created physical manifest without creating any path. */
  public static PathStatePhysicalStoreManifest validateExisting(Path directory, Engine engine)
      throws IOException {
    Path root = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    Engine selected = Objects.requireNonNull(engine, "engine");
    if (Files.isSymbolicLink(root)
        || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state physical root is missing or invalid: " + root);
    }
    Path manifest = root.resolve(FILE);
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state physical manifest is missing");
    }
    byte[] expected = encode(selected);
    byte[] actual = Files.readAllBytes(manifest);
    if (!Arrays.equals(expected, actual)) {
      throw new IOException("TASK-018 physical manifest identity mismatch");
    }
    return new PathStatePhysicalStoreManifest(root, selected, expected);
  }

  public Path getDirectory() {
    return directory;
  }

  public Engine getEngine() {
    return engine;
  }

  public byte[] getIdentityDigest() {
    return Arrays.copyOf(identityDigest, identityDigest.length);
  }

  private static byte[] encode(Engine engine) {
    PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
    ByteBuffer encoded = ByteBuffer.allocate(HEADER_LENGTH + descriptor.getStores().size()
        * Integer.BYTES);
    encoded.putInt(MAGIC).putShort(VERSION).putShort((short) PathStateCommitmentCodec.FORMAT_VERSION)
        .putShort((short) engine.ordinal()).putInt(descriptor.getStores().size());
    for (PathStateParticipantDescriptor.StoreIdentity store : descriptor.getStores()) {
      encoded.putInt(store.getStoreId());
    }
    return encoded.array();
  }

  private static void publish(Path manifest, byte[] encoded) throws IOException {
    Path temporary = manifest.getParent().resolve(".MANIFEST-" + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      try {
        Files.move(temporary, manifest, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException failure) {
        throw new IOException("TASK-018 manifest requires atomic publication", failure);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }
}
