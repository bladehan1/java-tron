package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Durable generation base identity for the experimental state archive. */
final class ArchiveBaseManifest {

  private static final int MAGIC = 0x54414d46; // TAMF
  private static final short VERSION = 3;
  private static final int MAX_LENGTH = 1024 * 1024;

  private final Path directory;
  private final Path path;
  private final List<String> participants;
  private final String scopeIdentity;
  private BaseIdentity base;

  ArchiveBaseManifest(Path directory, List<String> participants) throws IOException {
    this.directory = directory;
    this.path = directory.resolve("MANIFEST");
    this.participants = new ArrayList<>(participants);
    this.scopeIdentity = scopeIdentity(this.participants);
    Files.createDirectories(directory);
    if (Files.exists(path)) {
      base = loadExisting(path, scopeIdentity, this.participants);
    }
  }

  /** Validates an existing manifest without creating or modifying any filesystem entry. */
  static ExistingBase validateExisting(Path directory, List<String> participants)
      throws IOException {
    List<String> expectedParticipants = new ArrayList<>(participants);
    Path manifest = directory.resolve("MANIFEST");
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      throw new ArchivePersistenceException("Archive manifest is missing or not a regular file");
    }
    BaseIdentity existing = loadExisting(manifest, scopeIdentity(expectedParticipants),
        expectedParticipants);
    return new ExistingBase(existing.epoch, existing.hash);
  }

  synchronized void ensureBase(BlockSnapshotMeta firstArchivedBlock) throws IOException {
    long epoch = firstArchivedBlock.getEpoch() - 1;
    byte[] hash = firstArchivedBlock.getParentHash();
    if (base != null) {
      if (base.epoch != epoch || !Arrays.equals(base.hash, hash)) {
        throw new ArchivePersistenceException("Archive input does not extend the manifest base");
      }
      return;
    }
    BaseIdentity identity = new BaseIdentity(scopeIdentity, epoch, hash, participants);
    byte[] encoded = encode(identity);
    Path temporary = directory.resolve(".MANIFEST-" + UUID.randomUUID());
    Files.write(temporary, encoded);
    try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(temporary,
        java.nio.file.StandardOpenOption.WRITE)) {
      channel.force(true);
    }
    try {
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException failure) {
      Files.deleteIfExists(temporary);
      throw new IOException("Atomic archive manifest publication is not supported", failure);
    }
    HistorySegmentStore.syncDirectory(directory);
    base = identity;
  }

  private static byte[] encode(BaseIdentity identity) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(MAGIC);
    output.writeShort(VERSION);
    output.writeShort(0);
    output.writeInt(0);
    writeString(output, identity.scopeIdentity);
    output.writeLong(identity.epoch);
    output.write(identity.hash);
    output.writeInt(identity.participants.size());
    for (String participant : identity.participants) {
      byte[] name = participant.getBytes(StandardCharsets.UTF_8);
      output.writeInt(name.length);
      output.write(name);
    }
    output.flush();
    byte[] payload = bytes.toByteArray();
    int length = payload.length + Integer.BYTES;
    if (length > MAX_LENGTH) {
      throw new IllegalArgumentException("Archive manifest is too large");
    }
    ByteBuffer.wrap(payload).putInt(8, length);
    ByteArrayOutputStream encoded = new ByteArrayOutputStream(length);
    encoded.write(payload);
    try (DataOutputStream checksum = new DataOutputStream(encoded)) {
      checksum.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      checksum.flush();
    }
    return encoded.toByteArray();
  }

  private static BaseIdentity decode(byte[] encoded) throws IOException {
    if (encoded.length < 64 || encoded.length > MAX_LENGTH) {
      throw new ArchivePersistenceException("Archive manifest length is invalid");
    }
    int checksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new ArchivePersistenceException("Archive manifest checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new ArchivePersistenceException("Unsupported archive manifest header");
      }
      String scopeIdentity = readString(input, "Archive manifest scope identity is invalid");
      long epoch = input.readLong();
      byte[] hash = new byte[32];
      input.readFully(hash);
      int count = input.readInt();
      if (epoch < -1 || count <= 0 || count > 1024) {
        throw new ArchivePersistenceException("Archive manifest identity is invalid");
      }
      List<String> participants = new ArrayList<>(count);
      String previous = null;
      for (int i = 0; i < count; i++) {
        int length = input.readInt();
        if (length <= 0 || length > 1024) {
          throw new ArchivePersistenceException("Archive manifest participant is invalid");
        }
        byte[] name = new byte[length];
        input.readFully(name);
        String participant = new String(name, StandardCharsets.UTF_8);
        if (previous != null && previous.compareTo(participant) >= 0) {
          throw new ArchivePersistenceException("Archive manifest participants are not sorted");
        }
        participants.add(participant);
        previous = participant;
      }
      if (input.available() != Integer.BYTES) {
        throw new ArchivePersistenceException("Archive manifest payload mismatch");
      }
      return new BaseIdentity(scopeIdentity, epoch, hash, participants);
    }
  }

  private static String scopeIdentity(List<String> participants) {
    return ArchiveParticipantDescriptor.scopeIdentity(participants);
  }

  private static BaseIdentity loadExisting(Path path, String expectedScope,
      List<String> expectedParticipants) throws IOException {
    BaseIdentity existing = decode(Files.readAllBytes(path));
    if (!expectedScope.equals(existing.scopeIdentity)
        || !expectedParticipants.equals(existing.participants)) {
      throw new ArchivePersistenceException("Archive manifest participant set mismatch");
    }
    return existing;
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    if (encoded.length == 0 || encoded.length > 1024) {
      throw new IllegalArgumentException("Archive manifest string is invalid");
    }
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private static String readString(DataInputStream input, String error) throws IOException {
    int length = input.readInt();
    if (length <= 0 || length > 1024 || length > input.available() - Integer.BYTES) {
      throw new ArchivePersistenceException(error);
    }
    byte[] encoded = new byte[length];
    input.readFully(encoded);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  private static final class BaseIdentity {
    private final String scopeIdentity;
    private final long epoch;
    private final byte[] hash;
    private final List<String> participants;

    private BaseIdentity(String scopeIdentity, long epoch, byte[] hash,
        List<String> participants) {
      this.scopeIdentity = scopeIdentity;
      this.epoch = epoch;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.participants = new ArrayList<>(participants);
    }
  }

  /** Read-only identity returned by validation; it exposes no manifest mutation capability. */
  static final class ExistingBase {
    private final long epoch;
    private final byte[] hash;

    private ExistingBase(long epoch, byte[] hash) {
      this.epoch = epoch;
      this.hash = Arrays.copyOf(hash, hash.length);
    }

    long getEpoch() {
      return epoch;
    }

    byte[] getHash() {
      return Arrays.copyOf(hash, hash.length);
    }
  }
}
