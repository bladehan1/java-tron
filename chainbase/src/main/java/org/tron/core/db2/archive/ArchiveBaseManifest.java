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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Durable generation base identity for the experimental state archive. */
final class ArchiveBaseManifest {

  private static final int MAGIC = 0x54414d46; // TAMF
  private static final short VERSION = 1;
  private static final int MAX_LENGTH = 1024 * 1024;

  private final Path directory;
  private final Path path;
  private final List<String> participants;
  private BaseIdentity base;

  ArchiveBaseManifest(Path directory, List<String> participants) throws IOException {
    this.directory = directory;
    this.path = directory.resolve("MANIFEST");
    this.participants = new ArrayList<>(participants);
    Files.createDirectories(directory);
    if (Files.exists(path)) {
      base = decode(Files.readAllBytes(path));
      if (!this.participants.equals(base.participants)) {
        throw new ArchivePersistenceException("Archive manifest participant set mismatch");
      }
    }
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
    BaseIdentity identity = new BaseIdentity(epoch, hash, participants);
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
      return new BaseIdentity(epoch, hash, participants);
    }
  }

  private static final class BaseIdentity {
    private final long epoch;
    private final byte[] hash;
    private final List<String> participants;

    private BaseIdentity(long epoch, byte[] hash, List<String> participants) {
      this.epoch = epoch;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.participants = new ArrayList<>(participants);
    }
  }
}
