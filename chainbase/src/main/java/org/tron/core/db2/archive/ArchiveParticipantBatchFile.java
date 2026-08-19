package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Atomic-file prototype containing participant business bytes and D[i] in one durable unit. */
public final class ArchiveParticipantBatchFile {

  private static final int MAGIC = 0x54414254; // TABT
  private static final short VERSION = 1;
  private static final int HEADER_LENGTH = 20;
  private static final int MAX_BUSINESS_LENGTH = 64 * 1024 * 1024;

  private final Path path;
  private final Path temporary;
  private final String participant;
  private final List<String> participants;
  private final ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
  private final FaultHook faultHook;

  public ArchiveParticipantBatchFile(Path path, String participant, List<String> participants) {
    this(path, participant, participants, temporary -> { });
  }

  ArchiveParticipantBatchFile(Path path, String participant, List<String> participants,
      FaultHook faultHook) {
    this.path = Objects.requireNonNull(path, "path");
    this.temporary = path.resolveSibling(path.getFileName() + ".tmp");
    this.participants = validateParticipants(participants);
    if (participant == null || participant.isEmpty() || !this.participants.contains(participant)) {
      throw new IllegalArgumentException("Archive batch participant is invalid");
    }
    this.participant = participant;
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public void store(byte[] businessPayload, ArchiveProgressEnvelope progress) throws IOException {
    byte[] encoded = encode(businessPayload, progress);
    Path directory = Objects.requireNonNull(path.getParent(), "participant batch directory");
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
          "Archive participant filesystem does not support atomic replacement", unsupported);
    }
    HistorySegmentStore.syncDirectory(directory);
  }

  public Snapshot load() throws IOException {
    if (!Files.exists(path)) {
      throw new ArchivePersistenceException("Archive participant batch is missing: " + path);
    }
    try {
      return decode(Files.readAllBytes(path));
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive participant batch is corrupt", invalid);
    }
  }

  Path getTemporaryPath() {
    return temporary;
  }

  private byte[] encode(byte[] businessPayload, ArchiveProgressEnvelope progress) {
    byte[] business = Arrays.copyOf(Objects.requireNonNull(businessPayload, "businessPayload"),
        businessPayload.length);
    if (business.length > MAX_BUSINESS_LENGTH) {
      throw new IllegalArgumentException("Archive participant business payload is too large");
    }
    requireProgress(progress);
    byte[] encodedProgress = progressCodec.encode(progress);
    int length = HEADER_LENGTH + business.length + encodedProgress.length + Integer.BYTES;
    ByteBuffer buffer = ByteBuffer.allocate(length);
    buffer.putInt(MAGIC).putShort(VERSION).putShort((short) 0).putInt(length)
        .putInt(business.length).putInt(encodedProgress.length).put(business).put(encodedProgress);
    byte[] payload = Arrays.copyOf(buffer.array(), length - Integer.BYTES);
    buffer.putInt(Hashing.crc32c().hashBytes(payload).asInt());
    return buffer.array();
  }

  private Snapshot decode(byte[] encoded) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES) {
      throw new IllegalArgumentException("Archive participant batch length is invalid");
    }
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    if (buffer.getInt() != MAGIC || buffer.getShort() != VERSION || buffer.getShort() != 0
        || buffer.getInt() != encoded.length) {
      throw new IllegalArgumentException("Unsupported archive participant batch header");
    }
    int businessLength = buffer.getInt();
    int progressLength = buffer.getInt();
    long expectedLength = HEADER_LENGTH + (long) businessLength + progressLength + Integer.BYTES;
    if (businessLength < 0 || businessLength > MAX_BUSINESS_LENGTH
        || progressLength <= 0 || expectedLength != encoded.length) {
      throw new IllegalArgumentException("Archive participant batch payload length is invalid");
    }
    int expectedChecksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    if (expectedChecksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Archive participant batch checksum mismatch");
    }
    byte[] business = new byte[businessLength];
    buffer.get(business);
    byte[] encodedProgress = new byte[progressLength];
    buffer.get(encodedProgress);
    ArchiveProgressEnvelope progress = progressCodec.decode(encodedProgress);
    requireProgress(progress);
    return new Snapshot(business, progress);
  }

  private void requireProgress(ArchiveProgressEnvelope progress) {
    Objects.requireNonNull(progress, "progress");
    if (progress.getKind() != Kind.PARTICIPANT_PROGRESS
        || !participant.equals(progress.getParticipant())
        || !participants.equals(progress.getParticipants())) {
      throw new IllegalArgumentException("Archive participant batch progress identity mismatch");
    }
  }

  private static List<String> validateParticipants(List<String> participants) {
    Objects.requireNonNull(participants, "participants");
    if (participants.isEmpty()) {
      throw new IllegalArgumentException("Archive participant set must not be empty");
    }
    List<String> copy = new ArrayList<>(participants.size());
    String previous = null;
    for (String participant : participants) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Archive participants must be non-empty, unique, and sorted");
      }
      copy.add(participant);
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  public static final class Snapshot {
    private final byte[] businessPayload;
    private final ArchiveProgressEnvelope progress;

    private Snapshot(byte[] businessPayload, ArchiveProgressEnvelope progress) {
      this.businessPayload = Arrays.copyOf(businessPayload, businessPayload.length);
      this.progress = progress;
    }

    public byte[] getBusinessPayload() {
      return Arrays.copyOf(businessPayload, businessPayload.length);
    }

    public ArchiveProgressEnvelope getProgress() {
      return progress;
    }
  }

  @FunctionalInterface
  interface FaultHook {
    void afterTemporaryForce(Path temporary) throws IOException;
  }
}
