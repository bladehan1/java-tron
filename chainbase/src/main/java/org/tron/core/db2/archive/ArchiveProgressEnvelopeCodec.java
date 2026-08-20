package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Checksummed prototype codec for archive C, D[i], and R progress identities. */
public final class ArchiveProgressEnvelopeCodec {

  private static final int MAGIC = 0x54415047; // TAPG
  private static final short VERSION = 3;
  private static final int HEADER_LENGTH = 12;
  private static final int MAX_FIELD_LENGTH = 1024;
  private static final int MAX_PARTICIPANTS = 1024;
  static final int MAX_ENCODED_LENGTH = 1024 * 1024;

  public byte[] encode(ArchiveProgressEnvelope envelope) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      byte[] mutationPlanDigest = envelope.getMutationPlanDigest();
      output.writeShort(VERSION);
      output.writeByte(kindCode(envelope.getKind()));
      output.writeByte(0);
      output.writeInt(0);
      writeString(output, envelope.getScopeIdentity());
      output.writeLong(envelope.getEpoch());
      output.write(envelope.getBlockHash());
      output.write(envelope.getBatchId());
      output.write(envelope.getPayloadDigest());
      output.writeBoolean(mutationPlanDigest != null);
      if (mutationPlanDigest != null) {
        output.write(mutationPlanDigest);
      }
      writeString(output, envelope.getParticipant() == null ? "" : envelope.getParticipant());
      output.writeInt(envelope.getParticipants().size());
      for (String participant : envelope.getParticipants()) {
        writeString(output, participant);
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = payload.length + Integer.BYTES;
      if (length > MAX_ENCODED_LENGTH) {
        throw new IllegalArgumentException("Archive progress envelope is too large");
      }
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(crc32c(payload));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected progress envelope encoding failure", impossible);
    }
  }

  public ArchiveProgressEnvelope decode(byte[] encoded) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES
        || encoded.length > MAX_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Archive progress envelope length is invalid");
    }
    int expectedChecksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    if (expectedChecksum != crc32c(payload)) {
      throw new IllegalArgumentException("Archive progress envelope checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      int magic = input.readInt();
      short version = input.readShort();
      if (magic != MAGIC || version != VERSION) {
        throw new IllegalArgumentException("Unsupported archive progress envelope header");
      }
      Kind kind = decodeKind(input.readUnsignedByte());
      if (input.readUnsignedByte() != 0 || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported archive progress envelope header");
      }
      String scopeIdentity = readString(input, false);
      long epoch = input.readLong();
      byte[] blockHash = readExact(input, 32);
      byte[] batchId = readExact(input, 16);
      byte[] payloadDigest = readExact(input, 32);
      int hasMutationPlanDigest = input.readUnsignedByte();
      if (hasMutationPlanDigest > 1) {
        throw new IllegalArgumentException("Archive progress plan digest marker is invalid");
      }
      byte[] mutationPlanDigest = hasMutationPlanDigest == 1 ? readExact(input, 32) : null;
      String participant = readString(input, true);
      int count = input.readInt();
      if (count <= 0 || count > MAX_PARTICIPANTS) {
        throw new IllegalArgumentException("Archive progress participant count is invalid");
      }
      List<String> participants = new ArrayList<>(count);
      for (int index = 0; index < count; index++) {
        participants.add(readString(input, false));
      }
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Archive progress envelope payload mismatch");
      }
      String expectedScope = ArchiveParticipantDescriptor.scopeIdentity(participants);
      if (!scopeIdentity.equals(expectedScope)) {
        throw new IllegalArgumentException("Archive progress scope identity mismatch");
      }
      return new ArchiveProgressEnvelope(kind, participant.isEmpty() ? null : participant, epoch,
          blockHash, batchId, payloadDigest, mutationPlanDigest, participants, scopeIdentity);
    } catch (EOFException truncated) {
      throw new IllegalArgumentException("Archive progress envelope is truncated", truncated);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Invalid archive progress envelope", invalid);
    }
  }

  private static int kindCode(Kind kind) {
    switch (kind) {
      case APPLY_CHECKPOINT:
        return 1;
      case PARTICIPANT_PROGRESS:
        return 2;
      case READER_VISIBLE:
        return 3;
      default:
        throw new IllegalArgumentException("Unknown archive progress envelope kind");
    }
  }

  private static Kind decodeKind(int code) {
    if (code == 1) {
      return Kind.APPLY_CHECKPOINT;
    }
    if (code == 2) {
      return Kind.PARTICIPANT_PROGRESS;
    }
    if (code == 3) {
      return Kind.READER_VISIBLE;
    }
    throw new IllegalArgumentException("Unknown archive progress envelope kind");
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    if (encoded.length > MAX_FIELD_LENGTH) {
      throw new IllegalArgumentException("Archive progress string is too large");
    }
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private static String readString(DataInputStream input, boolean allowEmpty) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > MAX_FIELD_LENGTH || !allowEmpty && length == 0) {
      throw new IllegalArgumentException("Archive progress string length is invalid");
    }
    byte[] encoded = readExact(input, length);
    String decoded = new String(encoded, StandardCharsets.UTF_8);
    if (!Arrays.equals(encoded, decoded.getBytes(StandardCharsets.UTF_8))) {
      throw new IllegalArgumentException("Archive progress string is not valid UTF-8");
    }
    return decoded;
  }

  private static byte[] readExact(DataInputStream input, int length) throws IOException {
    byte[] value = new byte[length];
    input.readFully(value);
    return value;
  }

  private static int crc32c(byte[] value) {
    return Hashing.crc32c().hashBytes(value).asInt();
  }
}
