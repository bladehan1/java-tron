package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Checksummed bounded codec for one durable target mutation plan. */
final class ArchiveTargetMutationPlanCodec {

  private static final int MAGIC = 0x54414d50; // TAMP
  private static final short VERSION = 1;
  private static final int HEADER_LENGTH = 12;
  private static final int MAX_PARTICIPANTS = 1024;
  private static final int MAX_MUTATIONS = 1_000_000;
  private static final int MAX_FIELD_LENGTH = 64 * 1024 * 1024;
  static final int MAX_ENCODED_LENGTH = 128 * 1024 * 1024;
  private final ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();

  byte[] encode(ArchiveTargetMutationPlan plan) {
    try {
      byte[] target = progressCodec.encode(plan.getTarget());
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      writeBytes(output, target);
      output.writeInt(plan.getTarget().getParticipants().size());
      for (String participant : plan.getTarget().getParticipants()) {
        List<ArchiveParticipantMutation> mutations = plan.getMutations(participant);
        if (mutations.size() > MAX_MUTATIONS) {
          throw new IllegalArgumentException("Mutation plan contains too many mutations");
        }
        output.writeInt(mutations.size());
        for (ArchiveParticipantMutation mutation : mutations) {
          writeBytes(output, mutation.getKey());
          byte[] value = mutation.getValue();
          output.writeInt(value == null ? -1 : value.length);
          if (value != null) {
            output.write(value);
          }
        }
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = Math.addExact(payload.length, Integer.BYTES);
      if (length > MAX_ENCODED_LENGTH) {
        throw new IllegalArgumentException("Mutation plan is too large");
      }
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(crc32c(payload));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected mutation-plan encoding failure", impossible);
    }
  }

  ArchiveTargetMutationPlan decode(byte[] encoded) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES
        || encoded.length > MAX_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Mutation-plan length is invalid");
    }
    int checksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    if (checksum != crc32c(payload)) {
      throw new IllegalArgumentException("Mutation-plan checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported mutation-plan header");
      }
      ArchiveProgressEnvelope target = progressCodec.decode(readBytes(input));
      int participantCount = input.readInt();
      if (participantCount <= 0 || participantCount > MAX_PARTICIPANTS
          || participantCount != target.getParticipants().size()) {
        throw new IllegalArgumentException("Mutation-plan participant count mismatch");
      }
      Map<String, List<ArchiveParticipantMutation>> mutations = new LinkedHashMap<>();
      for (String participant : target.getParticipants()) {
        int count = input.readInt();
        if (count < 0 || count > MAX_MUTATIONS) {
          throw new IllegalArgumentException("Mutation-plan mutation count is invalid");
        }
        List<ArchiveParticipantMutation> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
          byte[] key = readBytes(input);
          int valueLength = input.readInt();
          if (valueLength < -1 || valueLength > MAX_FIELD_LENGTH
              || valueLength > input.available() - Integer.BYTES) {
            throw new IllegalArgumentException("Mutation-plan value length is invalid");
          }
          values.add(valueLength == -1 ? ArchiveParticipantMutation.delete(key)
              : ArchiveParticipantMutation.put(key, readExact(input, valueLength)));
        }
        mutations.put(participant, values);
      }
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Mutation-plan payload mismatch");
      }
      return new ArchiveTargetMutationPlan(target, mutations);
    } catch (EOFException truncated) {
      throw new IllegalArgumentException("Mutation plan is truncated", truncated);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Invalid mutation plan", invalid);
    }
  }

  byte[] digest(ArchiveTargetMutationPlan plan) {
    return Hashing.sha256().hashBytes(encode(plan)).asBytes();
  }

  private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    if (value.length > MAX_FIELD_LENGTH) {
      throw new IllegalArgumentException("Mutation-plan field is too large");
    }
    output.writeInt(value.length);
    output.write(value);
  }

  private static byte[] readBytes(DataInputStream input) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > MAX_FIELD_LENGTH
        || length > input.available() - Integer.BYTES) {
      throw new IllegalArgumentException("Mutation-plan field length is invalid");
    }
    return readExact(input, length);
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
