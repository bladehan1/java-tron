package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Exact 27-participant plus super target used by TASK-018 INTENT and CURRENT. */
final class PathStatePhysicalGlobalIntent {

  static final int DIGEST_LENGTH = 32;
  static final int MAX_ENCODED_LENGTH = 32 * 1024;
  private static final int PARTICIPANT_COUNT = 27;

  private static final int MAGIC = 0x50534749; // PSGI
  private static final short VERSION = 1;
  private static final int ENTRY_LENGTH = Integer.BYTES + 3 * DIGEST_LENGTH;
  private static final int FIXED_LENGTH_WITHOUT_METADATA = Integer.BYTES + Short.BYTES
      + DIGEST_LENGTH + Integer.BYTES + Integer.BYTES + 2 * DIGEST_LENGTH + DIGEST_LENGTH;

  private final byte[] formatDigest;
  private final PathStateRootMetadata metadata;
  private final List<ParticipantTarget> participants;
  private final byte[] superGeneration;
  private final byte[] superRoot;

  PathStatePhysicalGlobalIntent(byte[] formatDigest, PathStateRootMetadata metadata,
      List<ParticipantTarget> participants, byte[] superGeneration, byte[] superRoot) {
    this.formatDigest = digest(formatDigest, "formatDigest");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    if (!Arrays.equals(this.formatDigest, metadata.getFormatDigest())) {
      throw new IllegalArgumentException("physical global intent metadata format differs");
    }
    List<ParticipantTarget> supplied = new ArrayList<>(
        Objects.requireNonNull(participants, "participants"));
    if (supplied.size() != PARTICIPANT_COUNT) {
      throw new IllegalArgumentException("physical global intent must contain exact-27 participants");
    }
    int previousStoreId = 0;
    for (ParticipantTarget target : supplied) {
      ParticipantTarget present = Objects.requireNonNull(target, "participant target");
      if (present.storeId <= previousStoreId) {
        throw new IllegalArgumentException(
            "physical global intent Store IDs must be strictly ascending");
      }
      previousStoreId = present.storeId;
    }
    this.participants = Collections.unmodifiableList(supplied);
    this.superGeneration = digest(superGeneration, "superGeneration");
    this.superRoot = digest(superRoot, "superRoot");
    if (!Arrays.equals(this.superRoot, metadata.getStateRoot())) {
      throw new IllegalArgumentException("physical global intent metadata root differs");
    }
  }

  byte[] encode() {
    byte[] encodedMetadata = metadata.encode();
    int payloadLength = FIXED_LENGTH_WITHOUT_METADATA - DIGEST_LENGTH
        + encodedMetadata.length + participants.size() * ENTRY_LENGTH;
    ByteBuffer payload = ByteBuffer.allocate(payloadLength);
    payload.putInt(MAGIC).putShort(VERSION).put(formatDigest)
        .putInt(encodedMetadata.length).put(encodedMetadata).putInt(participants.size());
    for (ParticipantTarget participant : participants) {
      payload.putInt(participant.storeId).put(participant.generation)
          .put(participant.flatDigest).put(participant.storeRoot);
    }
    payload.put(superGeneration).put(superRoot);
    byte[] body = payload.array();
    return ByteBuffer.allocate(body.length + DIGEST_LENGTH).put(body)
        .put(Hashing.sha256().hashBytes(body).asBytes()).array();
  }

  static PathStatePhysicalGlobalIntent decode(byte[] encoded) {
    byte[] supplied = Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length);
    if (supplied.length < FIXED_LENGTH_WITHOUT_METADATA || supplied.length > MAX_ENCODED_LENGTH) {
      throw new IllegalArgumentException("physical global intent length is invalid");
    }
    int payloadLength = supplied.length - DIGEST_LENGTH;
    byte[] body = Arrays.copyOf(supplied, payloadLength);
    byte[] checksum = Arrays.copyOfRange(supplied, payloadLength, supplied.length);
    if (!Arrays.equals(checksum, Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IllegalArgumentException("physical global intent checksum differs");
    }
    ByteBuffer input = ByteBuffer.wrap(body);
    if (input.getInt() != MAGIC || input.getShort() != VERSION) {
      throw new IllegalArgumentException("physical global intent format is unsupported");
    }
    byte[] formatDigest = readDigest(input);
    int metadataLength = input.getInt();
    if (metadataLength <= 0 || metadataLength > input.remaining() - Integer.BYTES
        - 2 * DIGEST_LENGTH) {
      throw new IllegalArgumentException("physical global intent metadata length is invalid");
    }
    byte[] encodedMetadata = new byte[metadataLength];
    input.get(encodedMetadata);
    PathStateRootMetadata metadata = PathStateRootMetadata.decode(encodedMetadata);
    int count = input.getInt();
    if (count != PARTICIPANT_COUNT
        || body.length != FIXED_LENGTH_WITHOUT_METADATA - DIGEST_LENGTH
        + metadataLength + count * ENTRY_LENGTH) {
      throw new IllegalArgumentException("physical global intent participant count is invalid");
    }
    List<ParticipantTarget> participants = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      participants.add(new ParticipantTarget(input.getInt(), readDigest(input),
          readDigest(input), readDigest(input)));
    }
    return new PathStatePhysicalGlobalIntent(formatDigest, metadata, participants,
        readDigest(input), readDigest(input));
  }

  byte[] getFormatDigest() {
    return Arrays.copyOf(formatDigest, formatDigest.length);
  }

  List<ParticipantTarget> getParticipants() {
    return participants;
  }

  PathStateRootMetadata getMetadata() {
    return PathStateRootMetadata.decode(metadata.encode());
  }

  byte[] getSuperGeneration() {
    return Arrays.copyOf(superGeneration, superGeneration.length);
  }

  byte[] getSuperRoot() {
    return Arrays.copyOf(superRoot, superRoot.length);
  }

  private static byte[] readDigest(ByteBuffer input) {
    byte[] value = new byte[DIGEST_LENGTH];
    input.get(value);
    return value;
  }

  private static byte[] digest(byte[] value, String name) {
    byte[] supplied = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (supplied.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return supplied;
  }

  static final class ParticipantTarget {

    private final int storeId;
    private final byte[] generation;
    private final byte[] flatDigest;
    private final byte[] storeRoot;

    ParticipantTarget(int storeId, byte[] generation, byte[] flatDigest, byte[] storeRoot) {
      if (storeId <= 0) {
        throw new IllegalArgumentException("storeId must be positive");
      }
      this.storeId = storeId;
      this.generation = digest(generation, "generation");
      this.flatDigest = digest(flatDigest, "flatDigest");
      this.storeRoot = digest(storeRoot, "storeRoot");
    }

    int getStoreId() {
      return storeId;
    }

    byte[] getGeneration() {
      return Arrays.copyOf(generation, generation.length);
    }

    byte[] getFlatDigest() {
      return Arrays.copyOf(flatDigest, flatDigest.length);
    }

    byte[] getStoreRoot() {
      return Arrays.copyOf(storeRoot, storeRoot.length);
    }
  }
}
