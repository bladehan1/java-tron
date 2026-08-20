package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Prototype identity envelope for apply checkpoint C or one participant progress D[i]. */
public final class ArchiveProgressEnvelope {

  public enum Kind {
    APPLY_CHECKPOINT,
    PARTICIPANT_PROGRESS,
    READER_VISIBLE
  }

  private final Kind kind;
  private final String participant;
  private final long epoch;
  private final byte[] blockHash;
  private final byte[] batchId;
  private final byte[] payloadDigest;
  private final byte[] mutationPlanDigest;
  private final List<String> participants;

  public ArchiveProgressEnvelope(Kind kind, String participant, long epoch, byte[] blockHash,
      byte[] batchId, byte[] payloadDigest, List<String> participants) {
    this(kind, participant, epoch, blockHash, batchId, payloadDigest, null, participants);
  }

  public ArchiveProgressEnvelope(Kind kind, String participant, long epoch, byte[] blockHash,
      byte[] batchId, byte[] payloadDigest, byte[] mutationPlanDigest,
      List<String> participants) {
    this.kind = Objects.requireNonNull(kind, "kind");
    if (epoch < 0) {
      throw new IllegalArgumentException("Archive progress epoch must be non-negative");
    }
    this.epoch = epoch;
    this.blockHash = exactBytes(blockHash, 32, "blockHash");
    this.batchId = exactBytes(batchId, 16, "batchId");
    this.payloadDigest = exactBytes(payloadDigest, 32, "payloadDigest");
    this.mutationPlanDigest = mutationPlanDigest == null ? null
        : exactBytes(mutationPlanDigest, 32, "mutationPlanDigest");
    this.participants = validateParticipants(participants);
    if (kind != Kind.PARTICIPANT_PROGRESS) {
      if (participant != null) {
        throw new IllegalArgumentException("Global archive progress must not name one participant");
      }
      this.participant = null;
    } else {
      if (participant == null || participant.isEmpty()
          || !this.participants.contains(participant)) {
        throw new IllegalArgumentException("Participant progress identity is invalid");
      }
      this.participant = participant;
    }
  }

  public Kind getKind() {
    return kind;
  }

  public String getParticipant() {
    return participant;
  }

  public long getEpoch() {
    return epoch;
  }

  public byte[] getBlockHash() {
    return Arrays.copyOf(blockHash, blockHash.length);
  }

  public byte[] getBatchId() {
    return Arrays.copyOf(batchId, batchId.length);
  }

  public byte[] getPayloadDigest() {
    return Arrays.copyOf(payloadDigest, payloadDigest.length);
  }

  public byte[] getMutationPlanDigest() {
    return mutationPlanDigest == null ? null
        : Arrays.copyOf(mutationPlanDigest, mutationPlanDigest.length);
  }

  public List<String> getParticipants() {
    return participants;
  }

  public void requireIdentity(Kind expectedKind, String expectedParticipant, long expectedEpoch,
      byte[] expectedBlockHash, byte[] expectedBatchId, byte[] expectedPayloadDigest,
      List<String> expectedParticipants) {
    requireIdentity(expectedKind, expectedParticipant, expectedEpoch, expectedBlockHash,
        expectedBatchId, expectedPayloadDigest, mutationPlanDigest, expectedParticipants);
  }

  public void requireIdentity(Kind expectedKind, String expectedParticipant, long expectedEpoch,
      byte[] expectedBlockHash, byte[] expectedBatchId, byte[] expectedPayloadDigest,
      byte[] expectedMutationPlanDigest, List<String> expectedParticipants) {
    if (kind != expectedKind || !Objects.equals(participant, expectedParticipant)
        || epoch != expectedEpoch || !Arrays.equals(blockHash, expectedBlockHash)
        || !Arrays.equals(batchId, expectedBatchId)
        || !Arrays.equals(payloadDigest, expectedPayloadDigest)
        || !Arrays.equals(mutationPlanDigest, expectedMutationPlanDigest)
        || !participants.equals(expectedParticipants)) {
      throw new ArchivePersistenceException("Archive progress identity mismatch");
    }
  }

  private static byte[] exactBytes(byte[] value, int length, String name) {
    if (value == null || value.length != length) {
      throw new IllegalArgumentException(name + " must be exactly " + length + " bytes");
    }
    return Arrays.copyOf(value, value.length);
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
}
