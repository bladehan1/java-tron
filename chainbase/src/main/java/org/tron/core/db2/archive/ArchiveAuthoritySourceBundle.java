package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable, read-only snapshots of authorities needed for archive startup admission.
 *
 * <p>The bundle deliberately does not know how to open engines or repair files. A future runtime
 * adapter must collect every value from already-opened read-only sources before calling the
 * validator.
 */
public final class ArchiveAuthoritySourceBundle {

  private final boolean mutationPlanPresent;
  private final HistoryCoverage historyCoverage;
  private final HistoryCommitMarker firstHistoryMarker;
  private final HistoryCommitMarker headHistoryMarker;
  private final ArchiveProgressEnvelope applyCheckpoint;
  private final Map<String, ArchiveProgressEnvelope> participantProgress;
  private final ArchiveProgressEnvelope readerVisible;
  private final ServingGenerationSnapshot servingGeneration;
  private final byte[] latestSourceIdentityDigest;

  public ArchiveAuthoritySourceBundle(boolean mutationPlanPresent,
      HistoryCoverage historyCoverage, HistoryCommitMarker firstHistoryMarker,
      HistoryCommitMarker headHistoryMarker,
      ArchiveProgressEnvelope applyCheckpoint,
      Map<String, ArchiveProgressEnvelope> participantProgress,
      ArchiveProgressEnvelope readerVisible, ServingGenerationSnapshot servingGeneration,
      byte[] latestSourceIdentityDigest) {
    this.mutationPlanPresent = mutationPlanPresent;
    this.historyCoverage = historyCoverage;
    this.firstHistoryMarker = firstHistoryMarker;
    this.headHistoryMarker = headHistoryMarker;
    this.applyCheckpoint = applyCheckpoint;
    this.participantProgress = participantProgress == null ? null
        : Collections.unmodifiableMap(new LinkedHashMap<>(participantProgress));
    this.readerVisible = readerVisible;
    this.servingGeneration = servingGeneration;
    this.latestSourceIdentityDigest = copy(latestSourceIdentityDigest);
  }

  boolean isMutationPlanPresent() {
    return mutationPlanPresent;
  }

  HistoryCoverage getHistoryCoverage() {
    return historyCoverage;
  }

  HistoryCommitMarker getFirstHistoryMarker() {
    return firstHistoryMarker;
  }

  HistoryCommitMarker getHeadHistoryMarker() {
    return headHistoryMarker;
  }

  ArchiveProgressEnvelope getApplyCheckpoint() {
    return applyCheckpoint;
  }

  Map<String, ArchiveProgressEnvelope> getParticipantProgress() {
    return participantProgress;
  }

  ArchiveProgressEnvelope getReaderVisible() {
    return readerVisible;
  }

  ServingGenerationSnapshot getServingGeneration() {
    return servingGeneration;
  }

  byte[] getLatestSourceIdentityDigest() {
    return copy(latestSourceIdentityDigest);
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  /** Read-only serving generation/catalog identity, independent of its physical engine. */
  public static final class ServingGenerationSnapshot {
    private final String scopeIdentity;
    private final List<String> participants;
    private final long indexedFromEpoch;
    private final long indexedThroughEpoch;
    private final byte[] headHash;
    private final byte[] authoritativePrefixDigest;
    private final byte[] latestSourceIdentityDigest;

    public ServingGenerationSnapshot(String scopeIdentity, List<String> participants,
        long indexedFromEpoch, long indexedThroughEpoch, byte[] headHash,
        byte[] authoritativePrefixDigest, byte[] latestSourceIdentityDigest) {
      this.scopeIdentity = scopeIdentity;
      this.participants = participants == null ? null
          : Collections.unmodifiableList(new java.util.ArrayList<>(participants));
      this.indexedFromEpoch = indexedFromEpoch;
      this.indexedThroughEpoch = indexedThroughEpoch;
      this.headHash = copy(headHash);
      this.authoritativePrefixDigest = copy(authoritativePrefixDigest);
      this.latestSourceIdentityDigest = copy(latestSourceIdentityDigest);
    }

    String getScopeIdentity() {
      return scopeIdentity;
    }

    List<String> getParticipants() {
      return participants;
    }

    long getIndexedFromEpoch() {
      return indexedFromEpoch;
    }

    long getIndexedThroughEpoch() {
      return indexedThroughEpoch;
    }

    byte[] getHeadHash() {
      return copy(headHash);
    }

    byte[] getAuthoritativePrefixDigest() {
      return copy(authoritativePrefixDigest);
    }

    byte[] getLatestSourceIdentityDigest() {
      return copy(latestSourceIdentityDigest);
    }
  }
}
