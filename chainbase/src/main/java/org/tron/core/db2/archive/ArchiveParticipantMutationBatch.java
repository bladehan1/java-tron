package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

/** Immutable producer payload for one committed target's exact physical participant mutations. */
public final class ArchiveParticipantMutationBatch {

  private final long targetEpoch;
  private final byte[] blockHash;
  private final byte[] batchId;
  private final byte[] historyPayloadDigest;
  private final String accountAssetFormatId;
  private final Phase targetPhase;
  private final List<String> participants;
  private final List<Mutation> mutations;

  public ArchiveParticipantMutationBatch(HistoryCommitMarker target, Phase targetPhase,
      List<Mutation> mutations) {
    this(target, P66AccountAssetCodec.FORMAT_ID, targetPhase, mutations);
  }

  ArchiveParticipantMutationBatch(HistoryCommitMarker target, String accountAssetFormatId,
      Phase targetPhase, List<Mutation> mutations) {
    HistoryCommitMarker checkedTarget = Objects.requireNonNull(target, "target");
    targetEpoch = checkedTarget.getMeta().getEpoch();
    blockHash = checkedTarget.getMeta().getBlockHash();
    batchId = checkedTarget.getBatchId();
    historyPayloadDigest = checkedTarget.getHistoryLocation().getBodyDigest();
    this.accountAssetFormatId = Objects.requireNonNull(accountAssetFormatId,
        "accountAssetFormatId");
    if (accountAssetFormatId.isEmpty()) {
      throw new IllegalArgumentException("AccountAsset transition format must not be empty");
    }
    this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
    participants = Collections.unmodifiableList(
        new ArrayList<>(checkedTarget.getDatabases()));
    List<Mutation> copy = new ArrayList<>(Objects.requireNonNull(mutations, "mutations"));
    if (copy.contains(null)) {
      throw new IllegalArgumentException("Participant mutation batch contains null mutation");
    }
    this.mutations = Collections.unmodifiableList(copy);
  }

  public long getTargetEpoch() {
    return targetEpoch;
  }

  byte[] getBlockHash() {
    return Arrays.copyOf(blockHash, blockHash.length);
  }

  byte[] getBatchId() {
    return Arrays.copyOf(batchId, batchId.length);
  }

  byte[] getHistoryPayloadDigest() {
    return Arrays.copyOf(historyPayloadDigest, historyPayloadDigest.length);
  }

  String getAccountAssetFormatId() {
    return accountAssetFormatId;
  }

  Phase getTargetPhase() {
    return targetPhase;
  }

  List<String> getParticipants() {
    return participants;
  }

  List<Mutation> getMutations() {
    return mutations;
  }

  /** One immutable put/delete against an exact participant physical key. */
  public static final class Mutation {
    private final String dbName;
    private final byte[] physicalRawKey;
    private final byte[] value;

    private Mutation(String dbName, byte[] physicalRawKey, byte[] value) {
      if (dbName == null || dbName.isEmpty()) {
        throw new IllegalArgumentException("Participant mutation dbName must not be empty");
      }
      this.dbName = dbName;
      this.physicalRawKey = Arrays.copyOf(
          Objects.requireNonNull(physicalRawKey, "physicalRawKey"), physicalRawKey.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public static Mutation put(String dbName, byte[] physicalRawKey, byte[] value) {
      return new Mutation(dbName, physicalRawKey, Objects.requireNonNull(value, "value"));
    }

    public static Mutation delete(String dbName, byte[] physicalRawKey) {
      return new Mutation(dbName, physicalRawKey, null);
    }

    String getDbName() {
      return dbName;
    }

    byte[] getPhysicalRawKey() {
      return Arrays.copyOf(physicalRawKey, physicalRawKey.length);
    }

    byte[] getValue() {
      return value == null ? null : Arrays.copyOf(value, value.length);
    }
  }
}
