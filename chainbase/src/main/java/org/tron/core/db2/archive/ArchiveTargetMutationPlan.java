package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Immutable target H identity plus exact per-participant business mutations. */
final class ArchiveTargetMutationPlan {

  private final ArchiveProgressEnvelope target;
  private final String accountAssetFormatId;
  private final Phase targetPhase;
  private final Map<String, List<ArchiveParticipantMutation>> mutations;

  ArchiveTargetMutationPlan(ArchiveProgressEnvelope target, String accountAssetFormatId,
      Phase targetPhase, Map<String, ? extends List<ArchiveParticipantMutation>> mutations) {
    this.target = Objects.requireNonNull(target, "target");
    this.accountAssetFormatId = Objects.requireNonNull(accountAssetFormatId,
        "accountAssetFormatId");
    if (accountAssetFormatId.isEmpty()) {
      throw new IllegalArgumentException("AccountAsset transition format must not be empty");
    }
    this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
    if (target.getKind() != Kind.APPLY_CHECKPOINT || target.getParticipant() != null) {
      throw new IllegalArgumentException("Mutation plan target must be a global checkpoint");
    }
    if (target.getMutationPlanDigest() != null) {
      throw new IllegalArgumentException("Mutation plan target must not contain its own digest");
    }
    TreeMap<String, ? extends List<ArchiveParticipantMutation>> checked = new TreeMap<>(
        Objects.requireNonNull(mutations, "mutations"));
    if (!new ArrayList<>(checked.keySet()).equals(target.getParticipants())
        || checked.containsValue(null)) {
      throw new IllegalArgumentException("Mutation plan participant set mismatch");
    }
    Map<String, List<ArchiveParticipantMutation>> copy = new LinkedHashMap<>();
    for (String participant : target.getParticipants()) {
      List<ArchiveParticipantMutation> values = new ArrayList<>(
          Objects.requireNonNull(checked.get(participant), "participant mutations"));
      if (values.contains(null)) {
        throw new IllegalArgumentException("Mutation plan contains null mutation");
      }
      values.sort((left, right) -> BlockReverseDiff.compareUnsigned(
          left.getKey(), right.getKey()));
      byte[] previous = null;
      for (ArchiveParticipantMutation mutation : values) {
        byte[] key = mutation.getKey();
        if (previous != null && BlockReverseDiff.compareUnsigned(previous, key) == 0) {
          throw new IllegalArgumentException("Mutation plan contains duplicate physical key");
        }
        previous = key;
      }
      copy.put(participant, Collections.unmodifiableList(values));
    }
    this.mutations = Collections.unmodifiableMap(copy);
  }

  ArchiveProgressEnvelope getTarget() {
    return target;
  }

  String getAccountAssetFormatId() {
    return accountAssetFormatId;
  }

  Phase getTargetPhase() {
    return targetPhase;
  }

  List<ArchiveParticipantMutation> getMutations(String participant) {
    List<ArchiveParticipantMutation> values = mutations.get(participant);
    if (values == null) {
      throw new ArchivePersistenceException("Unknown mutation-plan participant: " + participant);
    }
    return values;
  }

  Map<String, List<ArchiveParticipantMutation>> getMutations() {
    return mutations;
  }

  byte[] digest() {
    return new ArchiveTargetMutationPlanCodec().digest(this);
  }

  void requireIdentity(HistoryCommitMarker marker, List<String> participants) {
    target.requireIdentity(Kind.APPLY_CHECKPOINT, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), participants);
  }
}
