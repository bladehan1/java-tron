package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Fail-closed participant registry for one path-state root format.
 *
 * <p>TASK-016 has approved ABI and both AssetIssue stores for inclusion, while the complete
 * execution-state exact-set remains an H1-L1 gate. This class therefore validates the supplied
 * immutable scope and the three mandatory names without inventing the remaining registry.
 */
public final class PathStateParticipantScope {

  public static final String ABI_DB = "abi";
  public static final String ASSET_ISSUE_DB = "asset-issue";
  public static final String ASSET_ISSUE_V2_DB = "asset-issue-v2";

  private final List<PathStateParticipant> participants;
  private final Map<String, PathStateParticipant> participantsByName;

  public PathStateParticipantScope(Collection<PathStateParticipant> participants) {
    List<PathStateParticipant> sorted = new ArrayList<>(
        Objects.requireNonNull(participants, "participants"));
    if (sorted.isEmpty()) {
      throw new IllegalArgumentException("participant scope must not be empty");
    }
    Collections.sort(sorted);

    Set<Integer> storeIds = new LinkedHashSet<>();
    Map<String, PathStateParticipant> byName = new LinkedHashMap<>();
    for (PathStateParticipant participant : sorted) {
      PathStateParticipant present = Objects.requireNonNull(participant, "participant");
      if (!storeIds.add(present.getStoreId())) {
        throw new IllegalArgumentException("duplicate Store ID: " + present.getStoreId());
      }
      if (byName.put(present.getDbName(), present) != null) {
        throw new IllegalArgumentException("duplicate database: " + present.getDbName());
      }
    }
    requireMandatory(byName, ABI_DB);
    requireMandatory(byName, ASSET_ISSUE_DB);
    requireMandatory(byName, ASSET_ISSUE_V2_DB);
    this.participants = Collections.unmodifiableList(sorted);
    this.participantsByName = Collections.unmodifiableMap(byName);
  }

  public List<PathStateParticipant> getParticipants() {
    return participants;
  }

  public PathStateParticipant require(String dbName) {
    PathStateParticipant participant = participantsByName.get(
        Objects.requireNonNull(dbName, "dbName"));
    if (participant == null) {
      throw new IllegalArgumentException("unknown path-state participant: " + dbName);
    }
    return participant;
  }

  private static void requireMandatory(Map<String, PathStateParticipant> participants,
      String dbName) {
    if (!participants.containsKey(dbName)) {
      throw new IllegalArgumentException("missing mandatory path-state participant: " + dbName);
    }
  }
}
