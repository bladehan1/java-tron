package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Approved archive participant identity with stable Store IDs and reserved tombstones. */
final class ArchiveParticipantDescriptor {

  static final String FORMAT_ID = "archive-state/exact-26-abi-tombstone/v1";
  static final int ABI_STORE_ID = 1;

  private static final ArchiveParticipantDescriptor CURRENT =
      new ArchiveParticipantDescriptor();

  private final Map<Integer, String> activeByStoreId;
  private final Map<Integer, String> tombstonesByStoreId;
  private final Set<String> activeDatabases;
  private final Set<String> excludedDatabases;
  private final List<String> participants;

  private ArchiveParticipantDescriptor() {
    LinkedHashMap<Integer, String> stores = new LinkedHashMap<>();
    stores.put(2, "accountid-index");
    stores.put(3, "account-index");
    stores.put(4, "account");
    stores.put(5, "account-asset");
    stores.put(6, "asset-issue");
    stores.put(7, "asset-issue-v2");
    stores.put(8, "code");
    stores.put(9, "contract-state");
    stores.put(10, "contract");
    stores.put(11, "DelegatedResourceAccountIndex");
    stores.put(12, "DelegatedResource");
    stores.put(13, "delegation");
    stores.put(14, "properties");
    stores.put(15, "exchange");
    stores.put(16, "exchange-v2");
    stores.put(17, "market_account");
    stores.put(18, "market_order");
    stores.put(19, "market_pair_price_to_order");
    stores.put(20, "market_pair_to_price");
    stores.put(21, "proposal");
    stores.put(22, "storage-row");
    stores.put(23, "votes");
    stores.put(24, "witness_schedule");
    stores.put(25, "witness");
    stores.put(26, "nullifier");
    stores.put(27, "IncrementalMerkleTree");
    activeByStoreId = Collections.unmodifiableMap(stores);

    LinkedHashMap<Integer, String> tombstones = new LinkedHashMap<>();
    tombstones.put(ABI_STORE_ID, "abi");
    tombstonesByStoreId = Collections.unmodifiableMap(tombstones);

    activeDatabases = Collections.unmodifiableSet(
        new LinkedHashSet<>(activeByStoreId.values()));
    excludedDatabases = Collections.unmodifiableSet(
        new LinkedHashSet<>(tombstonesByStoreId.values()));
    List<String> sorted = new ArrayList<>(activeDatabases);
    Collections.sort(sorted);
    participants = Collections.unmodifiableList(sorted);
    validateStoreIds();
  }

  static ArchiveParticipantDescriptor current() {
    return CURRENT;
  }

  Set<String> getActiveDatabases() {
    return activeDatabases;
  }

  Set<String> getExcludedDatabases() {
    return excludedDatabases;
  }

  List<String> getParticipants() {
    return participants;
  }

  Map<Integer, String> getTombstonesByStoreId() {
    return tombstonesByStoreId;
  }

  int getStoreId(String dbName) {
    for (Map.Entry<Integer, String> entry : activeByStoreId.entrySet()) {
      if (entry.getValue().equals(dbName)) {
        return entry.getKey();
      }
    }
    for (Map.Entry<Integer, String> entry : tombstonesByStoreId.entrySet()) {
      if (entry.getValue().equals(dbName)) {
        return entry.getKey();
      }
    }
    throw new IllegalArgumentException("Unknown archive database: " + dbName);
  }

  void requireExactParticipants(Collection<String> actual) {
    List<String> sorted = new ArrayList<>(Objects.requireNonNull(actual, "actual"));
    Collections.sort(sorted);
    if (!participants.equals(sorted)) {
      throw new ArchivePersistenceException(
          "Archive participant descriptor does not match " + FORMAT_ID);
    }
  }

  private void validateStoreIds() {
    Set<Integer> allIds = new LinkedHashSet<>(activeByStoreId.keySet());
    allIds.addAll(tombstonesByStoreId.keySet());
    List<Integer> expected = new ArrayList<>();
    for (int storeId = 1; storeId <= 27; storeId++) {
      expected.add(storeId);
    }
    if (!allIds.equals(new LinkedHashSet<>(expected))
        || activeDatabases.size() != 26
        || !tombstonesByStoreId.equals(
            Collections.singletonMap(ABI_STORE_ID, "abi"))
        || !Collections.disjoint(activeDatabases, excludedDatabases)
        || !activeDatabases.containsAll(
            Arrays.asList("asset-issue", "asset-issue-v2"))) {
      throw new IllegalStateException("Invalid exact-26 archive participant descriptor");
    }
  }
}
