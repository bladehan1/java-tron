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

/** Stable participant and comparator identity for the current path-state root domain. */
public final class PathStateParticipantDescriptor {

  public static final String SCOPE_ID = "path-state-root/exact-27/v1";
  public static final String UNSIGNED_RAW_COMPARATOR = "unsigned-raw/v1";
  public static final String MARKET_PRICE_COMPARATOR = "market-pair-price/v1";
  public static final String MARKET_PRICE_DATABASE = "market_pair_price_to_order";

  private static final PathStateParticipantDescriptor CURRENT =
      new PathStateParticipantDescriptor();

  private final List<StoreIdentity> stores;
  private final Map<String, StoreIdentity> storesByName;

  private PathStateParticipantDescriptor() {
    LinkedHashMap<Integer, String> names = new LinkedHashMap<>();
    names.put(1, "abi");
    names.put(2, "accountid-index");
    names.put(3, "account-index");
    names.put(4, "account");
    names.put(5, "account-asset");
    names.put(6, "asset-issue");
    names.put(7, "asset-issue-v2");
    names.put(8, "code");
    names.put(9, "contract-state");
    names.put(10, "contract");
    names.put(11, "DelegatedResourceAccountIndex");
    names.put(12, "DelegatedResource");
    names.put(13, "delegation");
    names.put(14, "properties");
    names.put(15, "exchange");
    names.put(16, "exchange-v2");
    names.put(17, "market_account");
    names.put(18, "market_order");
    names.put(19, MARKET_PRICE_DATABASE);
    names.put(20, "market_pair_to_price");
    names.put(21, "proposal");
    names.put(22, "storage-row");
    names.put(23, "votes");
    names.put(24, "witness_schedule");
    names.put(25, "witness");
    names.put(26, "nullifier");
    names.put(27, "IncrementalMerkleTree");

    List<StoreIdentity> ordered = new ArrayList<>();
    LinkedHashMap<String, StoreIdentity> byName = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : names.entrySet()) {
      String comparator = MARKET_PRICE_DATABASE.equals(entry.getValue())
          ? MARKET_PRICE_COMPARATOR : UNSIGNED_RAW_COMPARATOR;
      StoreIdentity identity = new StoreIdentity(entry.getKey(), entry.getValue(), comparator);
      ordered.add(identity);
      if (byName.put(identity.getDbName(), identity) != null) {
        throw new IllegalStateException("duplicate path-state database: " + identity.getDbName());
      }
    }
    if (ordered.size() != 27) {
      throw new IllegalStateException("path-state participant descriptor must contain exact-27");
    }
    stores = Collections.unmodifiableList(ordered);
    storesByName = Collections.unmodifiableMap(byName);
  }

  public static PathStateParticipantDescriptor current() {
    return CURRENT;
  }

  public List<StoreIdentity> getStores() {
    return stores;
  }

  public StoreIdentity require(String dbName) {
    StoreIdentity identity = storesByName.get(Objects.requireNonNull(dbName, "dbName"));
    if (identity == null) {
      throw new IllegalArgumentException("unknown path-state database: " + dbName);
    }
    return identity;
  }

  /** Requires exact membership while allowing callers to enumerate databases in any order. */
  public void requireExactDatabases(Collection<String> dbNames) {
    Collection<String> supplied = Objects.requireNonNull(dbNames, "dbNames");
    LinkedHashSet<String> unique = new LinkedHashSet<>();
    for (String dbName : supplied) {
      if (dbName == null) {
        throw new IllegalArgumentException("path-state database must not be null");
      }
      if (!unique.add(dbName)) {
        throw new IllegalArgumentException("duplicate path-state database: " + dbName);
      }
    }
    Set<String> expected = storesByName.keySet();
    if (!expected.equals(unique)) {
      LinkedHashSet<String> missing = new LinkedHashSet<>(expected);
      missing.removeAll(unique);
      LinkedHashSet<String> unexpected = new LinkedHashSet<>(unique);
      unexpected.removeAll(expected);
      throw new IllegalArgumentException(
          "path-state exact-27 mismatch, missing=" + missing + ", unexpected=" + unexpected);
    }
  }

  /** Immutable Store identity; canonical key/value format is approved in a separate gate. */
  public static final class StoreIdentity {

    private final int storeId;
    private final String dbName;
    private final String comparatorId;

    private StoreIdentity(int storeId, String dbName, String comparatorId) {
      this.storeId = storeId;
      this.dbName = dbName;
      this.comparatorId = comparatorId;
    }

    public int getStoreId() {
      return storeId;
    }

    public String getDbName() {
      return dbName;
    }

    public String getComparatorId() {
      return comparatorId;
    }

    @Override
    public String toString() {
      return storeId + ":" + dbName + ":" + comparatorId;
    }
  }
}
