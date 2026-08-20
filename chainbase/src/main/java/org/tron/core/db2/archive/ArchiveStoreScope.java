package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.tron.core.db2.core.Chainbase;

/** Explicit classification of every database registered with {@code SnapshotManager}. */
public final class ArchiveStoreScope {

  private static final Set<String> STATE_DATABASES = immutableSet(
      "accountid-index",
      "account-index",
      "account",
      "account-asset",
      "asset-issue",
      "asset-issue-v2",
      "code",
      "contract-state",
      "contract",
      "DelegatedResourceAccountIndex",
      "DelegatedResource",
      "delegation",
      "properties",
      "exchange",
      "exchange-v2",
      "market_account",
      "market_order",
      "market_pair_price_to_order",
      "market_pair_to_price",
      "proposal",
      "storage-row",
      "votes",
      "witness_schedule",
      "witness",
      "nullifier",
      "IncrementalMerkleTree");

  // Candidate store ID 1 is reserved for abi and must never be reused by state history.
  private static final Set<String> EXCLUDED_DATABASES = immutableSet("abi");

  private static final Set<String> NON_STATE_DATABASES = immutableSet(
      "account-trace",
      "accountTrie",
      "balance-trace",
      "block",
      "block-index",
      "recent-block",
      "recent-transaction",
      "section-bloom",
      "trans",
      "trans-cache",
      "transactionHistoryStore",
      "transactionRetStore",
      "tree-block-index");

  private ArchiveStoreScope() {
  }

  public static boolean isStateDatabase(String dbName) {
    return STATE_DATABASES.contains(dbName);
  }

  public static boolean isClassified(String dbName) {
    return STATE_DATABASES.contains(dbName) || NON_STATE_DATABASES.contains(dbName)
        || EXCLUDED_DATABASES.contains(dbName);
  }

  public static Set<String> getStateDatabases() {
    return STATE_DATABASES;
  }

  public static Set<String> getNonStateDatabases() {
    return NON_STATE_DATABASES;
  }

  public static boolean isExcludedDatabase(String dbName) {
    return EXCLUDED_DATABASES.contains(dbName);
  }

  public static Set<String> getExcludedDatabases() {
    return EXCLUDED_DATABASES;
  }

  public static void validate(Collection<Chainbase> databases) {
    Set<String> duplicates = databases.stream()
        .collect(Collectors.groupingBy(Chainbase::getDbName, Collectors.counting()))
        .entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(java.util.Map.Entry::getKey)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    if (!duplicates.isEmpty()) {
      throw new IllegalStateException("Duplicate Chainbase dbName(s): " + duplicates);
    }

    Set<String> unknown = databases.stream()
        .filter(database -> !isClassified(database.getDbName()))
        .map(database -> database.getDbName() + " (" + database.getRegistrationSource() + ")")
        .collect(Collectors.toCollection(LinkedHashSet::new));
    if (!unknown.isEmpty()) {
      throw new IllegalStateException(
          "Archive state scope has unclassified Chainbase dbName(s): " + unknown);
    }
  }

  private static Set<String> immutableSet(String... values) {
    return Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(values)));
  }
}
