package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.tron.core.db2.stateroot.PathStateParticipantDescriptor.StoreIdentity;

public class PathStateParticipantDescriptorTest {

  private static final List<String> EXACT_27 = Arrays.asList(
      "abi", "accountid-index", "account-index", "account", "account-asset",
      "asset-issue", "asset-issue-v2", "code", "contract-state", "contract",
      "DelegatedResourceAccountIndex", "DelegatedResource", "delegation", "properties",
      "exchange", "exchange-v2", "market_account", "market_order",
      "market_pair_price_to_order", "market_pair_to_price", "proposal", "storage-row",
      "votes", "witness_schedule", "witness", "nullifier", "IncrementalMerkleTree");

  @Test
  public void definesIndependentExact27IdentityGolden() {
    PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
    assertEquals("path-state-root/exact-27/v1", PathStateParticipantDescriptor.SCOPE_ID);
    assertEquals(27, descriptor.getStores().size());

    for (int index = 0; index < EXACT_27.size(); index++) {
      StoreIdentity identity = descriptor.getStores().get(index);
      assertEquals(index + 1, identity.getStoreId());
      assertEquals(EXACT_27.get(index), identity.getDbName());
      assertEquals(identity, descriptor.require(EXACT_27.get(index)));
    }
  }

  @Test
  public void assignsOnlyTheMarketPriceComparatorProfile() {
    PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
    for (StoreIdentity identity : descriptor.getStores()) {
      String expected = identity.getDbName().equals(
          PathStateParticipantDescriptor.MARKET_PRICE_DATABASE)
          ? PathStateParticipantDescriptor.MARKET_PRICE_COMPARATOR
          : PathStateParticipantDescriptor.UNSIGNED_RAW_COMPARATOR;
      assertEquals(expected, identity.getComparatorId());
    }
  }

  @Test
  public void requiresExactMembershipIndependentOfInputOrder() {
    PathStateParticipantDescriptor descriptor = PathStateParticipantDescriptor.current();
    List<String> reversed = new ArrayList<>(EXACT_27);
    Collections.reverse(reversed);
    descriptor.requireExactDatabases(reversed);

    List<String> missing = new ArrayList<>(EXACT_27);
    missing.remove("abi");
    assertThrows(IllegalArgumentException.class,
        () -> descriptor.requireExactDatabases(missing));

    List<String> unexpected = new ArrayList<>(EXACT_27);
    unexpected.set(0, "accountTrie");
    assertThrows(IllegalArgumentException.class,
        () -> descriptor.requireExactDatabases(unexpected));

    List<String> duplicate = new ArrayList<>(EXACT_27);
    duplicate.add("account");
    assertThrows(IllegalArgumentException.class,
        () -> descriptor.requireExactDatabases(duplicate));
    assertThrows(IllegalArgumentException.class,
        () -> descriptor.requireExactDatabases(Arrays.asList("abi", null)));
    assertThrows(IllegalArgumentException.class,
        () -> descriptor.require("unknown"));
  }

  @Test
  public void exposesAnImmutableOrderedDescriptor() {
    assertThrows(UnsupportedOperationException.class,
        () -> PathStateParticipantDescriptor.current().getStores().clear());
  }
}
