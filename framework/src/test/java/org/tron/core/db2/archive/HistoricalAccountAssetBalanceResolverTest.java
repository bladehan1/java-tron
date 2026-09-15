package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Test;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.HistoricalAccountAssetBalanceResolver.Result;
import org.tron.core.db2.archive.HistoricalAccountAssetPrefixResolver.Balance;
import org.tron.core.db2.archive.HistoricalAccountAssetPrefixResolver.Limits;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;

public class HistoricalAccountAssetBalanceResolverTest {

  private static final String TOKEN_ID = "1000001";

  @Test
  public void resolvesP66OffBalanceOnlyFromHistoricalAccountMap() throws Exception {
    byte[] address = address(1);
    Fixture fixture = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(0L))
        .put("account", address, account(address, false, TOKEN_ID, 17L));

    try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
      Result result = new HistoricalAccountAssetBalanceResolver()
          .resolve(snapshot, address, TOKEN_ID);
      assertTrue(result.isAccountPresent());
      assertEquals(17L, result.getBalance());
      assertEquals(Phase.P66_OFF, result.getPhase());
      assertEquals(7L, result.getBlockNumber());
      assertEquals(Arrays.asList("properties", "account", "account-asset"), fixture.reads);
    }
  }

  @Test
  public void resolvesP66OnBalanceOnlyFromExactDirectRow() throws Exception {
    byte[] address = address(2);
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, TOKEN_ID);
    Fixture fixture = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(1L))
        .put("account", address, account(address, true, null, 0L))
        .put("account-asset", directKey, ByteArray.fromLong(29L));

    try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
      Result result = new HistoricalAccountAssetBalanceResolver()
          .resolve(snapshot, address, TOKEN_ID);
      assertTrue(result.isAccountPresent());
      assertEquals(29L, result.getBalance());
      assertEquals(Phase.P66_ON, result.getPhase());
    }
  }

  @Test
  public void preservesSemanticAbsenceForMissingAccountAndZeroBalance() throws Exception {
    byte[] existing = address(3);
    byte[] missing = address(4);
    Fixture fixture = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(1L))
        .put("account", existing, account(existing, true, null, 0L));

    try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
      HistoricalAccountAssetBalanceResolver resolver =
          new HistoricalAccountAssetBalanceResolver();
      Result zero = resolver.resolve(snapshot, existing, TOKEN_ID);
      assertTrue(zero.isAccountPresent());
      assertEquals(0L, zero.getBalance());

      Result absent = resolver.resolve(snapshot, missing, TOKEN_ID);
      assertFalse(absent.isAccountPresent());
      assertThrows(IllegalStateException.class, absent::getBalance);
    }
  }

  @Test
  public void rejectsMissingOrMalformedHistoricalProposal66Property() throws Exception {
    byte[] address = address(5);
    Fixture missing = new Fixture().put("account", address,
        account(address, false, TOKEN_ID, 1L));
    try (ArchiveReadSnapshot snapshot = missing.snapshot()) {
      assertThrows(ArchivePersistenceException.class,
          () -> new HistoricalAccountAssetBalanceResolver()
              .resolve(snapshot, address, TOKEN_ID));
    }

    for (byte[] invalid : Arrays.asList(new byte[]{1}, ByteArray.fromLong(2L))) {
      Fixture malformed = new Fixture()
          .put("properties", proposal66Key(), invalid)
          .put("account", address, account(address, false, TOKEN_ID, 1L));
      try (ArchiveReadSnapshot snapshot = malformed.snapshot()) {
        assertThrows(ArchivePersistenceException.class,
            () -> new HistoricalAccountAssetBalanceResolver()
                .resolve(snapshot, address, TOKEN_ID));
      }
    }
  }

  @Test
  public void rejectsMixedAndOrphanPhysicalLayouts() throws Exception {
    byte[] address = address(6);
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, TOKEN_ID);
    List<Fixture> invalid = Arrays.asList(
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(0L))
            .put("account", address, account(address, false, TOKEN_ID, 3L))
            .put("account-asset", directKey, ByteArray.fromLong(3L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address, false, TOKEN_ID, 3L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account-asset", directKey, ByteArray.fromLong(3L)));

    for (Fixture fixture : invalid) {
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(ArchivePersistenceException.class,
            () -> new HistoricalAccountAssetBalanceResolver()
                .resolve(snapshot, address, TOKEN_ID));
      }
    }
  }

  @Test
  public void rejectsCorruptWrongKeyAndNonCanonicalDirectValues() throws Exception {
    byte[] address = address(7);
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, TOKEN_ID);
    List<Fixture> invalid = Arrays.asList(
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, new byte[]{1, 2, 3}),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address(8), true, null, 0L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address, true, null, 0L))
            .put("account-asset", directKey, new byte[]{1}),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address, true, null, 0L))
            .put("account-asset", directKey, ByteArray.fromLong(0L)));

    for (Fixture fixture : invalid) {
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(ArchivePersistenceException.class,
            () -> new HistoricalAccountAssetBalanceResolver()
                .resolve(snapshot, address, TOKEN_ID));
      }
    }
  }

  @Test
  public void rejectsUnsupportedTokenIdentitiesBeforeReadingStores() throws Exception {
    byte[] address = address(9);
    for (String tokenId : Arrays.asList("", "01", "asset-name", "１２")) {
      Fixture fixture = new Fixture();
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(ArchivePersistenceException.class,
            () -> new HistoricalAccountAssetBalanceResolver()
                .resolve(snapshot, address, tokenId));
        assertTrue(fixture.reads.isEmpty());
      }
    }
  }

  @Test
  public void failsClosedWhenPinnedGenerationIdentityDriftsDuringResolution() throws Exception {
    byte[] address = address(10);
    Fixture fixture = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(1L))
        .put("account", address, account(address, true, null, 0L));
    fixture.driftAfterThirdRead = true;

    try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
      assertThrows(IllegalArgumentException.class,
          () -> new HistoricalAccountAssetBalanceResolver()
              .resolve(snapshot, address, TOKEN_ID));
    }
  }

  @Test
  public void prefixResolvesP66OffEmbeddedMapInCanonicalTokenOrder() throws Exception {
    byte[] address = address(11);
    Account account = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).putAssetV2("1000002", 22L)
        .putAssetV2("1000001", 11L).build();
    Fixture fixture = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(0L))
        .put("account", address, account.toByteArray());

    try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
      HistoricalAccountAssetPrefixResolver.Result result =
          new HistoricalAccountAssetPrefixResolver().resolve(snapshot, address, limits());
      assertEquals(Phase.P66_OFF, result.getPhase());
      assertTrue(result.isAccountPresent());
      assertBalances(result.getBalances(), "1000001", 11L, "1000002", 22L);
    }
  }

  @Test
  public void prefixResolvesP66OnExactRowsAndRejectsMixedOrInvalidRows() throws Exception {
    byte[] address = address(12);
    P66AccountAssetCodec codec = new P66AccountAssetCodec();
    byte[] firstKey = codec.assetPhysicalKey(address, "1000001");
    byte[] secondKey = codec.assetPhysicalKey(address, "1000002");
    Fixture valid = new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(1L))
        .put("account", address, account(address, true, null, 0L))
        .put("account-asset", secondKey, ByteArray.fromLong(22L))
        .put("account-asset", firstKey, ByteArray.fromLong(11L));
    try (ArchiveReadSnapshot snapshot = valid.snapshot()) {
      HistoricalAccountAssetPrefixResolver.Result result =
          new HistoricalAccountAssetPrefixResolver().resolve(snapshot, address, limits());
      assertEquals(Phase.P66_ON, result.getPhase());
      assertBalances(result.getBalances(), "1000001", 11L, "1000002", 22L);
    }

    List<Fixture> invalid = Arrays.asList(
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(0L))
            .put("account", address, account(address, false, TOKEN_ID, 1L))
            .put("account-asset", firstKey, ByteArray.fromLong(1L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account-asset", firstKey, ByteArray.fromLong(1L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address, true, null, 0L))
            .put("account-asset", concat(address, "01"), ByteArray.fromLong(1L)),
        new Fixture()
            .put("properties", proposal66Key(), ByteArray.fromLong(1L))
            .put("account", address, account(address, true, null, 0L))
            .put("account-asset", firstKey, ByteArray.fromLong(0L)));
    for (Fixture fixture : invalid) {
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(ArchivePersistenceException.class,
            () -> new HistoricalAccountAssetPrefixResolver()
                .resolve(snapshot, address, limits()));
      }
    }
  }

  @Test
  public void prefixRejectsEveryOutputBudgetWithoutReturningPartialResults() throws Exception {
    byte[] address = address(13);
    P66AccountAssetCodec codec = new P66AccountAssetCodec();
    byte[] firstKey = codec.assetPhysicalKey(address, "1000001");
    byte[] secondKey = codec.assetPhysicalKey(address, "1000002");
    List<Limits> rejected = Arrays.asList(
        new Limits(10, 10, 1, 64, 8, 1_000),
        new Limits(10, 10, 10, firstKey.length - 1, 8, 1_000),
        new Limits(10, 10, 10, 64, 7, 1_000),
        new Limits(10, 10, 10, 64, 8,
            (long) firstKey.length + Long.BYTES + secondKey.length + Long.BYTES - 1));
    for (Limits limits : rejected) {
      Fixture fixture = new Fixture()
          .put("properties", proposal66Key(), ByteArray.fromLong(1L))
          .put("account", address, account(address, true, null, 0L))
          .put("account-asset", firstKey, ByteArray.fromLong(11L))
          .put("account-asset", secondKey, ByteArray.fromLong(22L));
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(ArchiveQueryLimitExceededException.class,
            () -> new HistoricalAccountAssetPrefixResolver()
                .resolve(snapshot, address, limits));
      }
    }
    assertThrows(IllegalArgumentException.class,
        () -> new Limits(0, 1, 1, 1, 1, 1));
  }

  @Test
  public void prefixRejectsForeignUnsortedDuplicateAndGenerationDrift() throws Exception {
    byte[] address = address(14);
    byte[] firstKey = new P66AccountAssetCodec().assetPhysicalKey(address, "1000001");
    byte[] secondKey = new P66AccountAssetCodec().assetPhysicalKey(address, "1000002");
    Fixture foreign = prefixFixture(address, firstKey, secondKey);
    foreign.put("account-asset",
        new P66AccountAssetCodec().assetPhysicalKey(address(99), "1000003"),
        ByteArray.fromLong(33L));
    foreign.foreignRange = true;
    Fixture unsorted = prefixFixture(address, firstKey, secondKey);
    unsorted.reverseRange = true;
    Fixture duplicate = prefixFixture(address, firstKey, secondKey);
    duplicate.duplicateRange = true;
    for (Fixture fixture : Arrays.asList(foreign, unsorted, duplicate)) {
      try (ArchiveReadSnapshot snapshot = fixture.snapshot()) {
        assertThrows(IllegalArgumentException.class,
            () -> new HistoricalAccountAssetPrefixResolver()
                .resolve(snapshot, address, limits()));
      }
    }

    Fixture drift = prefixFixture(address, firstKey, secondKey);
    drift.driftAfterRange = true;
    try (ArchiveReadSnapshot snapshot = drift.snapshot()) {
      assertThrows(IllegalArgumentException.class,
          () -> new HistoricalAccountAssetPrefixResolver()
              .resolve(snapshot, address, limits()));
    }
  }

  private static byte[] proposal66Key() {
    return HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey();
  }

  private static Fixture prefixFixture(byte[] address, byte[] firstKey, byte[] secondKey) {
    return new Fixture()
        .put("properties", proposal66Key(), ByteArray.fromLong(1L))
        .put("account", address, account(address, true, null, 0L))
        .put("account-asset", firstKey, ByteArray.fromLong(11L))
        .put("account-asset", secondKey, ByteArray.fromLong(22L));
  }

  private static Limits limits() {
    return new Limits(10, 10, 10, 64, 8, 1_000);
  }

  private static void assertBalances(List<Balance> balances, String firstToken,
      long firstBalance, String secondToken, long secondBalance) {
    assertEquals(2, balances.size());
    assertEquals(firstToken, balances.get(0).getTokenId());
    assertEquals(firstBalance, balances.get(0).getBalance());
    assertEquals(secondToken, balances.get(1).getTokenId());
    assertEquals(secondBalance, balances.get(1).getBalance());
  }

  private static byte[] concat(byte[] address, String suffix) {
    byte[] token = suffix.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    byte[] key = Arrays.copyOf(address, address.length + token.length);
    System.arraycopy(token, 0, key, address.length, token.length);
    return key;
  }

  private static byte[] account(byte[] address, boolean optimized, String tokenId, long balance) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(optimized);
    if (tokenId != null) {
      builder.putAsset("asset-name", balance).putAssetV2(tokenId, balance);
    }
    return builder.build().toByteArray();
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class Fixture {
    private final List<Value> values = new ArrayList<>();
    private final List<String> reads = new ArrayList<>();
    private final MutableServing serving = new MutableServing();
    private boolean driftAfterThirdRead;
    private boolean driftAfterRange;
    private boolean foreignRange;
    private boolean reverseRange;
    private boolean duplicateRange;

    private Fixture put(String dbName, byte[] key, byte[] value) {
      values.add(new Value(dbName, key, value));
      return this;
    }

    private ArchiveReadSnapshot snapshot() throws Exception {
      ArchiveReadSnapshot.PinnedLatestState latest = new ArchiveReadSnapshot.PinnedLatestState() {
        @Override
        public long getBlockNumber() {
          return 7L;
        }

        @Override
        public byte[] getBlockHash() {
          return hash(7);
        }

        @Override
        public OldValue get(String dbName, byte[] rawKey) {
          reads.add(dbName);
          if (driftAfterThirdRead && reads.size() == 3) {
            serving.drifted = true;
          }
          for (Value value : values) {
            if (value.dbName.equals(dbName) && Arrays.equals(value.key, rawKey)) {
              return OldValue.present(value.value);
            }
          }
          return OldValue.absent();
        }

        @Override
        public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lower,
            byte[] upper, int maxEntries) {
          List<HistoricalRangeOverlay.Entry> result = new ArrayList<>();
          for (Value value : values) {
            if (value.dbName.equals(dbName)
                && (foreignRange || inRange(value.key, lower, upper))) {
              result.add(new HistoricalRangeOverlay.Entry(value.key, value.value));
            }
          }
          result.sort((left, right) -> BlockReverseDiff.compareUnsigned(
              left.getKey(), right.getKey()));
          if (reverseRange) {
            Collections.reverse(result);
          }
          if (duplicateRange && !result.isEmpty()) {
            result.add(result.get(result.size() - 1));
          }
          if (driftAfterRange) {
            serving.drifted = true;
          }
          return result;
        }

        @Override
        public void close() {
        }
      };
      ArchiveReadSnapshot.PinnedHistory history = new ArchiveReadSnapshot.PinnedHistory() {
        @Override
        public long getIndexedFrom() {
          return 0L;
        }

        @Override
        public long getIndexedThrough() {
          return 7L;
        }

        @Override
        public byte[] getHeadHash() {
          return hash(7);
        }

        @Override
        public byte[] getAuthoritativePrefixDigest() {
          return hash(11);
        }

        @Override
        public OldValue read(String dbName, byte[] rawKey, long firstChangeBlock) {
          throw new AssertionError("Fixture must not read unconfigured history");
        }

        @Override
        public void close() {
        }
      };
      return ArchiveReadSnapshot.pin(7L, 7L, hash(7), serving, latest, history);
    }
  }

  private static boolean inRange(byte[] key, byte[] lower, byte[] upper) {
    return BlockReverseDiff.compareUnsigned(key, lower) >= 0
        && (upper == null || BlockReverseDiff.compareUnsigned(key, upper) < 0);
  }

  private static final class MutableServing implements ServingKeyIndex {
    private boolean drifted;

    @Override
    public String getGenerationId() {
      return "account-asset-resolver";
    }

    @Override
    public long getIndexedFrom() {
      return 0L;
    }

    @Override
    public long getIndexedThrough() {
      return 7L;
    }

    @Override
    public byte[] getHeadHash() {
      return drifted ? hash(8) : hash(7);
    }

    @Override
    public byte[] getAuthoritativePrefixDigest() {
      return hash(11);
    }

    @Override
    public OptionalLong firstChangeAfter(String dbName, byte[] rawKey, long targetBlock,
        long upperBound) {
      return OptionalLong.empty();
    }

    @Override
    public List<ServingKeyIndexGeneration.ChangedKey> changesInRange(String dbName,
        byte[] lowerInclusive, byte[] upperExclusive, long targetBlock, long upperBound,
        int maxChangedKeys) {
      return Collections.emptyList();
    }
  }

  private static final class Value {
    private final String dbName;
    private final byte[] key;
    private final byte[] value;

    private Value(String dbName, byte[] key, byte[] value) {
      this.dbName = dbName;
      this.key = Arrays.copyOf(key, key.length);
      this.value = Arrays.copyOf(value, value.length);
    }
  }
}
