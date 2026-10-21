package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.config.args.Args;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.common.RocksDB;
import org.tron.protos.Protocol.Account;

/**
 * Table-driven check of design 07 §3.9.4 (07-D): the block-final P66 materializer must apply
 * upstream {@code SnapshotRoot.processAccount} semantics to each block layer, including
 * {@code getDeletedAssets} for non-optimized PUTs (review R4-07).
 *
 * <p>Uses RocksDB so it runs on arm64 (the LevelDB-based P66 suites are skipped there).
 */
public class P66MaterializerUpstreamRulesTest {
  private static final byte[] FLAG = "ALLOW_ASSET_OPTIMIZATION"
      .getBytes(StandardCharsets.US_ASCII);
  private static final String TOKEN = "1000001";
  private static final String TOKEN_2 = "1000002";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SnapshotManager manager;
  private Chainbase accounts;
  private Chainbase assets;
  private Chainbase properties;
  private int blockNumber;

  @BeforeClass
  public static void configure() {
    Args.setParam(new String[]{"--output-directory", "output_p66_materializer_rules"},
        "config-test.conf");
  }

  @AfterClass
  public static void clear() {
    Args.clearParam();
  }

  @Before
  public void open() throws Exception {
    Path path = temporaryFolder.newFolder().toPath();
    manager = new SnapshotManager("");
    accounts = database(path, "account");
    assets = database(path, "account-asset");
    properties = database(path, "properties");
    manager.add(accounts);
    manager.add(properties);
    manager.installP66SnapshotLane(assets);
    manager.enable();
    blockNumber = 0;
  }

  @After
  public void close() {
    manager.shutdown();
    accounts.close();
    assets.close();
    properties.close();
  }

  /** The R4-07 component repro (review scratchpad R4VerifyP66DeleteRecreateTest), as a guard. */
  @Test
  public void sameLayerDeleteThenRecreateDropsOldAssetRows() throws Exception {
    byte[] address = address(7);
    setFlag(1);
    block(() -> accounts.put(address, account(address, false, TOKEN, 100L)));
    assertArrayEquals(Longs.toByteArray(100), assets.getUnchecked(row(address, TOKEN)));
    assertTrue(Account.parseFrom(accounts.getUnchecked(address)).getAssetOptimized());

    block(() -> {
      accounts.delete(address);
      accounts.put(address, Account.newBuilder().setAddress(ByteString.copyFrom(address))
          .setBalance(1L).build().toByteArray());
    });
    // Upstream: the final non-optimized PUT prefix-deletes every row before migrating.
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    Account recreated = Account.parseFrom(accounts.getUnchecked(address));
    assertTrue(recreated.getAssetOptimized());
    assertTrue(recreated.getAssetV2Map().isEmpty());
    assertEquals(1L, recreated.getBalance());
  }

  @Test
  public void flagZeroIsANoOp() throws Exception {
    byte[] address = address(1);
    setFlag(0);
    byte[] original = account(address, false, TOKEN, 5L);
    P66CoupledMutationMaterializer.Statistics stats =
        block(() -> accounts.put(address, original));
    assertArrayEquals(original, accounts.getUnchecked(address));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertEquals(0, stats.changedAccounts);
    assertEquals(0, stats.prefixQueries);
  }

  @Test
  public void deleteRemovesEveryVisibleRow() throws Exception {
    byte[] address = address(2);
    setFlag(1);
    block(() -> accounts.put(address, account(address, false, TOKEN, 3L, TOKEN_2, 4L)));
    P66CoupledMutationMaterializer.Statistics stats = block(() -> accounts.delete(address));
    assertNull(accounts.getUnchecked(address));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertNull(assets.getUnchecked(row(address, TOKEN_2)));
    assertEquals(1, stats.prefixQueries);
    assertEquals(2, stats.prefixRows);
    assertEquals(2, stats.assetDeletes);
  }

  @Test
  public void emptyPutKeepsEmptyAccountAndDeletesRows() throws Exception {
    byte[] address = address(3);
    setFlag(1);
    block(() -> accounts.put(address, account(address, true, TOKEN, 9L)));
    assertNotNull(assets.getUnchecked(row(address, TOKEN)));
    P66CoupledMutationMaterializer.Statistics stats =
        block(() -> accounts.put(address, new byte[0]));
    byte[] value = accounts.getUnchecked(address);
    assertNotNull("empty PUT must stay a PUT (has() semantics)", value);
    assertEquals(0, value.length);
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertEquals(1, stats.prefixQueries);
  }

  @Test
  public void nonOptimizedPutPrefixDeletesThenWritesNonZeroAndDeletesZero() throws Exception {
    byte[] address = address(4);
    setFlag(1);
    // A residual row under an un-optimized Account (violates the upstream root invariant, but
    // upstream still clears it on migration).
    block(() -> assets.put(row(address, TOKEN_2), Longs.toByteArray(77)));
    P66CoupledMutationMaterializer.Statistics stats = block(() ->
        accounts.put(address, account(address, false, TOKEN, 0L, "1000003", 8L)));
    assertNull(assets.getUnchecked(row(address, TOKEN_2)));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertArrayEquals(Longs.toByteArray(8), assets.getUnchecked(row(address, "1000003")));
    Account stored = Account.parseFrom(accounts.getUnchecked(address));
    assertTrue(stored.getAssetOptimized());
    assertTrue(stored.getAssetV2Map().isEmpty());
    assertTrue(stored.getAssetMap().isEmpty());
    assertEquals(1, stats.prefixQueries);
    assertEquals(1, stats.migratedAccounts);
  }

  @Test
  public void unoptimizedEmptyMapStillQueriesPrefixAndOptimizedEmptyMapDoesNot()
      throws Exception {
    byte[] fresh = address(5);
    byte[] optimized = address(6);
    setFlag(1);
    P66CoupledMutationMaterializer.Statistics first = block(() ->
        accounts.put(fresh, Account.newBuilder().setAddress(ByteString.copyFrom(fresh))
            .build().toByteArray()));
    assertEquals(1, first.prefixQueries);
    assertEquals(0, first.assetDeletes);
    P66CoupledMutationMaterializer.Statistics second = block(() ->
        accounts.put(optimized, Account.newBuilder().setAddress(ByteString.copyFrom(optimized))
            .setAssetOptimized(true).build().toByteArray()));
    assertEquals(0, second.prefixQueries);
  }

  @Test
  public void optimizedPutWritesNonZeroAndDeletesZero() throws Exception {
    byte[] address = address(8);
    setFlag(1);
    block(() -> accounts.put(address, account(address, true, TOKEN, 10L, TOKEN_2, 11L)));
    block(() -> accounts.put(address, account(address, true, TOKEN, 0L, TOKEN_2, 12L)));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertArrayEquals(Longs.toByteArray(12), assets.getUnchecked(row(address, TOKEN_2)));
  }

  @Test
  public void crossBlockDeleteThenRecreateStartsFromZero() throws Exception {
    byte[] address = address(9);
    setFlag(1);
    block(() -> accounts.put(address, account(address, false, TOKEN, 50L)));
    block(() -> accounts.delete(address));
    block(() -> accounts.put(address, account(address, false, TOKEN_2, 1L)));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
    assertArrayEquals(Longs.toByteArray(1), assets.getUnchecked(row(address, TOKEN_2)));
  }

  @Test
  public void revokedNestedSessionLeavesNoMaterializedRows() throws Exception {
    byte[] address = address(10);
    setFlag(1);
    block(() -> {
      try (ISession nested = manager.buildSession()) {
        accounts.put(address, account(address, false, TOKEN, 5L));
        nested.revoke();
      }
    });
    assertNull(accounts.getUnchecked(address));
    assertNull(assets.getUnchecked(row(address, TOKEN)));
  }

  @Test
  public void malformedKeyAndAddressMismatchAreRejected() {
    setFlag(1);
    expectMaterializeFailure(() -> accounts.put(new byte[20], new byte[]{1}));
    expectMaterializeFailure(() -> accounts.put(address(11), account(address(12), true)));
  }

  private void expectMaterializeFailure(Runnable writes) {
    try (ISession session = manager.buildSession()) {
      writes.run();
      try {
        P66CoupledMutationMaterializer materializer =
            new P66CoupledMutationMaterializer(accounts, assets, properties);
        materializer.materialize();
        fail("materializer accepted a corrupt Account key");
      } catch (IllegalStateException expected) {
        assertFalse(expected.getMessage().isEmpty());
      }
    }
  }

  private interface Writes {
    void run() throws Exception;
  }

  private P66CoupledMutationMaterializer.Statistics block(Writes writes) throws Exception {
    P66CoupledMutationMaterializer.Statistics[] stats =
        new P66CoupledMutationMaterializer.Statistics[1];
    int number = ++blockNumber;
    try (ISession block = manager.buildSession()) {
      writes.run();
      block.finalizeBlock(meta(number), stages -> {
        // Measure the materializer on a twin instance before the real stage runs it; the plan
        // is idempotent on the same layer, so statistics of the first run are what we need.
        stats[0] = new P66CoupledMutationMaterializer(accounts, assets, properties)
            .materialize();
        stages.normalizeSnapshot();
        stages.startBlockDiff();
        stages.buildPathState();
        stages.completeArtifacts();
        stages.commitSession();
      });
    }
    return stats[0];
  }

  private void setFlag(long value) {
    properties.getHead().put(FLAG, Longs.toByteArray(value));
  }

  private static byte[] account(byte[] address, boolean optimized, Object... assetPairs) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setAssetOptimized(optimized);
    for (int i = 0; i < assetPairs.length; i += 2) {
      builder.putAssetV2((String) assetPairs[i], (Long) assetPairs[i + 1]);
    }
    return builder.build().toByteArray();
  }

  private static byte[] address(int seed) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) seed;
    return address;
  }

  private static byte[] row(byte[] address, String token) {
    return Bytes.concat(address, token.getBytes(StandardCharsets.UTF_8));
  }

  private static Chainbase database(Path path, String name) {
    return new Chainbase(new SnapshotRoot(new RocksDB(
        new RocksDbDataSourceImpl(path.toString(), name))));
  }

  private static BlockSnapshotMeta meta(int number) {
    byte[] hash = new byte[32];
    hash[31] = (byte) number;
    byte[] parent = new byte[32];
    parent[31] = (byte) (number - 1);
    return BlockSnapshotMeta.forBlock(number, hash, parent, number * 3000L);
  }
}
