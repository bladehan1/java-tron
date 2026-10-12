package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.consensus.dpos.DposSlot;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.consensus.ConsensusService;
import org.tron.core.db.Manager;
import org.tron.core.db.ManagerForTest;
import org.tron.core.db.TronDatabase;
import org.tron.core.db2.ISession;
import org.tron.core.db2.common.Value;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.CheckPointV2Store;
import org.tron.core.store.CheckTmpStore;
import org.tron.protos.Protocol.Account;

/**
 * WP-07-1 (D3, 07-B, 07-C): the P66 AccountAsset lane is gated behind the archive feature
 * switches. Form OFF must register exactly the upstream participants, commit block sessions with
 * the upstream {@code commit()}, keep account-asset out of the legacy checkpoint, and migrate
 * Account maps at flush through upstream {@code processAccount}. A legacy checkpoint that already
 * contains account-asset redo (written by the unconditional-lane PR head) is replayed once with
 * the recovery-only lane, then every legacy redo is retired and the lane removed.
 */
public class P66LaneGateTest {
  private static final byte[] ADDRESS = address(31);
  private static final String TOKEN = "1000001";
  private static final String TOKEN_2 = "1000002";
  private static final String TOKEN_OLD = "1000003";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TronApplicationContext context;

  @After
  public void closeContext() {
    close();
  }

  @Test
  public void offFormRegistersUpstreamParticipantsAndMigratesAtFlush() throws Exception {
    File directory = temporaryFolder.newFolder();
    open(directory, 1);
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    ChainBaseManager chain = context.getBean(ChainBaseManager.class);
    AccountAssetStore assets = chain.getAccountAssetStore();

    assertFalse(snapshots.isP66SnapshotLaneInstalled());
    assertFalse(assets.isSnapshotsAttached());
    assertEquals(0, snapshots.getDbs().stream()
        .filter(db -> "account-asset".equals(db.getDbName())).count());
    assertFalse(accountRoot(snapshots).isCoupledMutationsMaterialized());

    enableAssetOptimizationAtRoot(snapshots, chain);
    try (ISession block = snapshots.buildSession()) {
      accounts(snapshots).put(ADDRESS, account(false, TOKEN, 100L));
      // Session writes to account-asset go straight to the native DB, as upstream.
      assets.put(row(ADDRESS, TOKEN_2), Longs.toByteArray(5));
      block.commit();
    }
    assertArrayEquals(Longs.toByteArray(5), assets.getDbSource().getData(row(ADDRESS, TOKEN_2)));
    // Block layer still carries the upstream Account with its map; no row written yet.
    assertNull(assets.getDbSource().getData(row(ADDRESS, TOKEN)));
    assertFalse(Account.parseFrom(accounts(snapshots).getUnchecked(ADDRESS)).getAssetOptimized());
    flushAll(snapshots);

    // Upstream processAccount at flush: prefix delete (un-optimized), map rows, cleared maps.
    assertArrayEquals(Longs.toByteArray(100), assets.getDbSource().getData(row(ADDRESS, TOKEN)));
    assertNull(assets.getDbSource().getData(row(ADDRESS, TOKEN_2)));
    Account flushed = Account.parseFrom(accountRoot(snapshots).get(ADDRESS));
    assertTrue(flushed.getAssetOptimized());
    assertTrue(flushed.getAssetV2Map().isEmpty());
    assertTrue("legacy checkpoint must carry account but never account-asset",
        checkpointContains(context.getBean(CheckTmpStore.class), "account"));
    assertFalse(checkpointContains(context.getBean(CheckTmpStore.class), "account-asset"));
  }

  @Test
  public void offFormPushBlockUsesUpstreamCommit() throws Exception {
    File directory = temporaryFolder.newFolder();
    open(directory, 1);
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    Manager manager = context.getBean(Manager.class);
    context.getBean(ConsensusService.class).start();
    ManagerForTest blocks = new ManagerForTest(manager, context.getBean(DposSlot.class));
    ChainBaseManager chain = context.getBean(ChainBaseManager.class);
    for (int i = 0; i < 3; i++) {
      long time = chain.getDynamicPropertiesStore().getLatestBlockHeaderTimestamp() + 3000L;
      long number = chain.getDynamicPropertiesStore().getLatestBlockHeaderNumber() + 1;
      ByteString parent = chain.getDynamicPropertiesStore().getLatestBlockHeaderHash()
          .getByteString();
      chain.getWitnessScheduleStore().saveActiveWitnesses(new java.util.ArrayList<>());
      manager.pushBlock(blocks.createTestBlockCapsule(time, number, parent));
    }
    assertTrue(snapshots.size() > 0);
    for (Chainbase db : snapshots.getDbs()) {
      Snapshot layer = db.getHead();
      while (Snapshot.isImpl(layer)) {
        assertNull("form OFF must not bind block metadata: " + db.getDbName(),
            ((SnapshotImpl) layer).getBlockSnapshotMeta());
        layer = layer.getPrevious();
      }
    }
    assertFalse(snapshots.isP66SnapshotLaneInstalled());
  }

  @Test
  public void recoveryOnlyReplaysV1AccountAssetRedoRetiresItAndUninstallsLane()
      throws Exception {
    File directory = temporaryFolder.newFolder();
    open(directory, 1);
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    ChainBaseManager chain = context.getBean(ChainBaseManager.class);
    enableAssetOptimizationAtRoot(snapshots, chain);
    chain.getAccountAssetStore().getDbSource().putData(row(ADDRESS, TOKEN_2),
        Longs.toByteArray(5));
    writeLaneCheckpoint(context.getBean(CheckTmpStore.class), TOKEN);
    close();

    open(directory, 1);
    assertRecoveredAndRetired(TOKEN);
    close();

    // A crash before retirement leaves the same redo behind: the replay is idempotent.
    open(directory, 1);
    writeLaneCheckpoint(context.getBean(CheckTmpStore.class), TOKEN);
    close();
    open(directory, 1);
    assertRecoveredAndRetired(TOKEN);
    close();

    // Next start is the plain upstream path and replays nothing.
    open(directory, 1);
    assertFalse(context.getBean(SnapshotManager.class).isP66SnapshotLaneInstalled());
    assertArrayEquals(Longs.toByteArray(100),
        context.getBean(ChainBaseManager.class).getAccountAssetStore().getDbSource()
            .getData(row(ADDRESS, TOKEN)));
  }

  @Test
  public void recoveryOnlyRetiresEveryV2DirectoryIncludingOnesOutsideReplayWindow()
      throws Exception {
    File directory = temporaryFolder.newFolder();
    open(directory, 2);
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    ChainBaseManager chain = context.getBean(ChainBaseManager.class);
    enableAssetOptimizationAtRoot(snapshots, chain);
    chain.getAccountAssetStore().getDbSource().putData(row(ADDRESS, TOKEN_2),
        Longs.toByteArray(5));
    long now = System.currentTimeMillis() + 5_000L;
    // Older than the checkV2 two-minute window: never replayed, but must still be retired so a
    // later upstream start cannot replay it onto a newer root.
    writeLaneCheckpoint(v2Store(now - 10 * 60_000L), TOKEN_OLD);
    writeLaneCheckpoint(v2Store(now - 30_000L), TOKEN);
    writeLaneCheckpoint(v2Store(now), TOKEN);
    assertTrue(snapshots.getCheckpointList().size() >= 3);
    close();

    open(directory, 2);
    assertRecoveredAndRetired(TOKEN);
    List<String> remaining = context.getBean(SnapshotManager.class).getCheckpointList();
    assertTrue(remaining == null || remaining.isEmpty());
    assertNull(context.getBean(ChainBaseManager.class).getAccountAssetStore().getDbSource()
        .getData(row(ADDRESS, TOKEN_OLD)));
  }

  private void assertRecoveredAndRetired(String token) throws Exception {
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    ChainBaseManager chain = context.getBean(ChainBaseManager.class);
    AccountAssetStore assets = chain.getAccountAssetStore();
    assertFalse(snapshots.isP66SnapshotLaneInstalled());
    assertFalse(assets.isSnapshotsAttached());
    assertEquals(0, snapshots.getDbs().stream()
        .filter(db -> "account-asset".equals(db.getDbName())).count());
    assertFalse(accountRoot(snapshots).isCoupledMutationsMaterialized());
    assertArrayEquals(Longs.toByteArray(100), assets.getDbSource().getData(row(ADDRESS, token)));
    assertNull("the redo's DELETE must reach the root",
        assets.getDbSource().getData(row(ADDRESS, TOKEN_2)));
    Account account = Account.parseFrom(accountRoot(snapshots).get(ADDRESS));
    assertTrue(account.getAssetOptimized());
    assertTrue(account.getAssetV2Map().isEmpty());
    assertFalse(context.getBean(CheckTmpStore.class).getDbSource().iterator().hasNext());
  }

  /** The shape PR head b76ddef2e4 wrote without Common: stripped Account plus lane rows. */
  private static void writeLaneCheckpoint(TronDatabase<byte[]> store, String token) {
    Map<byte[], byte[]> batch = new HashMap<>();
    batch.put(Bytes.concat(encode("account"), ADDRESS), Value.of(Value.Operator.PUT,
        account(true)).encode());
    batch.put(Bytes.concat(encode("account-asset"), row(ADDRESS, token)),
        Value.of(Value.Operator.PUT, Longs.toByteArray(100)).encode());
    batch.put(Bytes.concat(encode("account-asset"), row(ADDRESS, TOKEN_2)),
        Value.of(Value.Operator.DELETE, null).encode());
    try {
      store.getDbSource().updateByBatch(batch);
    } finally {
      if (store instanceof CheckPointV2Store) {
        store.close();
      }
    }
  }

  private static CheckPointV2Store v2Store(long timestamp) {
    return new CheckPointV2Store("checkpoint/" + timestamp);
  }

  private static boolean checkpointContains(TronDatabase<byte[]> store, String dbName) {
    byte[] prefix = encode(dbName);
    for (Map.Entry<byte[], byte[]> entry : store.getDbSource()) {
      byte[] key = entry.getKey();
      if (key.length > prefix.length && SnapshotManager.simpleDecode(key).equals(dbName)) {
        return true;
      }
    }
    return false;
  }

  private static void enableAssetOptimizationAtRoot(SnapshotManager snapshots,
      ChainBaseManager chain) {
    try (ISession block = snapshots.buildSession()) {
      chain.getDynamicPropertiesStore().setAllowAssetOptimization(1);
      block.commit();
    }
    flushAll(snapshots);
    assertEquals(1, chain.getDynamicPropertiesStore().getAllowAccountAssetOptimizationFromRoot());
  }

  /** Forces every committed layer into the roots through the upstream flush path. */
  private static void flushAll(SnapshotManager snapshots) {
    int maxSize = snapshots.getMaxSize();
    snapshots.setMaxSize(0);
    try (ISession ignored = snapshots.buildSession()) {
      assertEquals(1, snapshots.size());
    } finally {
      snapshots.setMaxSize(maxSize);
    }
    assertEquals(0, snapshots.size());
  }

  private static Chainbase accounts(SnapshotManager snapshots) {
    return snapshots.getDbs().stream()
        .filter(db -> "account".equals(db.getDbName())).findFirst().get();
  }

  private static SnapshotRoot accountRoot(SnapshotManager snapshots) {
    return (SnapshotRoot) accounts(snapshots).getHead().getRoot();
  }

  private void open(File directory, int checkpointVersion) {
    Args.setParam(new String[]{"--output-directory", directory.toString()},
        TestConstants.TEST_CONF);
    Args.getInstance().getStorage().setCheckpointVersion(checkpointVersion);
    context = new TronApplicationContext(DefaultConfig.class);
    ApplicationFactory.create(context);
  }

  private void close() {
    if (context != null) {
      context.close();
      context = null;
      Args.clearParam();
    }
  }

  private static byte[] encode(String name) {
    byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
    return Bytes.concat(Ints.toByteArray(bytes.length), bytes);
  }

  private static byte[] account(boolean optimized, Object... assetPairs) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(ADDRESS))
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
}
