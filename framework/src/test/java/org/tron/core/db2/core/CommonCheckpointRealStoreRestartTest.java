package org.tron.core.db2.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.Longs;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.config.args.StorageConfig.StateArchiveAppendFileConfig;
import org.tron.core.db.Manager;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/**
 * End-to-end regression for the common-checkpoint write path against real stores: blocks carry
 * account, recent-transaction, trans-cache and block writes; checkpoints complete back to back;
 * a restart on the same data dir must boot cleanly. Covers ARM-001 Bug B (internal checkpoint
 * keys must never land in self-iterating business stores such as recent-transaction) and
 * exercises the multi-checkpoint and append-finalized paths around ARM-001 Bug A, in both the
 * legacy v3 and the append v5 archive modes.
 */
public class CommonCheckpointRealStoreRestartTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void legacyModeCheckpointsAcrossRealStoresThenRestartBootsCleanly() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path output = temporaryFolder.newFolder("real-store-legacy").toPath();
    runBlocksThenRestart(open(output, false), output, 5, false);
  }

  @Test
  public void appendModeCheckpointsAcrossRealStoresThenRestartBootsCleanly() throws Exception {
    TestConstants.assumeLevelDbAvailable();
    Path output = temporaryFolder.newFolder("real-store-append").toPath();
    runBlocksThenRestart(open(output, true), output, 5, true);
  }

  private void runBlocksThenRestart(TronApplicationContext context, Path output, int blocks,
      boolean appendMode) throws Exception {
    try {
      Manager manager = context.getBean(Manager.class);
      SnapshotManager snapshots = context.getBean(SnapshotManager.class);
      snapshots.setMaxFlushCount(1);
      for (int number = 1; number <= blocks; number++) {
        writeBlock(context, snapshots, number);
        // One deterministic checkpoint per block.
        setFlushCount(snapshots, 1);
        snapshots.flush();
        assertEquals(CommonCheckpointRuntimeAttachment.State.READY,
            manager.getCommonCheckpointRuntime().getState());
        assertEquals(number,
            manager.getPathStateSnapshotHead().getHead().getBlockNumber());
      }

      // No internal checkpoint key may leak into business stores: every value in
      // recent-transaction must remain the JSON we wrote (TxCacheDB parses them at startup).
      Chainbase recentTransactions = store(snapshots, "recent-transaction");
      java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> iterator =
          recentTransactions.iterator();
      int entries = 0;
      while (iterator.hasNext()) {
        java.util.Map.Entry<byte[], byte[]> entry = iterator.next();
        entries++;
        String value = new String(entry.getValue(), StandardCharsets.UTF_8);
        assertTrue("business value must stay valid JSON: " + value, value.startsWith("{"));
        assertTrue(Longs.fromByteArray(entry.getKey()) >= 1);
      }
      assertEquals(blocks, entries);
    } finally {
      context.close();
      Args.clearParam();
    }
    assertRestartBootsCleanly(output, blocks, appendMode);
  }

  private void writeBlock(TronApplicationContext context, SnapshotManager snapshots, int number)
      throws Exception {
    ChainBaseManager chainBase = context.getBean(ChainBaseManager.class);
    org.tron.common.utils.Sha256Hash parent = number == 1
        ? chainBase.getGenesisBlock().getBlockId() : lastBlockHash;
    BlockCapsule block = new BlockCapsule(number, parent, number * 3_000L,
        com.google.protobuf.ByteString.EMPTY);
    byte[] blockHash = block.getBlockId().getBytes();
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(number, blockHash,
        parent.getBytes(), number * 3_000L);
    try (ISession session = snapshots.buildSession()) {
      store(snapshots, "account").put(accountKey(number), accountValue(number));
      store(snapshots, "recent-transaction").put(Longs.toByteArray(number), jsonValue(number));
      store(snapshots, "trans-cache").put(Longs.toByteArray(number), Longs.toByteArray(number));
      chainBase.getBlockStore().put(blockHash, block);
      chainBase.getBlockIndexStore().put(block.getBlockId());
      session.commit(meta);
    }
    // Keep the canonical head tracking the committed block, like Manager.applyBlock does.
    chainBase.getDynamicPropertiesStore().saveLatestBlockHeaderNumber(number);
    chainBase.getDynamicPropertiesStore().saveLatestBlockHeaderHash(
        com.google.protobuf.ByteString.copyFrom(blockHash));
    chainBase.getDynamicPropertiesStore().saveLatestBlockHeaderTimestamp(number * 3_000L);
    lastBlockHash = block.getBlockId();
  }

  private org.tron.common.utils.Sha256Hash lastBlockHash;

  private void assertRestartBootsCleanly(Path output, int expectedHead, boolean appendMode)
      throws java.io.IOException {
    // Restart on the same data: Manager.init runs TxCacheDB.init, which iterates and parses
    // every recent-transaction entry. Before the Bug B fix this died with a JsonParseException
    // on the binary checkpoint head record.
    TronApplicationContext restarted = open(output, appendMode);
    try {
      Manager manager = restarted.getBean(Manager.class);
      assertNotNull(manager.getCommonCheckpointRuntime());
      assertEquals(CommonCheckpointRuntimeAttachment.State.READY,
          manager.getCommonCheckpointRuntime().getState());
      assertEquals(expectedHead,
          manager.getPathStateSnapshotHead().getHead().getBlockNumber());
    } finally {
      restarted.close();
      Args.clearParam();
    }
  }

  private static TronApplicationContext open(Path output, boolean appendMode) {
    Args.setParam(new String[]{"--output-directory", output.toString()},
        TestConstants.TEST_CONF);
    org.tron.core.config.args.Storage storage = Args.getInstance().getStorage();
    storage.setCommonCheckpointEnabled(true);
    storage.setPathStateRootEnabled(true);
    storage.setPathStateRootEngine("LEVELDB");
    storage.setStateArchiveServingIndexEngine("LEVELDB");
    if (appendMode) {
      StateArchiveAppendFileConfig append = new StateArchiveAppendFileConfig();
      append.setEnabled(true);
      storage.setStateArchiveAppendFileSettings(append);
    }
    TronApplicationContext context = new TronApplicationContext(DefaultConfig.class);
    ApplicationFactory.create(context);
    return context;
  }

  private static Chainbase store(SnapshotManager snapshots, String name) {
    return snapshots.getDbs().stream().filter(db -> name.equals(db.getDbName())).findFirst()
        .orElseThrow(() -> new IllegalStateException("missing store " + name));
  }

  private static void setFlushCount(SnapshotManager snapshots, int value) throws Exception {
    Field field = SnapshotManager.class.getDeclaredField("flushCount");
    field.setAccessible(true);
    field.setInt(snapshots, value);
  }

  private static byte[] accountKey(int number) {
    byte[] key = new byte[21];
    key[0] = 0x41;
    key[20] = (byte) number;
    return key;
  }

  private static byte[] accountValue(int number) {
    byte[] value = new byte[8];
    value[7] = (byte) number;
    return value;
  }

  private static byte[] jsonValue(int number) {
    return ("{\"transactionIds\":[\""
            + org.bouncycastle.util.encoders.Hex.toHexString(Longs.toByteArray(number))
            + "\"],\"num\":" + number + "}").getBytes(StandardCharsets.UTF_8);
  }
}
