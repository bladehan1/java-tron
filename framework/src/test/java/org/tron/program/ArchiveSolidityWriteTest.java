package org.tron.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.db.KhaosDatabase;
import org.tron.core.db.Manager;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.SnapshotOldValueCollector;
import org.tron.core.db2.common.RocksDB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotImpl;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.Protocol;

/** Regression for the real ARM snapshot startup refusal and the subsequent post-block writer. */
public class ArchiveSolidityWriteTest {

  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Args.setParam(new String[]{"--output-directory", "output_archive_solidity_test"},
        "config-test.conf");
  }

  @Test
  public void archiveCursorUsesHeadWithoutStartupOrPostBlockRootWrites() throws Exception {
    boolean original = Args.getInstance().getStorage().isCommonCheckpointEnabled();
    DynamicPropertiesStore properties = mock(DynamicPropertiesStore.class);
    when(properties.getLatestSolidifiedBlockNum()).thenReturn(3L);
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(properties);
    when(chainBase.getHeadBlockNum()).thenReturn(10L);
    SolidityNode node = new SolidityNode();
    ReflectionTestUtils.setField(node, "chainBaseManager", chainBase);
    ReflectionTestUtils.setField(node, "tronNetDelegate", mock(TronNetDelegate.class));
    try {
      Args.getInstance().getStorage().setCommonCheckpointEnabled(true);
      invoke(node, "init");
      Field cursor = SolidityNode.class.getDeclaredField("ID");
      cursor.setAccessible(true);
      assertEquals(10L, ((AtomicLong) cursor.get(node)).get());
      Method process = SolidityNode.class.getDeclaredMethod("loopProcessBlock",
          Protocol.Block.class);
      process.setAccessible(true);
      process.invoke(node, block(11).getInstance());
      verify(properties, never()).saveLatestSolidifiedBlockNum(anyLong());
    } finally {
      node.close();
      Args.getInstance().getStorage().setCommonCheckpointEnabled(original);
    }
  }

  @Test
  public void trustedSolidHeightIsInReverseDiffAndRestoredOnPop() throws Exception {
    Path path = temporary.newFolder().toPath();
    byte[] key = "LATEST_SOLIDIFIED_BLOCK_NUM".getBytes(StandardCharsets.US_ASCII);
    Chainbase properties = new Chainbase(new SnapshotRoot(new RocksDB(
        new RocksDbDataSourceImpl(path.toString(), "properties"))));
    SnapshotManager snapshots = new SnapshotManager("");
    boolean original = Args.getInstance().isSolidityNode();
    try {
      properties.put(key, ByteArray.fromLong(3));
      snapshots.add(properties);
      snapshots.enable();
      snapshots.installCommonCheckpointArchiveCollector(new SnapshotOldValueCollector());
      DynamicPropertiesStore dynamic = mock(DynamicPropertiesStore.class);
      when(dynamic.getLatestBlockHeaderNumber()).thenReturn(11L);
      when(dynamic.getLatestSolidifiedBlockNum()).thenAnswer(
          ignored -> ByteArray.toLong(properties.getUnchecked(key)));
      doAnswer(call -> {
        properties.put(key, ByteArray.fromLong(call.getArgument(0)));
        return null;
      }).when(dynamic).saveLatestSolidifiedBlockNum(anyLong());
      ChainBaseManager chainBase = mock(ChainBaseManager.class);
      when(chainBase.getDynamicPropertiesStore()).thenReturn(dynamic);
      Manager manager = new Manager();
      ReflectionTestUtils.setField(manager, "chainBaseManager", chainBase);
      ReflectionTestUtils.setField(manager, "revokingStore", snapshots);
      ReflectionTestUtils.setField(manager, "khaosDb", mock(KhaosDatabase.class));
      ReflectionTestUtils.setField(manager, "archiveFeaturesEnabled", true);
      Args.getInstance().setSolidityNode(true);
      try (ISession failed = snapshots.buildSession()) {
        manager.updateDynamicProperties(block(11));
        assertEquals(11L, dynamic.getLatestSolidifiedBlockNum());
        assertEquals(3L, ByteArray.toLong(properties.getHead().getRoot().get(key)));
      }
      assertEquals(3L, dynamic.getLatestSolidifiedBlockNum());
      try (ISession session = snapshots.buildSession()) {
        manager.updateDynamicProperties(block(11));
        session.commit(BlockSnapshotMeta.forBlock(11, hash(11), hash(10), 11));
      }
      assertEquals(11L, dynamic.getLatestSolidifiedBlockNum());
      org.tron.core.db2.archive.BlockReverseDiff diff =
          ((SnapshotImpl) properties.getHead()).getPreparedArchiveBlock();
      assertFalse(diff.getGroups().isEmpty());
      assertEquals(3L, ByteArray.toLong(
          diff.getGroups().get(0).getEntries().get(0).getOldValue().getValue()));
      snapshots.fastPop();
      assertEquals(3L, dynamic.getLatestSolidifiedBlockNum());
    } finally {
      Args.getInstance().setSolidityNode(original);
      snapshots.shutdown();
      properties.close();
    }
  }

  private static void invoke(SolidityNode node, String name) throws Exception {
    Method method = SolidityNode.class.getDeclaredMethod(name);
    method.setAccessible(true);
    method.invoke(node);
  }

  private static BlockCapsule block(long number) {
    return new BlockCapsule(Protocol.Block.newBuilder().setBlockHeader(
        Protocol.BlockHeader.newBuilder().setRawData(
            Protocol.BlockHeader.raw.newBuilder().setNumber(number).setTimestamp(number)))
        .build());
  }

  private static byte[] hash(int value) {
    byte[] bytes = new byte[32];
    bytes[31] = (byte) value;
    return bytes;
  }
}
