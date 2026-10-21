package org.tron.core.archive.checkpoint;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.args.StorageConfig.StateArchiveAppendFileConfig;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.StateArchiveAppendCheckpointMaterializerV5;
import org.tron.core.db2.archive.StateArchiveAppendFileRuntime;
import org.tron.core.db2.checkpoint.common.CommonCheckpointBaseline;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.core.store.DynamicPropertiesStore;

public class StateArchiveCheckpointStartupTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test
  public void factoryAlwaysCreatesV5() throws Exception {
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    DynamicPropertiesStore dynamicProperties = mock(DynamicPropertiesStore.class);
    BlockCapsule canonical = new BlockCapsule(99, Sha256Hash.wrap(hash(98)), 3_000L,
        ByteString.EMPTY);
    when(chainBase.getBlockByNum(99)).thenReturn(canonical);
    CommonCheckpointBaseline baseline = new CommonCheckpointBaseline(hash(70),
        BlockSnapshotMeta.forBlock(99, canonical.getBlockId().getBytes(), hash(98), 3_000L),
        hash(40));

    StateArchiveAppendFileConfig config = new StateArchiveAppendFileConfig();
    Path v5Root = temporaryFolder.newFolder("checkpoint-v5").toPath();
    try (StateArchiveAppendFileRuntime runtime =
        StateArchiveCheckpointStartup.createAppendMaterializer(v5Root, hash(70), Engine.LEVELDB,
            baseline, config, null, dynamicProperties, chainBase)) {
      assertTrue(runtime instanceof StateArchiveAppendCheckpointMaterializerV5);
    }
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
