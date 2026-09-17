package org.tron.core.db;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.StorageConfig.StateArchiveAppendFileConfig;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.StateArchiveAppendCheckpointMaterializerV4;
import org.tron.core.db2.archive.StateArchiveAppendCheckpointMaterializerV5;
import org.tron.core.db2.archive.StateArchiveAppendFileRuntime;
import org.tron.core.db2.core.CommonCheckpointBaseline;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.core.store.DynamicPropertiesStore;

public class ManagerStateArchiveAppendFormatTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void factoryKeepsV4DefaultAndRequiresExplicitV5() throws Exception {
    Manager manager = new Manager();
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(mock(DynamicPropertiesStore.class));
    ReflectionTestUtils.setField(manager, "chainBaseManager", chainBase);
    CommonCheckpointBaseline baseline = new CommonCheckpointBaseline(hash(70),
        BlockSnapshotMeta.forBlock(99, hash(99), hash(98), 3_000L), hash(40));

    StateArchiveAppendFileConfig config = new StateArchiveAppendFileConfig();
    Path v4Root = temporaryFolder.newFolder("manager-v4").toPath();
    try (StateArchiveAppendFileRuntime runtime = ReflectionTestUtils.invokeMethod(manager,
        "createAppendMaterializer", v4Root, hash(70), Engine.LEVELDB, baseline, config)) {
      assertTrue(runtime instanceof StateArchiveAppendCheckpointMaterializerV4);
    }

    config.setFormatVersion(5);
    Path v5Root = temporaryFolder.newFolder("manager-v5").toPath();
    try (StateArchiveAppendFileRuntime runtime = ReflectionTestUtils.invokeMethod(manager,
        "createAppendMaterializer", v5Root, hash(70), Engine.LEVELDB, baseline, config)) {
      assertTrue(runtime instanceof StateArchiveAppendCheckpointMaterializerV5);
    }
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
