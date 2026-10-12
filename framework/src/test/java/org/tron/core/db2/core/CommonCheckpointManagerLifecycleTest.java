package org.tron.core.db2.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.config.args.Storage;
import org.tron.core.config.args.StorageConfig.StateArchiveAppendFileConfig;
import org.tron.core.consensus.ConsensusService;
import org.tron.core.db.Manager;

/** Real Spring/native startup, including constructors, registered genesis writer and reopen. */
public class CommonCheckpointManagerLifecycleTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();
  private TronApplicationContext context;

  @After
  public void close() {
    if (context != null) {
      context.close();
      context = null;
    }
    Args.clearParam();
  }

  @Test
  public void freshAndExistingBaselineStartupUseRegisteredWritersAndCommonRuntime()
      throws Exception {
    File output = temporary.newFolder();
    open(output);
    SnapshotManager snapshots = context.getBean(SnapshotManager.class);
    assertTrue(snapshots.isP66SnapshotLaneInstalled());
    assertNotNull(context.getBean(Manager.class).getCommonCheckpointRuntime());
    assertTrue(Files.isRegularFile(output.toPath().resolve("common-checkpoint/COMMON_BASELINE")));
    assertFalse(snapshots.hasPendingMaintenance());
    context.getBean(ConsensusService.class).start();
    long head = context.getBean(Manager.class).getDynamicPropertiesStore()
        .getLatestBlockHeaderNumber();
    close();
    open(output);
    snapshots = context.getBean(SnapshotManager.class);
    assertTrue(snapshots.isP66SnapshotLaneInstalled());
    assertNotNull(context.getBean(Manager.class).getCommonCheckpointRuntime());
    assertFalse("same startup values must settle without a carrier",
        snapshots.hasPendingMaintenance());
    assertEquals(head, context.getBean(Manager.class).getDynamicPropertiesStore()
        .getLatestBlockHeaderNumber());
    context.getBean(ConsensusService.class).start();
  }

  private void open(File output) {
    Args.setParam(new String[]{"--output-directory", output.toString(),
        "--storage-db-engine", "ROCKSDB"}, TestConstants.TEST_CONF);
    Storage storage = Args.getInstance().getStorage();
    storage.setStateArchiveEnabled(true);
    storage.setCommonCheckpointEnabled(true);
    storage.setPathStateRootEnabled(true);
    StateArchiveAppendFileConfig append = new StateArchiveAppendFileConfig();
    append.setEnabled(true);
    storage.setStateArchiveAppendFileSettings(append);
    context = new TronApplicationContext(DefaultConfig.class);
    ApplicationFactory.create(context);
  }
}
