package org.tron.core.db2.core;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.Longs;
import java.util.Collections;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.config.args.Args;
import org.tron.core.config.args.Storage;
import org.tron.core.store.AccountAssetStore;

public class P66SnapshotRecoveryTest extends BaseMethodTest {

  @Override
  protected void beforeContext() {
    Storage storage = Args.getInstance().getStorage();
    storage.setDbEngine("ROCKSDB");
    storage.setCommonCheckpointEnabled(true);
    storage.setStateArchiveEnabled(true);
    storage.setPathStateRootEnabled(true);
    storage.setPathStateRootEngine("ROCKSDB");
    storage.setStateArchiveServingIndexEngine("ROCKSDB");
  }

  @Test
  public void managerStartupFinishesRecoveryBeforePhysicalSnapshotWritesTakeOver() {
    SnapshotManager manager = context.getBean(SnapshotManager.class);
    AccountAssetStore assets = chainBaseManager.getAccountAssetStore();
    assertTrue(manager.isP66SnapshotLaneInstalled());
    assertTrue(assets.isSnapshotsAttached());
    byte[] asset = new byte[22];
    asset[0] = 0x41;
    assertThrows(IllegalStateException.class,
        () -> assets.updateByBatchSynced(Collections.singletonMap(asset, Longs.toByteArray(24))));
    assertThrows(IllegalStateException.class, () -> assets.enableSnapshots(manager, true));
  }
}
