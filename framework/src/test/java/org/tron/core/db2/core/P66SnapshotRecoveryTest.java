package org.tron.core.db2.core;

import static org.junit.Assert.assertThrows;

import com.google.common.primitives.Longs;
import java.util.Collections;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.store.AccountAssetStore;

public class P66SnapshotRecoveryTest extends BaseMethodTest {

  @Test
  public void managerStartupFinishesRecoveryBeforePhysicalSnapshotWritesTakeOver() {
    SnapshotManager manager = context.getBean(SnapshotManager.class);
    AccountAssetStore assets = chainBaseManager.getAccountAssetStore();
    byte[] asset = new byte[22];
    asset[0] = 0x41;
    assertThrows(IllegalStateException.class,
        () -> assets.updateByBatchSynced(Collections.singletonMap(asset, Longs.toByteArray(24))));
    assertThrows(IllegalStateException.class, () -> assets.enableSnapshots(manager, true));
  }
}
