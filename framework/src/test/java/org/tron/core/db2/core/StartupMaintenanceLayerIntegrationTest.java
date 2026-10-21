package org.tron.core.db2.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.tron.core.db2.NativeCheckpointTestSupport.database;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.config.args.Args;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.SnapshotOldValueCollector;

public class StartupMaintenanceLayerIntegrationTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Args.setParam(new String[]{"--output-directory", "output_maintenance_test"},
        "config-test.conf");
  }

  @Test
  public void startupWritesFoldIntoHistoryAndSurviveCarrierPop() throws Exception {
    Path path = temporary.newFolder().toPath();
    SnapshotManager manager = new SnapshotManager("");
    Chainbase properties = new Chainbase(new SnapshotRoot(database(path, "properties")));
    try {
      manager.add(properties);
      manager.enable();
      byte[] key = "new-default".getBytes(StandardCharsets.US_ASCII);
      manager.openMaintenanceLayer();
      assertThrows(IllegalStateException.class, () -> properties.put(key, new byte[]{9}));
      manager.beginMaintenanceWrites();
      properties.put(key, new byte[]{7});
      manager.endMaintenanceWrites();
      manager.installCommonCheckpointArchiveCollector(new SnapshotOldValueCollector());
      try (ISession failed = manager.buildSession()) {
        properties.put(key, new byte[]{12});
        // No commit: revoking this first candidate must preserve the startup maintenance layer.
      }
      assertTrue(manager.hasPendingMaintenance());
      assertEquals(7, properties.getUnchecked(key)[0]);
      try (ISession session = manager.buildSession()) {
        properties.put(key, new byte[]{8});
        session.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1));
      }
      assertEquals(8, properties.getUnchecked(key)[0]);
      assertFalse(((SnapshotImpl) properties.getHead()).getPreparedArchiveBlock()
          .getGroups().get(0).getEntries().get(0).getOldValue().isPresent());
      manager.fastPop();
      assertTrue(manager.hasPendingMaintenance());
      assertEquals(7, properties.getUnchecked(key)[0]);
      try (ISession session = manager.buildSession()) {
        session.commit(BlockSnapshotMeta.forBlock(1, hash(2), hash(0), 2));
      }
      assertEquals(7, properties.getUnchecked(key)[0]);
      assertFalse(((SnapshotImpl) properties.getHead()).getPreparedArchiveBlock()
          .getGroups().get(0).getEntries().get(0).getOldValue().isPresent());
    } finally {
      manager.shutdown();
      properties.close();
    }
  }

  @Test
  public void diskAdmissionStopsNormalFlushButAllowsShutdownFlush() throws Exception {
    Path path = temporary.newFolder().toPath();
    Args.setParam(new String[]{"--output-directory", path.toString()}, "config-test.conf");
    org.tron.core.store.CheckTmpStore checkpoint = new org.tron.core.store.CheckTmpStore(null);
    SnapshotManager manager = new SnapshotManager("");
    manager.setCheckTmpStore(checkpoint);
    Chainbase properties = new Chainbase(new SnapshotRoot(database(path, "properties")));
    try {
      manager.add(properties);
      manager.check();
      manager.enable();
      byte[] key = new byte[]{1};
      try (ISession session = manager.buildSession()) {
        properties.put(key, new byte[]{7});
        session.commit();
      }
      manager.setMaxFlushCount(10);
      manager.setMaxSize(0);
      try (ISession ignored = manager.buildSession()) {
        // Stage the finalized layer below the normal checkpoint threshold.
      }
      manager.setMaxFlushCount(1);
      manager.setArchiveWriteAdmission(() -> {
        throw new IllegalStateException("disk redline");
      });
      assertThrows(IllegalStateException.class, manager::buildSession);
      assertThrows(IllegalStateException.class, manager::flush);
      manager.flushPending();
      assertEquals(7, properties.getHead().getRoot().get(key)[0]);
    } finally {
      manager.shutdown();
      properties.close();
      checkpoint.close();
    }
  }

  private static byte[] hash(int value) {
    byte[] bytes = new byte[32];
    bytes[31] = (byte) value;
    return bytes;
  }
}
