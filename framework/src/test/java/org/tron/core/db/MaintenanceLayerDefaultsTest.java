package org.tron.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.tron.common.TestConstants.TEST_CONF;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.BaseTest;
import org.tron.common.utils.ByteArray;
import org.tron.core.config.args.Args;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.DynamicPropertiesStore;

/**
 * T02-15, DynamicPropertiesStore part (design v2 ch02 §3.11.3, WP-02-12): the real constructor
 * defaults as a registered intent. Form OFF (this context) never defers, so the constructor
 * wrote them as upstream does (D3). A deferred intent, applied inside the maintenance layer the
 * way {@code Manager.init} does, writes a missing key into the layer only (computed from the
 * visible state), and leaves a key that is already present, e.g. rolled forward from the WAL,
 * untouched.
 */
public class MaintenanceLayerDefaultsTest extends BaseTest {

  private static final byte[] KEY = "DYNAMIC_ENERGY_MAX_FACTOR".getBytes(StandardCharsets.UTF_8);

  static {
    Args.setParam(new String[] {"--output-directory", dbPath()}, TEST_CONF);
  }

  @Test
  public void defaultIntentIsAppliedInsideTheLayerAgainstTheVisibleState() {
    DynamicPropertiesStore properties = chainBaseManager.getDynamicPropertiesStore();
    SnapshotManager snapshots = (SnapshotManager) dbManager.getRevokingStore();
    assertFalse("form OFF writes the defaults at construction (D3)",
        properties.isConstructorDefaultsDeferred());
    assertFalse(snapshots.hasPendingMaintenance());
    assertEquals(0, snapshots.size());
    Chainbase root = properties("properties", snapshots);
    byte[] original = root.getHead().getRoot().get(KEY);
    long expected = Args.getInstance().getDynamicEnergyMaxFactor();
    try {
      // A release introducing this key: missing at the root, intent registered at construction.
      properties.delete(KEY);
      assertNull(root.getHead().getRoot().get(KEY));
      ReflectionTestUtils.setField(properties, "constructorDefaultsDeferred", true);

      snapshots.openMaintenanceLayer();
      snapshots.beginMaintenanceWrites();
      properties.applyDeferredConstructorDefaults();
      snapshots.endMaintenanceWrites();

      assertFalse(properties.isConstructorDefaultsDeferred());
      assertEquals(expected, properties.getDynamicEnergyMaxFactor());
      assertNull("the default stays in the layer", root.getHead().getRoot().get(KEY));
      Map<String, Integer> effective = snapshots.settleMaintenanceLayer();
      assertEquals(Collections.singletonMap("properties", 1), effective);
      snapshots.discardMaintenanceLayer();
      assertFalse(snapshots.hasPendingMaintenance());

      // The key rolled forward by the WAL is present after recovery: the intent leaves it.
      properties.saveDynamicEnergyMaxFactor(expected + 7);
      ReflectionTestUtils.setField(properties, "constructorDefaultsDeferred", true);
      snapshots.openMaintenanceLayer();
      snapshots.beginMaintenanceWrites();
      properties.applyDeferredConstructorDefaults();
      snapshots.endMaintenanceWrites();
      assertEquals(expected + 7, properties.getDynamicEnergyMaxFactor());
      assertTrue("nothing effective: the layer is closed",
          snapshots.settleMaintenanceLayer().isEmpty());
      assertFalse(snapshots.hasPendingMaintenance());
      // Applied once: a second call is a no-op.
      properties.applyDeferredConstructorDefaults();
    } finally {
      snapshots.discardMaintenanceLayer();
      if (original != null) {
        properties.saveDynamicEnergyMaxFactor(ByteArray.toLong(original));
      }
    }
  }

  private static Chainbase properties(String name, SnapshotManager snapshots) {
    return snapshots.getDbs().stream().filter(db -> name.equals(db.getDbName())).findFirst()
        .orElseThrow(IllegalStateException::new);
  }
}
