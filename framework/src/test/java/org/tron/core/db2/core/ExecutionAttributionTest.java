package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.db2.common.DB;

public class ExecutionAttributionTest {
  private String previous;
  private boolean metrics;
  private org.tron.core.config.args.Storage storage;

  @Before
  public void setup() {
    storage = CommonParameter.getInstance().getStorage();
    CommonParameter.getInstance().storage = new org.tron.core.config.args.Storage();
    previous = System.getProperty("tron.chainbase.executionAttribution");
    metrics = CommonParameter.getInstance().isMetricsPrometheusEnable();
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    System.setProperty("tron.chainbase.executionAttribution", "true");
  }

  @After
  public void cleanup() {
    CommonParameter.getInstance().storage = storage;
    if (previous == null) {
      System.clearProperty("tron.chainbase.executionAttribution");
    } else {
      System.setProperty("tron.chainbase.executionAttribution", previous);
    }
    CommonParameter.getInstance().setMetricsPrometheusEnable(metrics);
  }

  @Test
  public void disabledAndAbandonedAttemptsDoNotExportOrLeak() {
    System.clearProperty("tron.chainbase.executionAttribution");
    assertNull(ExecutionAttribution.open(1, "hash"));
    assertEquals(0, ExecutionAttribution.start());
    System.setProperty("tron.chainbase.executionAttribution", "true");
    double before = stageCalls();
    try (ExecutionAttribution ignored = ExecutionAttribution.open(1, "hash")) {
      ExecutionAttribution.stage("validate", ExecutionAttribution.start());
    }
    assertEquals(before, stageCalls(), 0);
    assertEquals(0, ExecutionAttribution.sample("account", "snapshot"));
    try (ExecutionAttribution scope = ExecutionAttribution.open(2, "hash2")) {
      ExecutionAttribution.stage("validate", ExecutionAttribution.start());
      scope.publish();
      scope.publish();
    }
    assertEquals(before + 1, stageCalls(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sampledReadsPreserveTombstonesLayersAndRootFallback() {
    DB<byte[], byte[]> db = mock(DB.class);
    when(db.getDbName()).thenReturn("account");
    byte[] key = {1};
    byte[] value = {7};
    when(db.get(key)).thenReturn(value);
    SnapshotRoot root = new SnapshotRoot(db);
    SnapshotImpl older = new SnapshotImpl(root);
    older.remove(key);
    SnapshotImpl head = new SnapshotImpl(older);
    double calls = read("snapshot", "calls");
    double samples = read("snapshot", "samples");
    double layers = read("snapshot", "layers");
    double hits = read("snapshot", "hits");
    try (ExecutionAttribution scope = ExecutionAttribution.open(1, "hash")) {
      for (int i = 0; i < 65; i++) {
        assertNull(head.get(key));
      }
      scope.publish();
    }
    assertEquals(calls + 65, read("snapshot", "calls"), 0);
    assertEquals(samples + 2, read("snapshot", "samples"), 0);
    assertEquals(layers + 4, read("snapshot", "layers"), 0);
    assertEquals(hits + 2, read("snapshot", "hits"), 0);
    double databaseCalls = read("database", "calls");
    try (ExecutionAttribution scope = ExecutionAttribution.open(2, "hash2")) {
      assertArrayEquals(value, new SnapshotImpl(root).get(key));
      scope.publish();
    }
    assertEquals(databaseCalls + 1, read("database", "calls"), 0);
  }

  @Test
  public void otherThreadsAndUnselectedStoresDoNotContribute() throws Exception {
    try (ExecutionAttribution scope = ExecutionAttribution.open(1, "hash")) {
      assertEquals(0, ExecutionAttribution.sample("unregistered", "snapshot"));
      long[] other = {-1};
      Thread thread = new Thread(() -> other[0] = ExecutionAttribution.start());
      thread.start();
      thread.join();
      assertEquals(0, other[0]);
      assertTrue(ExecutionAttribution.start() != 0);
      scope.publish();
    }
  }

  @Test
  public void phaseCountersPartitionTheSameSamplesWithoutResampling() {
    String metric = "tron_chainbase_execution_phase_read_total";
    String[] labels = {"phase", "store", "operation", "kind"};
    String[] bandwidthCalls = {"bandwidth", "account", "database", "calls"};
    String[] runtimeCalls = {"runtime", "account", "database", "calls"};
    String[] bandwidthSamples = {"bandwidth", "account", "database", "samples"};
    String[] runtimeSamples = {"runtime", "account", "database", "samples"};
    double bCalls = sample(metric, labels, bandwidthCalls);
    double rCalls = sample(metric, labels, runtimeCalls);
    double bSamples = sample(metric, labels, bandwidthSamples);
    double rSamples = sample(metric, labels, runtimeSamples);
    double total = read("database", "calls");
    try (ExecutionAttribution scope = ExecutionAttribution.open(3, "hash3")) {
      ExecutionAttribution.phase(ExecutionAttribution.Phase.BANDWIDTH);
      long started = ExecutionAttribution.sample("account", "database");
      ExecutionAttribution.sampled("account", "database", started, 0, false);
      ExecutionAttribution.phase(ExecutionAttribution.Phase.RUNTIME);
      for (int i = 0; i < 64; i++) {
        started = ExecutionAttribution.sample("account", "database");
        ExecutionAttribution.sampled("account", "database", started, 0, false);
      }
      scope.publish();
    }
    assertEquals(bCalls + 1, sample(metric, labels, bandwidthCalls), 0);
    assertEquals(rCalls + 64, sample(metric, labels, runtimeCalls), 0);
    assertEquals(bSamples + 1, sample(metric, labels, bandwidthSamples), 0);
    assertEquals(rSamples + 1, sample(metric, labels, runtimeSamples), 0);
    assertEquals(total + 65, read("database", "calls"), 0);
  }

  private static double stageCalls() {
    return sample("tron_chainbase_execution_stage_total", new String[]{"stage", "kind"},
        new String[]{"validate", "calls"});
  }

  private static double read(String operation, String kind) {
    return sample("tron_chainbase_execution_read_total",
        new String[]{"store", "operation", "kind"}, new String[]{"account", operation, kind});
  }

  private static double sample(String name, String[] labels, String[] values) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(name, labels, values);
    return value == null ? 0 : value;
  }
}
