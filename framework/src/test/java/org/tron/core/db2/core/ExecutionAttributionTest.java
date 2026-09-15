package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import java.lang.reflect.Field;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.db2.common.DB;

public class ExecutionAttributionTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
  public void databaseTimingPreservesLockingValuesAndFailureRelease() throws Exception {
    RocksDbDataSourceImpl db = new RocksDbDataSourceImpl(
        temporaryFolder.newFolder().toString(), "account");
    byte[] key = {1};
    byte[] value = {2};
    db.putData(key, value);
    Field field = RocksDbDataSourceImpl.class.getDeclaredField("resetDbLock");
    field.setAccessible(true);
    ReentrantReadWriteLock lock = (ReentrantReadWriteLock) field.get(db);
    double lockCalls = read("database_lock", "calls");
    double getCalls = read("database_get", "calls");
    FutureTask<byte[]> task = new FutureTask<>(() -> {
      try (ExecutionAttribution attempt = ExecutionAttribution.open(4, "locked")) {
        byte[] result = db.getData(key);
        attempt.publish();
        return result;
      }
    });
    Thread reader = new Thread(task);
    try {
      lock.writeLock().lock();
      try {
        reader.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!lock.hasQueuedThread(reader) && System.nanoTime() < deadline) {
          Thread.yield();
        }
        assertTrue("reader must wait for reset lock", lock.hasQueuedThread(reader));
        assertEquals(getCalls, read("database_get", "calls"), 0);
      } finally {
        lock.writeLock().unlock();
      }
      assertArrayEquals(value, task.get(5, TimeUnit.SECONDS));
      assertEquals(lockCalls + 1, read("database_lock", "calls"), 0);
      assertEquals(getCalls + 1, read("database_get", "calls"), 0);
      try (ExecutionAttribution attempt = ExecutionAttribution.open(5, "invalid")) {
        org.junit.Assert.assertThrows(IllegalArgumentException.class, () -> db.getData(null));
        assertEquals(0, lock.getReadLockCount());
        assertNull(db.getData(new byte[] {3}));
        assertArrayEquals(value, db.getData(key));
        attempt.publish();
      }
      assertEquals(lockCalls + 4, read("database_lock", "calls"), 0);
      assertEquals(getCalls + 3, read("database_get", "calls"), 0);
    } finally {
      reader.join(5000);
      db.closeDB();
    }
  }

  @Test
  public void callerScopesPartitionReadsAndRestoreAfterExceptions() {
    double owner = siteCalls("bandwidth_owner");
    double history = siteCalls("balance_history");
    double other = siteCalls("other");
    double total = read("snapshot", "calls");
    try (ExecutionAttribution attempt = ExecutionAttribution.open(3, "sites")) {
      try (ExecutionAttribution.ReadScope ignored = ExecutionAttribution.readSite(
          ExecutionAttribution.ReadSite.BANDWIDTH_OWNER)) {
        sampledAccountRead();
        try (ExecutionAttribution.ReadScope nested = ExecutionAttribution.readSite(
            ExecutionAttribution.ReadSite.BALANCE_HISTORY)) {
          sampledAccountRead();
          throw new IllegalStateException("read failed");
        } catch (IllegalStateException expected) {
          sampledAccountRead();
        }
      }
      sampledAccountRead();
      attempt.publish();
    }
    assertEquals(owner + 2, siteCalls("bandwidth_owner"), 0);
    assertEquals(history + 1, siteCalls("balance_history"), 0);
    assertEquals(other + 1, siteCalls("other"), 0);
    assertEquals(total + 4, read("snapshot", "calls"), 0);
    assertNull(ExecutionAttribution.readSite(ExecutionAttribution.ReadSite.BANDWIDTH_OWNER));
  }

  private static void sampledAccountRead() {
    long started = ExecutionAttribution.sample("account", "snapshot");
    ExecutionAttribution.sampled("account", "snapshot", started, 0, false);
  }

  private static double siteCalls(String site) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(
        "tron_chainbase_execution_site_read_total",
        new String[] {"phase", "site", "store", "operation", "kind"},
        new String[] {"other", site, "account", "snapshot", "calls"});
    return value == null ? 0 : value;
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
