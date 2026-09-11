package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.prometheus.client.CollectorRegistry;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.db2.archive.ServingIndexTiming.Stage;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class ServingIndexTimingTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void observesActualIndexWorkAndSeparatesFailureFromSuccess() throws Exception {
    boolean previous = CommonParameter.getInstance().isMetricsPrometheusEnable();
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    try (PersistentServingKeyIndexGeneration.MutableIndex index =
        new PersistentServingKeyIndexGeneration.MutableIndex(
            temporaryFolder.newFolder().toPath(), Engine.ROCKSDB)) {
      try (ServingIndexTiming timing = new ServingIndexTiming("BULK_CATCH_UP", 100, 101)) {
        BlockReverseDiff diff = new BlockReverseDiff(BlockSnapshotMeta.forBlock(101, hash(101),
            hash(100), 303000), Collections.singletonList(new BlockReverseDiff.DbGroup("code",
                Collections.singletonList(new BlockReverseDiff.Entry(new byte[]{1},
                    OldValue.present(new byte[]{2}))))));
        ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.planCommittedDiffs(
            100, hash(100), Collections.singletonList(diff));
        index.append("timing", plan, hash(1), () -> { }, () -> { });
        timing.succeeded();
        assertEquals(1, timing.calls(Stage.DIGEST));
        assertEquals(1, timing.calls(Stage.META_GET));
        assertEquals(0, timing.calls(Stage.TAIL_GET));
        assertEquals(1, timing.calls(Stage.WRITE_SYNC));
      }
      Double duration = CollectorRegistry.defaultRegistry.getSampleValue(
          "tron_archive_serving_stage_seconds_sum", new String[]{"mode", "stage", "result"},
          new String[]{"BULK_CATCH_UP", "WRITE_SYNC", "success"});
      assertNotNull(duration);
      assertTrue(duration >= 0);
      Double blocks = CollectorRegistry.defaultRegistry.getSampleValue(
          "tron_archive_serving_work_total", new String[]{"mode", "kind", "result"},
          new String[]{"BULK_CATCH_UP", "indexed_blocks", "success"});
      assertNotNull(blocks);
      try (ServingIndexTiming failed = new ServingIndexTiming("BULK_CATCH_UP", 101, 102)) {
        ServingIndexTiming.record(Stage.PLAN, System.nanoTime());
      }
      assertEquals(blocks, CollectorRegistry.defaultRegistry.getSampleValue(
          "tron_archive_serving_work_total", new String[]{"mode", "kind", "result"},
          new String[]{"BULK_CATCH_UP", "indexed_blocks", "success"}));
      assertNotNull(CollectorRegistry.defaultRegistry.getSampleValue(
          "tron_archive_serving_stage_seconds_count", new String[]{"mode", "stage", "result"},
          new String[]{"BULK_CATCH_UP", "total", "failure"}));
    } finally {
      CommonParameter.getInstance().setMetricsPrometheusEnable(previous);
    }
  }

  private static byte[] hash(int height) {
    return ByteBuffer.allocate(32).putInt(height).array();
  }
}
