package org.tron.common.storage.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import org.junit.After;
import org.junit.Test;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;

public class DbOperationMetricsTest {

  @After
  public void restoreMetricsFlags() {
    CommonParameter.getInstance().setMetricsPrometheusEnable(false);
    CommonParameter.getInstance().setMetricsPrometheusDatabaseEnable(false);
  }

  @Test
  public void databaseMetricsRequireDedicatedOptIn() {
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    CommonParameter.getInstance().setMetricsPrometheusDatabaseEnable(false);

    DbOperationMetrics metrics = DbOperationMetrics.create("ROCKSDB", "disabled-test");

    assertNull(metrics.startGet());
  }

  @Test
  public void preBoundChildrenRecordLatencyAndBytes() {
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    CommonParameter.getInstance().setMetricsPrometheusDatabaseEnable(true);

    String database = "pre-bound-test";
    DbOperationMetrics metrics = DbOperationMetrics.create("ROCKSDB", database);
    Double latencyBefore = sample(MetricKeys.Histogram.DB_OPERATE_LATENCY + "_count", database);
    Double bytesBefore = sample(MetricKeys.Histogram.DB_OPERATE_BYTES + "_count", database);

    try (Histogram.Timer timer = metrics.startGet()) {
      assertNotNull(timer);
    }
    metrics.observeGetBytes(128);

    assertEquals(latencyBefore + 1, sample(
        MetricKeys.Histogram.DB_OPERATE_LATENCY + "_count", database), 0.0);
    assertEquals(bytesBefore + 1, sample(
        MetricKeys.Histogram.DB_OPERATE_BYTES + "_count", database), 0.0);
  }

  private Double sample(String metric, String database) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(metric,
        new String[]{"type", "db", "op"}, new String[]{"ROCKSDB", database, "get"});
    return value == null ? 0.0 : value;
  }
}
