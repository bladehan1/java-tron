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

  @Test
  public void preBoundChildrenRecordGetOutcome() {
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    CommonParameter.getInstance().setMetricsPrometheusDatabaseEnable(true);

    String database = "outcome-test";
    DbOperationMetrics metrics = DbOperationMetrics.create("ROCKSDB", database);
    double hitBefore = outcomeSample(database, "hit");
    double missBefore = outcomeSample(database, "miss");

    metrics.observeGetOutcome(true);
    metrics.observeGetOutcome(false);
    metrics.observeGetOutcome(false);

    assertEquals(hitBefore + 1, outcomeSample(database, "hit"), 0.0);
    assertEquals(missBefore + 2, outcomeSample(database, "miss"), 0.0);
  }

  private Double sample(String metric, String database) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(metric,
        new String[]{"type", "db", "op"}, new String[]{"ROCKSDB", database, "get"});
    return value == null ? 0.0 : value;
  }

  private double outcomeSample(String database, String outcome) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(
        MetricKeys.Counter.DB_GET + "_total",
        new String[]{"type", "db", "outcome"},
        new String[]{"ROCKSDB", database, outcome});
    return value == null ? 0.0 : value;
  }
}
