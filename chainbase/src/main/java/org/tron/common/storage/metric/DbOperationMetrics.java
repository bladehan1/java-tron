package org.tron.common.storage.metric;

import io.prometheus.client.Histogram;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;

/**
 * Pre-bound Prometheus children for a database instance.
 *
 * <p>Resolving label values on every DB operation adds avoidable allocations and map lookups to
 * the hottest storage paths. This holder resolves the bounded engine/database/operation label set
 * once when a DB is opened. When database metrics are disabled all methods are cheap no-ops.
 */
public final class DbOperationMetrics {

  private final Histogram.Child getLatency;
  private final Histogram.Child putLatency;
  private final Histogram.Child deleteLatency;
  private final Histogram.Child batchLatency;
  private final Histogram.Child getBytes;
  private final Histogram.Child putBytes;
  private final Histogram.Child batchBytes;

  private DbOperationMetrics(String engine, String database) {
    getLatency = child(MetricKeys.Histogram.DB_OPERATE_LATENCY, engine, database, "get");
    putLatency = child(MetricKeys.Histogram.DB_OPERATE_LATENCY, engine, database, "put");
    deleteLatency = child(MetricKeys.Histogram.DB_OPERATE_LATENCY, engine, database, "delete");
    batchLatency = child(MetricKeys.Histogram.DB_OPERATE_LATENCY, engine, database, "batch");
    getBytes = child(MetricKeys.Histogram.DB_OPERATE_BYTES, engine, database, "get");
    putBytes = child(MetricKeys.Histogram.DB_OPERATE_BYTES, engine, database, "put");
    batchBytes = child(MetricKeys.Histogram.DB_OPERATE_BYTES, engine, database, "batch");
  }

  public static DbOperationMetrics create(String engine, String database) {
    return new DbOperationMetrics(engine, database);
  }

  public boolean enabled() {
    return getLatency != null;
  }

  public Histogram.Timer startGet() {
    return timer(getLatency);
  }

  public Histogram.Timer startPut() {
    return timer(putLatency);
  }

  public Histogram.Timer startDelete() {
    return timer(deleteLatency);
  }

  public Histogram.Timer startBatch() {
    return timer(batchLatency);
  }

  public void observeGetBytes(long bytes) {
    observe(getBytes, bytes);
  }

  public void observePutBytes(long bytes) {
    observe(putBytes, bytes);
  }

  public void observeBatchBytes(long bytes) {
    observe(batchBytes, bytes);
  }

  private static Histogram.Child child(String key, String engine, String database, String op) {
    return Metrics.databaseHistogramChild(key, engine, database, op);
  }

  private static Histogram.Timer timer(Histogram.Child child) {
    return child == null ? null : child.startTimer();
  }

  private static void observe(Histogram.Child child, long value) {
    if (child != null) {
      child.observe(value);
    }
  }
}
