package org.tron.core.db2.stateroot;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.tron.common.prometheus.Metrics;

/** Prepared-attempt observations. Labels deliberately exclude block identities and keys. */
final class PathStateAttributionMetrics {
  private PathStateAttributionMetrics() {
  }

  private static boolean enabled() {
    return PathStateOperationTimer.enabled() && Metrics.enabled();
  }

  static void operation(int storeId, String operation, long calls, long samples, long nanos) {
    if (enabled()) {
      Export.CALLS.labels(Integer.toString(storeId), operation).inc(calls);
      Export.SAMPLES.labels(Integer.toString(storeId), operation).inc(samples);
      Export.SECONDS.labels(Integer.toString(storeId), operation).inc(nanos / 1e9);
    }
  }

  static void cache(int storeId, String kind, long count) {
    if (count >= 0 && enabled()) {
      Export.CACHE.labels(Integer.toString(storeId), kind).inc(count);
    }
  }

  static void rocksDb(int storeId, String ticker, long delta) {
    if (enabled() && PathStateOperationTimer.observesRocksDb(storeId) && delta >= 0) {
      Export.ROCKS_DB.labels(Integer.toString(storeId), ticker).inc(delta);
    }
  }

  static void nativeGetPerf(int storeId, String kind, long delta) {
    if (enabled() && PathStateOperationTimer.observesRocksDb(storeId) && delta >= 0) {
      Export.NATIVE_GET_PERF.labels(Integer.toString(storeId), kind).inc(delta);
    }
  }

  static void participant(int storeId, long submitToStartNanos, long workNanos) {
    if (enabled()) {
      String store = Integer.toString(storeId);
      Export.PARTICIPANT.labels(store, "submit_to_start").observe(submitToStartNanos / 1e9);
      Export.PARTICIPANT.labels(store, "work").observe(workNanos / 1e9);
    }
  }

  static void prepared(long head) {
    if (enabled()) {
      Export.ATTEMPTS.inc();
      Export.HEAD.set(head);
    }
  }

  private static final class Export {
    private static final Counter NATIVE_GET_PERF = Counter.build()
        .name("tron_pathstate_prepared_native_get_perf_total")
        .help("Point-get perf deltas; kind states units; sampled times may overlap.")
        .labelNames("store", "kind").register();
    private static final Counter ROCKS_DB = Counter.build()
        .name("tron_pathstate_prepared_rocksdb_ticker_total")
        .help("Database-wide ticker deltas during successful prepare windows; not N-only or disk IO.")
        .labelNames("store", "ticker").register();
    private static final Counter CALLS = Counter.build()
        .name("tron_pathstate_prepared_operation_calls_total")
        .help("Observed operation calls in completed PathState prepare attempts.")
        .labelNames("store", "operation").register();
    private static final Counter SAMPLES = Counter.build()
        .name("tron_pathstate_prepared_operation_samples_total")
        .help("Systematic one-in-64 timed samples in completed prepare attempts.")
        .labelNames("store", "operation").register();
    private static final Counter SECONDS = Counter.build()
        .name("tron_pathstate_prepared_operation_sampled_seconds_total")
        .help("Sampled operation seconds; not extrapolated total or wall time.")
        .labelNames("store", "operation").register();
    private static final Counter CACHE = Counter.build()
        .name("tron_pathstate_prepared_cache_events_total")
        .help("Cache events in completed prepare windows; layers are not disjoint.")
        .labelNames("store", "kind").register();
    private static final Histogram PARTICIPANT = Histogram.build()
        .name("tron_pathstate_prepared_participant_seconds")
        .help("Participant submit-to-start or work wall durations in completed prepares.")
        .labelNames("store", "stage").register();
    private static final Counter ATTEMPTS = Counter.build()
        .name("tron_pathstate_prepared_attempts_total")
        .help("Completed prepare attempts, including repeated or later abandoned candidates.")
        .register();
    private static final Gauge HEAD = Gauge.build()
        .name("tron_pathstate_last_prepared_height")
        .help("Last observed prepared height; not canonical or durable and may rewind.")
        .register();
  }
}
