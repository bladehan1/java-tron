package org.tron.core.db2.stateroot.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.tron.common.prometheus.Metrics;

/** Standing L1 resident-cache access counters (TASK-049 Phase 0.2). Labels exclude keys. */
public final class PathStateResidentAccessMetrics {

  private PathStateResidentAccessMetrics() {
  }

  public static void access(int storeId, boolean updated, boolean nativeRead) {
    if (Metrics.enabled()) {
      Export.ACCESS.labels(Integer.toString(storeId),
              nativeRead ? "miss_native" : updated ? "hit_updated" : "hit_clean")
          .inc();
    }
  }

  public static void cacheState(long bytes, long entries, long evictions) {
    if (Metrics.enabled()) {
      Export.CACHE_BYTES.set(bytes);
      Export.CACHE_ENTRIES.set(entries);
      Export.CACHE_EVICTIONS.set(evictions);
    }
  }

  private static final class Export {
    private static final Counter ACCESS = Counter.build()
        .name("tron_pathstate_resident_access_total")
        .help("L1 resident-node cache access results by store; misses continue to native store.")
        .labelNames("store", "kind").register();
    private static final Gauge CACHE_BYTES = Gauge.build()
        .name("tron_pathstate_resident_cache_bytes")
        .help("Current L1 resident cache bytes against the shared budget.")
        .register();
    private static final Gauge CACHE_ENTRIES = Gauge.build()
        .name("tron_pathstate_resident_cache_entries")
        .help("Current L1 resident cache entries.")
        .register();
    private static final Gauge CACHE_EVICTIONS = Gauge.build()
        .name("tron_pathstate_resident_cache_evictions")
        .help("Cumulative L1 resident cache evictions since node start.")
        .register();
  }
}
