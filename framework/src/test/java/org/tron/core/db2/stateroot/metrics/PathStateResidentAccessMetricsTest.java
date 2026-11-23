package org.tron.core.db2.stateroot.metrics;

import static org.junit.Assert.assertEquals;

import io.prometheus.client.CollectorRegistry;
import org.junit.Test;
import org.tron.common.parameter.CommonParameter;

public class PathStateResidentAccessMetricsTest {

  @Test
  public void accessCountsRespectMetricsGateAndStoreIsolation() {
    boolean oldMetrics = CommonParameter.getInstance().isMetricsPrometheusEnable();
    String metric = "tron_pathstate_resident_access_total";
    String[] labels = {"store", "kind"};
    String[] clean = {"4", "hit_clean"};
    String[] updated = {"4", "hit_updated"};
    String[] miss = {"22", "miss_native"};
    try {
      CommonParameter.getInstance().setMetricsPrometheusEnable(false);
      double baseClean = sample(metric, labels, clean);
      double baseUpdated = sample(metric, labels, updated);
      double baseMiss = sample(metric, labels, miss);
      PathStateResidentAccessMetrics.access(4, false, false);
      PathStateResidentAccessMetrics.access(4, true, false);
      PathStateResidentAccessMetrics.access(22, false, true);
      assertEquals(baseClean, sample(metric, labels, clean), 0);
      assertEquals(baseUpdated, sample(metric, labels, updated), 0);
      assertEquals(baseMiss, sample(metric, labels, miss), 0);

      CommonParameter.getInstance().setMetricsPrometheusEnable(true);
      PathStateResidentAccessMetrics.access(4, false, false);
      PathStateResidentAccessMetrics.access(4, true, false);
      PathStateResidentAccessMetrics.access(22, false, true);
      assertEquals(baseClean + 1, sample(metric, labels, clean), 0);
      assertEquals(baseUpdated + 1, sample(metric, labels, updated), 0);
      assertEquals(baseMiss + 1, sample(metric, labels, miss), 0);
    } finally {
      CommonParameter.getInstance().setMetricsPrometheusEnable(oldMetrics);
    }
  }

  @Test
  public void cacheStateSetsGaugesOnlyWhenEnabled() {
    boolean oldMetrics = CommonParameter.getInstance().isMetricsPrometheusEnable();
    try {
      CommonParameter.getInstance().setMetricsPrometheusEnable(false);
      PathStateResidentAccessMetrics.cacheState(1, 2, 3);
      assertEquals(0, sample("tron_pathstate_resident_cache_bytes"), 0);

      CommonParameter.getInstance().setMetricsPrometheusEnable(true);
      PathStateResidentAccessMetrics.cacheState(1024, 7, 11);
      assertEquals(1024, sample("tron_pathstate_resident_cache_bytes"), 0);
      assertEquals(7, sample("tron_pathstate_resident_cache_entries"), 0);
      assertEquals(11, sample("tron_pathstate_resident_cache_evictions"), 0);
    } finally {
      CommonParameter.getInstance().setMetricsPrometheusEnable(oldMetrics);
    }
  }

  private static double sample(String name, String[] labels, String[] values) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(name, labels, values);
    return value == null ? 0 : value;
  }

  private static double sample(String name) {
    Double value = CollectorRegistry.defaultRegistry.getSampleValue(name);
    return value == null ? 0 : value;
  }
}
