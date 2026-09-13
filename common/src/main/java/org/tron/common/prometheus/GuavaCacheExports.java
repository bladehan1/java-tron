package org.tron.common.prometheus;

import static io.prometheus.client.SampleNameFilter.ALLOW_ALL;

import com.google.common.cache.CacheStats;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.tron.common.cache.CacheManager;
import org.tron.common.cache.TronCache;

/**
 * Exports metrics about for guava cache.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *   new GuavaCacheExports().register();
 * }
 * </pre>
 * Example metrics being exported:
 * <pre>
 *   tron:guava_cache_hit_rate{type="account"} 0.135679
 *   tron:guava_cache_request{type="account"} 3000
 * </pre>
 */
public class GuavaCacheExports extends Collector {

  private static final String TRON_GUAVA_CACHE_HIT_RATE = "tron:guava_cache_hit_rate";
  private static final String TRON_GUAVA_CACHE_REQUEST = "tron:guava_cache_request";
  private static final String TRON_GUAVA_CACHE_EVICTION_COUNT = "tron:guava_cache_eviction_count";


  public GuavaCacheExports() {
  }


  void addHitRateMetrics(List<MetricFamilySamples> sampleFamilies, Predicate<String> nameFilter,
      Map<String, CacheStats> stats) {
    if (nameFilter.test(TRON_GUAVA_CACHE_HIT_RATE)) {
      GaugeMetricFamily hitRate = new GaugeMetricFamily(
          TRON_GUAVA_CACHE_HIT_RATE,
          "Hit rate of a guava cache.",
          Collections.singletonList("type"));
      stats.forEach((k, v) -> hitRate
          .addMetric(Collections.singletonList(k), v.hitRate()));
      sampleFamilies.add(hitRate);
    }
  }

  void addRequestMetrics(List<MetricFamilySamples> sampleFamilies, Predicate<String> nameFilter,
      Map<String, CacheStats> stats) {
    if (nameFilter.test(TRON_GUAVA_CACHE_REQUEST)) {
      GaugeMetricFamily request = new GaugeMetricFamily(
          TRON_GUAVA_CACHE_REQUEST,
          "Request of a guava cache.",
          Collections.singletonList("type"));
      stats.forEach((k, v) -> request
          .addMetric(Collections.singletonList(k), v.requestCount()));
      sampleFamilies.add(request);
    }
  }

  void addEvictionCountMetrics(List<MetricFamilySamples> sampleFamilies,
      Predicate<String> nameFilter,
      Map<String, CacheStats> stats) {
    if (nameFilter.test(TRON_GUAVA_CACHE_EVICTION_COUNT)) {
      GaugeMetricFamily request = new GaugeMetricFamily(
          TRON_GUAVA_CACHE_EVICTION_COUNT,
          "Eviction count of a guava cache.",
          Collections.singletonList("type"));
      stats.forEach((k, v) -> request
          .addMetric(Collections.singletonList(k), v.evictionCount()));
      sampleFamilies.add(request);
    }
  }

  private void addReadCounts(List<MetricFamilySamples> families, Predicate<String> filter,
      Map<String, CacheStats> stats) {
    for (String outcome : Arrays.asList("hit", "miss")) {
      String metric = "tron:guava_cache_" + outcome + "_count";
      if (filter.test(metric)) {
        GaugeMetricFamily family = new GaugeMetricFamily(metric,
            "Cumulative cache reads; resets when the cache instance is replaced.",
            Collections.singletonList("type"));
        stats.forEach((name, value) -> family.addMetric(Collections.singletonList(name),
            "hit".equals(outcome) ? value.hitCount() : value.missCount()));
        families.add(family);
      }
    }
    String metric = "tron:guava_cache_removal_count";
    if (filter.test(metric)) {
      GaugeMetricFamily family = new GaugeMetricFamily(metric,
          "Removal notifications by cause; delivery may lag cache stats. Not read misses.",
          Arrays.asList("type", "cause"));
      CacheManager.removalCounts().forEach((name, counts) -> counts.forEach((cause, count) ->
          family.addMetric(Arrays.asList(name, cause.name().toLowerCase(Locale.ROOT)), count)));
      families.add(family);
    }
  }

  @Override
  public List<MetricFamilySamples> collect() {
    return collect(null);
  }

  @Override
  public List<MetricFamilySamples> collect(Predicate<String> nameFilter) {
    List<MetricFamilySamples> mfs = new ArrayList<>();
    Predicate<String> filter = nameFilter == null ? ALLOW_ALL : nameFilter;
    // Reuse each cache's snapshot across families. This is not a global atomic cache snapshot.
    Map<String, CacheStats> stats = CacheManager.stats();
    addHitRateMetrics(mfs, filter, stats);
    addRequestMetrics(mfs, filter, stats);
    addEvictionCountMetrics(mfs, filter, stats);
    if (Boolean.getBoolean(TronCache.READ_ATTRIBUTION_PROPERTY)) {
      addReadCounts(mfs, filter, stats);
    }
    return mfs;
  }
}
