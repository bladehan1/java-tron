package org.tron.common.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import io.prometheus.client.Collector.MetricFamilySamples;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.prometheus.GuavaCacheExports;

public class CacheAttributionTest {

  private String previous;

  @Before
  public void enable() {
    previous = System.getProperty(TronCache.READ_ATTRIBUTION_PROPERTY);
    System.setProperty(TronCache.READ_ATTRIBUTION_PROPERTY, "true");
  }

  @After
  public void restore() {
    if (previous == null) {
      System.clearProperty(TronCache.READ_ATTRIBUTION_PROPERTY);
    } else {
      System.setProperty(TronCache.READ_ATTRIBUTION_PROPERTY, previous);
    }
  }

  @Test
  public void separatesRemovalCausesWithoutChangingLookup() {
    AtomicLong now = new AtomicLong();
    Ticker ticker = new Ticker() {
      @Override
      public long read() {
        return now.get();
      }
    };
    TronCache<String, String> cache = new TronCache<>(CacheType.account,
        CacheBuilder.newBuilder().maximumSize(1).concurrencyLevel(1)
            .expireAfterAccess(1, TimeUnit.SECONDS).ticker(ticker).recordStats());
    cache.put("a", "one");
    cache.put("a", "two");
    assertEquals("two", cache.getIfPresent("a"));
    cache.put("b", "three");
    assertEquals(1L, (long) cache.removalCounts().get(RemovalCause.SIZE));
    now.set(TimeUnit.SECONDS.toNanos(2));
    cache.put("c", "four");
    assertEquals(1L, (long) cache.removalCounts().get(RemovalCause.EXPIRED));
    cache.invalidateAll();
    assertEquals(1L, (long) cache.removalCounts().get(RemovalCause.REPLACED));
    assertEquals(1L, (long) cache.removalCounts().get(RemovalCause.EXPLICIT));
    assertEquals(2, cache.stats().evictionCount());
  }

  @Test
  public void disabledAndUnselectedCachesHaveNoRemovalTelemetry() {
    TronCache<String, String> other = new TronCache<>(CacheType.code, "maximumSize=1");
    System.clearProperty(TronCache.READ_ATTRIBUTION_PROPERTY);
    TronCache<String, String> disabled = new TronCache<>(CacheType.account, "maximumSize=1");
    for (TronCache<String, String> cache : java.util.Arrays.asList(other, disabled)) {
      cache.put("a", "one");
      cache.put("b", "two");
      assertTrue(cache.removalCounts().isEmpty());
      assertEquals("two", cache.getIfPresent("b"));
    }
  }

  @Test
  public void exportsExactReadCountsAndHonorsFilter() {
    TronCache<String, String> cache = CacheManager.allocate(CacheType.account,
        "maximumSize=2,recordStats");
    cache.put("a", "one");
    cache.getIfPresent("a");
    cache.getIfPresent("missing");
    GuavaCacheExports exporter = new GuavaCacheExports();
    Map<String, Double> values = new HashMap<>();
    for (MetricFamilySamples family : exporter.collect()) {
      family.samples.stream().filter(s -> s.labelValues.equals(
          java.util.Collections.singletonList("account")))
          .forEach(s -> values.put(s.name, s.value));
    }
    assertEquals(1.0, values.get("tron:guava_cache_hit_count"), 0);
    assertEquals(1.0, values.get("tron:guava_cache_miss_count"), 0);
    assertEquals(2.0, values.get("tron:guava_cache_request"), 0);
    assertEquals(0.5, values.get("tron:guava_cache_hit_rate"), 0);
    assertEquals(1, exporter.collect(n -> n.equals("tron:guava_cache_miss_count")).size());
    System.clearProperty(TronCache.READ_ATTRIBUTION_PROPERTY);
    assertEquals(3, exporter.collect().size());
  }
}
