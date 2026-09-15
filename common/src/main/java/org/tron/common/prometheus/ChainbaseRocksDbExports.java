package org.tron.common.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Optional, lifecycle-bound business database metrics. No native handles are owned here. */
public class ChainbaseRocksDbExports extends Collector {

  public static final String ENABLE_PROPERTY = "tron.chainbase.rocksdbStats";
  private static final AtomicLong NEXT_EPOCH = new AtomicLong();
  private static final Map<Registration, Boolean> REGISTRATIONS = new ConcurrentHashMap<>();

  public static boolean selected(String name) {
    return Boolean.getBoolean(ENABLE_PROPERTY)
        && Arrays.asList("account", "account-asset", "storage-row").contains(name);
  }

  public static Registration register(String name, Supplier<Map<String, Long>> reader) {
    Registration registration = new Registration(name, reader);
    REGISTRATIONS.put(registration, Boolean.TRUE);
    return registration;
  }

  public static final class Registration implements AutoCloseable {
    private final String name;
    private final long epoch = NEXT_EPOCH.incrementAndGet();
    private final Supplier<Map<String, Long>> reader;

    private Registration(String name, Supplier<Map<String, Long>> reader) {
      this.name = name;
      this.reader = reader;
    }

    @Override
    public void close() {
      REGISTRATIONS.remove(this);
    }
  }

  @Override
  public List<MetricFamilySamples> collect() {
    return collect(null);
  }

  @Override
  public List<MetricFamilySamples> collect(Predicate<String> filter) {
    GaugeMetricFamily available = new GaugeMetricFamily("tron_chainbase_rocksdb_available",
        "One unambiguous open database was sampled; zero means unavailable, not zero IO.",
        Collections.singletonList("store"));
    GaugeMetricFamily epoch = new GaugeMetricFamily("tron_chainbase_rocksdb_epoch",
        "Process-local open generation; split windows when this or PID changes.",
        Collections.singletonList("store"));
    GaugeMetricFamily values = new GaugeMetricFamily("tron_chainbase_rocksdb_read_count",
        "Database-wide cumulative tickers, not physical disk IO; reset on open.",
        Arrays.asList("store", "ticker"));
    GaugeMetricFamily properties = new GaugeMetricFamily("tron_chainbase_rocksdb_bytes",
        "Actual DB properties; cache values may be shared and must not be summed across stores.",
        Arrays.asList("store", "property"));
    List<MetricFamilySamples> families = Arrays.asList(available, epoch, values, properties);
    if (filter != null && families.stream().noneMatch(f -> filter.test(f.name))) {
      return Collections.emptyList();
    }
    Map<String, List<Registration>> groups = new ArrayList<>(REGISTRATIONS.keySet()).stream()
        .collect(Collectors.groupingBy(r -> r.name));
    groups.forEach((name, registrations) -> {
      Map<String, Long> sample = null;
      Registration registration = registrations.get(0);
      if (registrations.size() == 1) {
        try {
          sample = registration.reader.get();
        } catch (RuntimeException unavailable) {
          // A diagnostics scrape must not break the metrics endpoint.
          sample = null;
        }
      }
      available.addMetric(Collections.singletonList(name), sample == null ? 0 : 1);
      if (sample != null) {
        epoch.addMetric(Collections.singletonList(name), registration.epoch);
        sample.forEach((key, value) -> {
          if (key.startsWith("rocksdb.")) {
            properties.addMetric(Arrays.asList(name, key.substring("rocksdb.".length())), value);
          } else {
            values.addMetric(Arrays.asList(name, key), value);
          }
        });
      }
    });
    return families.stream().filter(f -> filter == null || filter.test(f.name))
        .collect(Collectors.toList());
  }
}
