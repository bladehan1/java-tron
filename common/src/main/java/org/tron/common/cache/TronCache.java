package org.tron.common.cache;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;

public class TronCache<K, V> {

  public static final String READ_ATTRIBUTION_PROPERTY = "tron.chainbase.cacheAttribution";

  private final Map<RemovalCause, AtomicLong> removals = new EnumMap<>(RemovalCause.class);

  @Getter
  private final CacheType name;
  private final Cache<K, V> cache;

  TronCache(CacheType name, String strategy) {
    this(name, CacheBuilder.from(strategy));
  }

  TronCache(CacheType name, CacheBuilder<Object, Object> builder) {
    this.name = name;
    this.cache = configure(builder).build();
  }

  TronCache(CacheType name, String strategy, CacheLoader<K, V> loader) {
    this.name = name;
    this.cache = configure(CacheBuilder.from(strategy)).build(loader);
  }

  private CacheBuilder<Object, Object> configure(CacheBuilder<Object, Object> builder) {
    if (Boolean.getBoolean(READ_ATTRIBUTION_PROPERTY)
        && (name == CacheType.account || name == CacheType.storageRow)) {
      for (RemovalCause cause : RemovalCause.values()) {
        removals.put(cause, new AtomicLong());
      }
      builder.removalListener(notification ->
          removals.get(notification.getCause()).incrementAndGet());
    }
    return builder;
  }

  /** Per-instance removal notifications; empty when attribution was disabled at construction. */
  public Map<RemovalCause, Long> removalCounts() {
    if (removals.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<RemovalCause, Long> counts = new EnumMap<>(RemovalCause.class);
    removals.forEach((cause, count) -> counts.put(cause, count.get()));
    return counts;
  }

  public void put(K k, V v) {
    this.cache.put(k, v);
  }

  public V getIfPresent(K k) {
    return this.cache.getIfPresent(k);
  }

  public V get(K k, Callable<? extends V> loader) throws ExecutionException {
    return this.cache.get(k, loader);
  }

  public CacheStats stats() {
    return this.cache.stats();
  }

  public void invalidateAll() {
    this.cache.invalidateAll();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TronCache<?, ?> tronCache = (TronCache<?, ?>) o;
    return Objects.equal(name, tronCache.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }
}
