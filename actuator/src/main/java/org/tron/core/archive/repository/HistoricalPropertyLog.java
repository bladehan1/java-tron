package org.tron.core.archive.repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.store.DynamicPropertiesStore;

/**
 * Logging policy for dynamic properties that are ABSENT at a historical block (chapter 05 section
 * 3.10.5). Older blocks legitimately lack properties introduced later; those use the typed-getter
 * default and must not flood the log. Only unregistered keys (which then fail closed) warn.
 */
@Slf4j(topic = "VM")
final class HistoricalPropertyLog {

  private static final long WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  private static final AtomicLong DEFAULTED_ABSENT = new AtomicLong();
  private static final Map<String, Long> LAST_WARN = new ConcurrentHashMap<>();

  private HistoricalPropertyLog() {
  }

  static void absent(String key) {
    if (DynamicPropertiesStore.getLongPropertyDefault(key) != null) {
      DEFAULTED_ABSENT.incrementAndGet();
      logger.debug("Historical dynamic property {} is absent; using its typed-getter default",
          key);
      return;
    }
    long now = System.nanoTime();
    Long previous = LAST_WARN.get(key);
    if (previous == null || now - previous >= WARN_INTERVAL_NANOS) {
      LAST_WARN.put(key, now);
      logger.warn("Historical dynamic property {} is absent and has no typed-getter default",
          key);
    }
  }

  /** Number of ABSENT reads answered by a registered default since start (metric source). */
  static long defaultedAbsentCount() {
    return DEFAULTED_ABSENT.get();
  }
}
