package org.tron.common.storage.metric;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;

/**
 * Reports on-disk size for archive databases that open storage directly instead of through
 * RocksDbDataSourceImpl, which is what DbStatService registers. Sizes come from a file-tree
 * walk and reflect whatever files are present (including not-yet-compacted garbage), unlike the
 * per-level live SST accounting behind tron:db_size_bytes for chainbase stores.
 */
@Slf4j(topic = "metrics")
public final class ArchiveDbSizeMetrics {

  private static final long PERIOD_MINUTES = 60;

  private ArchiveDbSizeMetrics() {
  }

  public static void register(Path directory, String engine, String dbName) {
    if (!Metrics.enabled()) {
      return;
    }
    ScheduledExecutorService executor = ExecutorServiceManager.newSingleThreadScheduledExecutor(
        "archive-db-size-" + dbName, true);
    executor.scheduleWithFixedDelay(
        () -> report(directory, engine, dbName), 1, PERIOD_MINUTES, TimeUnit.MINUTES);
  }

  static void report(Path directory, String engine, String dbName) {
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    try {
      long size = directorySize(directory);
      Metrics.gaugeSet(MetricKeys.Gauge.DB_SIZE_BYTES, size, engine, dbName, "all");
      logger.debug("Archive DB {} size {} bytes", dbName, size);
    } catch (IOException e) {
      logger.warn("Archive DB {} size stats error", dbName, e);
    }
  }

  static long directorySize(Path directory) throws IOException {
    AtomicLong size = new AtomicLong();
    try (Stream<Path> files = Files.walk(directory)) {
      files.filter(Files::isRegularFile).forEach(file -> {
        try {
          size.addAndGet(Files.size(file));
        } catch (IOException e) {
          logger.warn("Archive DB size: skip unreadable file {}", file, e);
        }
      });
    }
    return size.get();
  }
}
