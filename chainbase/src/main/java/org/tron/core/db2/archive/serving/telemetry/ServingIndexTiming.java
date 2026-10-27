package org.tron.core.db2.archive.serving.telemetry;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.EnumMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.prometheus.Metrics;
import org.tron.core.db2.archive.serving.status.ServingFailure;
import org.tron.core.db2.archive.serving.status.ServingState;

/** Batch-level observations; no key, hash or height is used as a metric label. */
public final class ServingIndexTiming implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger("DB");
  private static final ThreadLocal<ServingIndexTiming> CURRENT = new ThreadLocal<>();
  private final EnumMap<Stage, Long> nanos = new EnumMap<>(Stage.class);
  private final EnumMap<Stage, Long> calls = new EnumMap<>(Stage.class);
  private final long started = System.nanoTime();
  private final String mode;
  private final long from;
  private final long through;
  private boolean success;
  private long entries;
  private long uniqueKeys;
  private long writeBytes;

  public enum Stage {
    PLAN, DIGEST, GROUP, BUILD, META_GET, TAIL_GET, WRITE_SYNC
  }

  public ServingIndexTiming(String mode, long from, long through) {
    if (CURRENT.get() != null) {
      throw new IllegalStateException("Serving timing scopes must not overlap");
    }
    this.mode = mode;
    this.from = from;
    this.through = through;
    CURRENT.set(this);
  }

  public static void leaseRefreshed() {
    if (Metrics.enabled()) {
      Export.WORK.labels("service", "lease_refresh", "success").inc();
    }
  }

  public static void batch(String kind) {
    if (Metrics.enabled()) {
      Export.WORK.labels("service", "batch_" + kind, "success").inc();
    }
  }

  public static void failure(ServingFailure failure) {
    if (Metrics.enabled()) {
      Export.FAILURES.labels(failure.getReason().name(),
          failure.getKind().name().toLowerCase(java.util.Locale.ROOT)).inc();
    }
  }

  public static void serving(ServingObservation view) {
    if (Metrics.enabled()) {
      for (ServingState state : ServingState.values()) {
        Export.STATE.labels(state.name()).set(view.getState() == state ? 1 : 0);
      }
      Export.BACKLOG_BYTES.set(view.getBacklogBytes());
      Export.LAST_FAILURE.set(view.getLastFailure() == null ? 0
          : view.getLastFailure().getTimeMillis() / 1000.0);
      Export.HEIGHT.labels("indexed_from").set(view.getIndexedFrom());
      Export.HEIGHT.labels("indexed_through").set(view.getIndexedThrough());
      Export.HEIGHT.labels("committed_through").set(view.getCommittedThrough());
      Export.HEIGHT.labels("backlog_blocks").set(view.getBacklogBlocks());
      Export.HEIGHT.labels("ready").set(view.getState() == ServingState.LIVE ? 1 : 0);
    }
  }

  public static void record(Stage stage, long started) {
    ServingIndexTiming timing = CURRENT.get();
    if (timing != null) {
      timing.nanos.merge(stage, System.nanoTime() - started, Long::sum);
      timing.calls.merge(stage, 1L, Long::sum);
    }
  }

  public static void work(long entries, long uniqueKeys, long writeBytes) {
    ServingIndexTiming timing = CURRENT.get();
    if (timing != null) {
      timing.entries = entries;
      timing.uniqueKeys = uniqueKeys;
      timing.writeBytes = writeBytes;
    }
  }

  public void succeeded() {
    success = true;
  }

  long calls(Stage stage) {
    return calls.getOrDefault(stage, 0L);
  }

  @Override
  public void close() {
    CURRENT.remove();
    long elapsed = System.nanoTime() - started;
    // DIGEST is inside PLAN; GETs are inside BUILD. Do not sum parent and child times.
    LOGGER.info("Serving index batch: mode={}, from={}, through={}, success={}, entries={}, "
            + "uniqueKeys={}, writeBytes={}, totalUs={}, stageUs={}, stageCalls={}",
        mode, from, through, success, entries, uniqueKeys, writeBytes, elapsed / 1000,
        microseconds(), calls);
    if (Metrics.enabled()) {
      String result = success ? "success" : "failure";
      Export.DURATION.labels(mode, "total", result).observe(elapsed / 1e9);
      nanos.forEach((stage, value) -> Export.DURATION.labels(mode, stage.name(), result)
          .observe(value / 1e9));
      calls.forEach((stage, value) -> Export.WORK.labels(mode, stage.name(), result).inc(value));
      Export.WORK.labels(mode, "entries", result).inc(entries);
      Export.WORK.labels(mode, "unique_keys", result).inc(uniqueKeys);
      Export.WORK.labels(mode, "write_bytes", result).inc(writeBytes);
      if (success) {
        Export.WORK.labels(mode, "indexed_blocks", result).inc(through - from);
      }
    }
  }

  private EnumMap<Stage, Long> microseconds() {
    EnumMap<Stage, Long> result = new EnumMap<>(Stage.class);
    nanos.forEach((stage, value) -> result.put(stage, value / 1000));
    return result;
  }

  public static void source(long from, long through, long totalNanos, long decodeNanos,
      long bytes, long frames, boolean success) {
    LOGGER.info("Serving source read: from={}, through={}, success={}, encodedBytes={}, "
            + "frames={}, totalUs={}, decodeUs={}", from, through, success, bytes, frames,
        totalNanos / 1000, decodeNanos / 1000);
    if (Metrics.enabled()) {
      String result = success ? "success" : "failure";
      // Preserve the existing label for dashboards; P04 narrowed locking to view capture only.
      Export.DURATION.labels("source", "locked_read", result).observe(totalNanos / 1e9);
      Export.DURATION.labels("source", "decode", result).observe(decodeNanos / 1e9);
      Export.WORK.labels("source", "encoded_bytes", result).inc(bytes);
      Export.WORK.labels("source", "frames", result).inc(frames);
    }
  }

  public static void sourceCall(long started) {
    if (Metrics.enabled()) {
      // Each attempt includes published-view monitor acquisition and the request-scoped read.
      Export.DURATION.labels("source", "call", "all").observe(
          (System.nanoTime() - started) / 1e9);
    }
  }

  public static void progress(ProgressObservation progress) {
    if (Metrics.enabled()) {
      Export.HEIGHT.labels("indexed_from").set(progress.getIndexedFrom());
      Export.HEIGHT.labels("indexed_through").set(progress.getIndexedThrough());
      Export.HEIGHT.labels("committed_through").set(progress.getCommittedThrough());
      Export.HEIGHT.labels("backlog_blocks").set(progress.getIndexedThrough() < 0 ? -1
          : progress.getCommittedThrough() - progress.getIndexedThrough());
      Export.HEIGHT.labels("ready").set(progress.isLiveBackground()
          && progress.getIndexedThrough() == progress.getCommittedThrough() ? 1 : 0);
    }
  }

  /** Read-only observations implemented directly by the existing immutable published view. */
  public interface ServingObservation {
    ServingState getState();

    long getBacklogBytes();

    ServingFailure getLastFailure();

    long getIndexedFrom();

    long getIndexedThrough();

    long getCommittedThrough();

    long getBacklogBlocks();
  }

  /** Read-only progress; telemetry does not own the coordinator or its mode. */
  public interface ProgressObservation {
    long getIndexedFrom();

    long getIndexedThrough();

    long getCommittedThrough();

    boolean isLiveBackground();
  }

  private static final class Export {
    private static final Gauge STATE = Gauge.build().name("tron_archive_serving_state")
        .help("Current serving lifecycle state.").labelNames("state").register();
    private static final Gauge BACKLOG_BYTES = Gauge.build()
        .name("tron_archive_serving_backlog_bytes")
        .help("Committed source bytes pending indexing; -1 means unknown.").register();
    private static final Gauge LAST_FAILURE = Gauge.build()
        .name("tron_archive_serving_last_failure_timestamp_seconds")
        .help("Timestamp of the most recent serving failure.").register();
    private static final Counter FAILURES = Counter.build()
        .name("tron_archive_serving_failures_total")
        .help("Classified serving failures.").labelNames("reason", "class").register();
    private static final Histogram DURATION = Histogram.build()
        .name("tron_archive_serving_stage_seconds")
        .help("Serving batch/source duration; nested stages must not be summed.")
        .labelNames("mode", "stage", "result")
        .buckets(.001, .01, .1, .5, 1, 5, 15, 60, 300, 900).register();
    private static final Counter WORK = Counter.build().name("tron_archive_serving_work_total")
        .help("Serving measured work, including failed attempts.")
        .labelNames("mode", "kind", "result").register();
    private static final Gauge HEIGHT = Gauge.build().name("tron_archive_serving_progress")
        .help("Serving boundaries and readiness; -1 means unknown, not zero.")
        .labelNames("kind").register();
  }
}
