package org.tron.core.db2.core;

import io.prometheus.client.Counter;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.prometheus.Metrics;

/** Thread-confined successful transaction-loop attempts; never canonical/durable counters. */
@Slf4j(topic = "DB")
public final class ExecutionAttribution implements AutoCloseable {
  private static final ThreadLocal<ExecutionAttribution> CURRENT = new ThreadLocal<>();
  private final long head;
  private final String hash;
  private final Map<String, long[]> stages = new LinkedHashMap<>();
  private final Map<String, long[]> reads = new LinkedHashMap<>();
  private final Map<Phase, Map<String, long[]>> phaseReads = new EnumMap<>(Phase.class);
  private Phase phase = Phase.OTHER;
  private boolean published;

  public enum Phase {
    OTHER, VALIDATE, TRACE_INIT, BANDWIDTH, MULTISIGN, MEMO, RUNTIME, FINALIZATION, RESULT, CALLBACK;

    private final String label = name().toLowerCase(Locale.ROOT);
  }

  public static long start(Phase phase) {
    ExecutionAttribution current = CURRENT.get();
    if (current == null) {
      return 0;
    }
    current.phase = phase;
    return System.nanoTime();
  }

  public static void phase(Phase phase) {
    ExecutionAttribution current = CURRENT.get();
    if (current != null) {
      current.phase = phase;
    }
  }

  private ExecutionAttribution(long head, String hash) {
    this.head = head;
    this.hash = hash;
  }

  public static ExecutionAttribution open(long head, String hash) {
    if (!Boolean.getBoolean("tron.chainbase.executionAttribution")) {
      return null;
    }
    if (CURRENT.get() != null) {
      throw new IllegalStateException("Nested execution attribution");
    }
    ExecutionAttribution result = new ExecutionAttribution(head, hash);
    CURRENT.set(result);
    return result;
  }

  public static long start() {
    return CURRENT.get() == null ? 0 : System.nanoTime();
  }

  public static void stage(String stage, long start) {
    ExecutionAttribution current = CURRENT.get();
    if (current != null && start != 0) {
      long[] row = current.stages.computeIfAbsent(stage, ignored -> new long[2]);
      row[0]++;
      row[1] += System.nanoTime() - start;
    }
  }

  /** Counts all calls, times calls 1,65,... independently per store/operation/attempt. */
  public static long sample(String store, String operation) {
    ExecutionAttribution current = CURRENT.get();
    if (current == null || !selected(store)) {
      return 0;
    }
    String key = store + ":" + operation;
    long[] row = current.reads.computeIfAbsent(key, ignored -> new long[5]);
    current.phaseReads.computeIfAbsent(current.phase, ignored -> new LinkedHashMap<>())
        .computeIfAbsent(key, ignored -> new long[5])[0]++;
    return (row[0]++ & 63) == 0 ? System.nanoTime() : 0;
  }

  public static void sampled(String store, String operation, long start, int layers,
      boolean hit) {
    if (start == 0) {
      return;
    }
    ExecutionAttribution current = CURRENT.get();
    if (current != null) {
      String key = store + ":" + operation;
      long elapsed = System.nanoTime() - start;
      finish(current.reads.get(key), elapsed, layers, hit);
      finish(current.phaseReads.get(current.phase).get(key), elapsed, layers, hit);
    }
  }

  private static void finish(long[] row, long nanos, int layers, boolean hit) {
    row[1]++;
    row[2] += nanos;
    row[3] += layers;
    row[4] += hit ? 1 : 0;
  }

  private static boolean selected(String store) {
    return "account".equals(store) || "account-asset".equals(store)
        || "storage-row".equals(store) || "properties".equals(store)
        || "contract".equals(store) || "code".equals(store);
  }

  public void publish() {
    if (published) {
      return;
    }
    published = true;
    stages.forEach((name, row) -> {
      logger.info("Chainbase execution stage: head={}, blockHash={}, status=executed, "
          + "stage={}, calls={}, nanos={}", head, hash, name, row[0], row[1]);
      if (Metrics.enabled()) {
        Export.STAGES.labels(name, "calls").inc(row[0]);
        Export.STAGES.labels(name, "nanos").inc(row[1]);
      }
    });
    reads.forEach((name, row) -> {
      String[] parts = name.split(":");
      logger.info("Chainbase execution read: head={}, blockHash={}, status=executed, "
          + "store={}, operation={}, calls={}, samples={}, nanos={}, layers={}, hits={}",
          head, hash, parts[0], parts[1], row[0], row[1], row[2], row[3], row[4]);
      if (Metrics.enabled()) {
        String[] kinds = {"calls", "samples", "nanos", "layers", "hits"};
        for (int i = 0; i < kinds.length; i++) {
          Export.READS.labels(parts[0], parts[1], kinds[i]).inc(row[i]);
        }
      }
    });
    phaseReads.forEach((phase, operations) -> operations.forEach((name, row) -> {
      String[] parts = name.split(":");
      logger.info("Chainbase execution phase read: head={}, blockHash={}, status=executed, "
          + "phase={}, store={}, operation={}, calls={}, samples={}, nanos={}, layers={}, hits={}",
          head, hash, phase.label, parts[0], parts[1], row[0], row[1], row[2], row[3], row[4]);
      if (Metrics.enabled()) {
        String[] kinds = {"calls", "samples", "nanos", "layers", "hits"};
        for (int i = 0; i < kinds.length; i++) {
          Export.PHASE_READS.labels(phase.label, parts[0], parts[1], kinds[i]).inc(row[i]);
        }
      }
    }));
  }

  public static void checkpoint(CommonCheckpointTarget target, String store, int entries,
      long nanos) {
    logger.info("Chainbase checkpoint store: head={}, blockHash={}, store={}, entries={}, nanos={}",
        target.getLastBlock().getBlockNumber(),
        org.tron.common.utils.ByteArray.toHexString(target.getLastBlock().getBlockHash()),
        store, entries, nanos);
    if (Metrics.enabled()) {
      Export.CHECKPOINT.labels(store, "calls").inc();
      Export.CHECKPOINT.labels(store, "entries").inc(entries);
      Export.CHECKPOINT.labels(store, "nanos").inc(nanos);
    }
  }

  @Override
  public void close() {
    CURRENT.remove();
  }

  private static final class Export {
    private static final Counter CHECKPOINT = Counter.build()
        .name("tron_chainbase_checkpoint_store_total")
        .help("Completed store batch attempts; not checkpoint publication or physical IO bytes.")
        .labelNames("store", "kind").register();
    private static final Counter STAGES = Counter.build()
        .name("tron_chainbase_execution_stage_total")
        .help("Completed transaction-loop attempts; calls or nanoseconds, not durable blocks.")
        .labelNames("stage", "kind").register();
    private static final Counter PHASE_READS = Counter.build()
        .name("tron_chainbase_execution_phase_read_total")
        .help("Partition of loop read counters by caller phase; sampled time is not total IO.")
        .labelNames("phase", "store", "operation", "kind").register();
    private static final Counter READS = Counter.build()
        .name("tron_chainbase_execution_read_total")
        .help("Loop-thread reads; sampled nanoseconds/layers/hits, not physical disk IO.")
        .labelNames("store", "operation", "kind").register();
  }
}
