package org.tron.core.db2.stateroot.metrics;

/** Observation-only entry point; trie, store and publication ownership stay with their callers. */
public final class PathStateObservations {
  private PathStateObservations() {}

  public static boolean enabled() {
    return PathStateOperationTimer.enabled();
  }

  public static boolean observesStore(int storeId) {
    return PathStateOperationTimer.observesStore(storeId);
  }

  public static boolean observesRocksDb(int storeId) {
    return PathStateOperationTimer.observesRocksDb(storeId);
  }

  public static Timer timer() {
    return new PathStateOperationTimer();
  }

  public static void operation(
      int storeId, String operation, long calls, long samples, long nanos) {
    PathStateAttributionMetrics.operation(storeId, operation, calls, samples, nanos);
  }

  public static void cache(int storeId, String kind, long count) {
    PathStateAttributionMetrics.cache(storeId, kind, count);
  }

  public static void rocksDb(int storeId, String ticker, long delta) {
    PathStateAttributionMetrics.rocksDb(storeId, ticker, delta);
  }

  public static void nativeGetPerf(int storeId, String kind, long delta) {
    PathStateAttributionMetrics.nativeGetPerf(storeId, kind, delta);
  }

  public static void participant(int storeId, long submitToStartNanos, long workNanos) {
    PathStateAttributionMetrics.participant(storeId, submitToStartNanos, workNanos);
  }

  public static void prepared(long head) {
    PathStateAttributionMetrics.prepared(head);
  }

  /** The existing private timer implementation; sampling and snapshots retain their semantics. */
  public interface Timer {
    long start();

    void finish(long started);

    long[] snapshot();
  }
}
