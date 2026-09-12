package org.tron.core.db2.stateroot;

import java.util.concurrent.atomic.AtomicLong;

/** Optional systematic 1/64 samples. Durations are sampled work, never estimated wall time. */
final class PathStateOperationTimer {
  private final AtomicLong calls = new AtomicLong();
  private final AtomicLong samples = new AtomicLong();
  private final AtomicLong nanos = new AtomicLong();

  static boolean enabled() {
    return Boolean.getBoolean("tron.pathstate.attribution");
  }

  static boolean observesStore(int storeId) {
    return storeId == 4 || storeId == 5 || storeId == 22;
  }

  long start() {
    return ((calls.getAndIncrement() & 63) == 0) ? System.nanoTime() : 0;
  }

  void finish(long started) {
    if (started != 0) {
      nanos.addAndGet(System.nanoTime() - started);
      samples.incrementAndGet();
    }
  }

  long[] snapshot() {
    // Read only after the owning participant workers have joined.
    return new long[]{calls.get(), samples.get(), nanos.get()};
  }
}
