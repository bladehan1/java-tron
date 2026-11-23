package org.tron.core.db2.stateroot;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Config-gated diagnostic (storage.pathStateRoot.readTraceEnabled, TASK-049 Phase 0.2):
 * per-block histogram of the block-distance between repeated trie-node lookups, sizing
 * the geth-style diff-layer absorption opportunity. No instance exists unless enabled,
 * so the read path pays nothing by default.
 */
final class PathStateReadDistanceTrace {

  private static final int FIRST_TOUCH = 10;
  private static final int OVERFLOW = 11;
  private static final int BUCKET_COUNT = 12;

  private final ConcurrentHashMap<TraceKey, Long> lastTouchBlock;
  private final long maxKeys;
  private final LongAdder[] counts = new LongAdder[BUCKET_COUNT];
  private volatile long currentBlock;

  PathStateReadDistanceTrace(long maxKeys) {
    if (maxKeys <= 0) {
      throw new IllegalArgumentException("read trace maxKeys must be positive");
    }
    this.maxKeys = maxKeys;
    this.lastTouchBlock = new ConcurrentHashMap<>(1024);
    for (int index = 0; index < BUCKET_COUNT; index++) {
      counts[index] = new LongAdder();
    }
  }

  long maxKeys() {
    return maxKeys;
  }

  void beginBlock(long blockNumber) {
    currentBlock = blockNumber;
  }

  void touch(int storeId, byte[] path) {
    long now = currentBlock;
    TraceKey key = new TraceKey(storeId, path);
    Long previous = lastTouchBlock.put(key, now);
    if (previous == null) {
      if (lastTouchBlock.size() > maxKeys) {
        lastTouchBlock.remove(key);
        counts[OVERFLOW].increment();
      } else {
        counts[FIRST_TOUCH].increment();
      }
      return;
    }
    long distance = now >= previous ? now - previous : 0;
    counts[bucket(distance)].increment();
  }

  /** Buckets 0-2 are exact distances; 3-9 are power-of-two windows; 10 first-ever; 11 dropped. */
  private static int bucket(long distance) {
    if (distance <= 2) {
      return (int) distance;
    }
    if (distance <= 4) {
      return 3;
    }
    if (distance <= 8) {
      return 4;
    }
    if (distance <= 16) {
      return 5;
    }
    if (distance <= 32) {
      return 6;
    }
    if (distance <= 64) {
      return 7;
    }
    if (distance <= 128) {
      return 8;
    }
    return 9;
  }

  long[] endBlock() {
    long[] snapshot = new long[BUCKET_COUNT];
    for (int index = 0; index < BUCKET_COUNT; index++) {
      snapshot[index] = counts[index].sumThenReset();
    }
    return snapshot;
  }

  int trackedKeys() {
    return lastTouchBlock.size();
  }

  static final class TraceKey {

    private final int storeId;
    private final byte[] path;

    private TraceKey(int storeId, byte[] path) {
      this.storeId = storeId;
      this.path = Arrays.copyOf(Objects.requireNonNull(path, "path"), path.length);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof TraceKey)) {
        return false;
      }
      TraceKey that = (TraceKey) other;
      return storeId == that.storeId && Arrays.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return 31 * Objects.hash(storeId) + Arrays.hashCode(path);
    }
  }
}
