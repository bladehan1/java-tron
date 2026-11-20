package org.tron.core.db2.stateroot.storage;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM-wide shared RocksDB block cache for PathState stores (M3 cache-allocation model,
 * design/ai/stateroot/cache-allocation-model-20261116.md). When configured, every PathState
 * store attaches to one pool instead of owning a per-store LRUCache.
 *
 * <p>Purpose: the pool turns ~28 per-store block-cache budgets into a single fitting knob, so a
 * static memory budget can be enforced up front (the only reclaim path this stack really has).
 * Note: dynamic shrink is NOT available — the RocksDB Java binding does not expose
 * {@code Cache::SetCapacity} (verified absent in both 5.15.10 and 9.7.4), so capacity is fixed
 * at construction and the pool must be sized correctly from the start.
 */
public final class PathStateSharedBlockCache {

  private static final Logger logger = LoggerFactory.getLogger(PathStateSharedBlockCache.class);
  private static final Object LOCK = new Object();

  private static Cache handle;
  private static long capacityBytes;

  private PathStateSharedBlockCache() {
  }

  /**
   * Returns the shared handle, creating it on first use. All requests in one JVM must
   * use the same capacity: the RocksDB Java binding cannot resize a live cache
   * (Cache::SetCapacity is not exposed), so a mismatch fails fast instead of being
   * silently ignored. The first-created capacity wins by construction.
   */
  public static Cache acquire(long totalBytes, int shardBits) {
    if (totalBytes <= 0) {
      throw new IllegalArgumentException("shared block cache bytes must be positive");
    }
    synchronized (LOCK) {
      if (handle == null) {
        org.rocksdb.RocksDB.loadLibrary();
        handle = shardBits < 0 ? new LRUCache(totalBytes)
            : new LRUCache(totalBytes, shardBits, false);
        capacityBytes = totalBytes;
        logger.info("Path-state shared block cache created: capacityBytes={} shardBits={}",
            totalBytes, shardBits);
      } else if (capacityBytes != totalBytes) {
        throw new IllegalStateException("Inconsistent path-state shared block cache capacity "
            + "request " + totalBytes + "; live cache is fixed at " + capacityBytes
            + " (RocksDB Java binding cannot resize). Fix the sharedCacheSize configuration.");
      }
      return handle;
    }
  }

  public static boolean isActive() {
    synchronized (LOCK) {
      return handle != null;
    }
  }

  public static long capacityBytes() {
    synchronized (LOCK) {
      return capacityBytes;
    }
  }

  /** Closes the handle. Test-only: production lifetime is the JVM. */
  static void resetForTest() {
    synchronized (LOCK) {
      if (handle != null) {
        handle.close();
        handle = null;
        capacityBytes = 0;
      }
    }
  }
}
