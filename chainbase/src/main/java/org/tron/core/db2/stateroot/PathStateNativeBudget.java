package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.config.args.StorageConfig.PathStateDbSettingsConfig;

/**
 * Native memory upper bounds of the 28 PathState databases for the closed budget (design v2
 * ch08 §3.8.1, E9, WP8-7). Same store/profile mapping and override table as the open path.
 */
public final class PathStateNativeBudget {

  /** Per-database upper bounds in bytes. */
  public static final class Entry {
    private final int storeId;
    private final String profile;
    private final long cacheBytes;
    private final long memtableBytes;
    private final long pinnedBytes;

    Entry(int storeId, String profile, long cacheBytes, long memtableBytes, long pinnedBytes) {
      this.storeId = storeId;
      this.profile = profile;
      this.cacheBytes = cacheBytes;
      this.memtableBytes = memtableBytes;
      this.pinnedBytes = pinnedBytes;
    }

    public int getStoreId() {
      return storeId;
    }

    public String getProfile() {
      return profile;
    }

    public long getCacheBytes() {
      return cacheBytes;
    }

    public long getMemtableBytes() {
      return memtableBytes;
    }

    /** Pinned metadata P_pin (cannot be evicted; may exceed the cache capacity). */
    public long getPinnedBytes() {
      return pinnedBytes;
    }
  }

  private PathStateNativeBudget() {
  }

  /**
   * Entries for the 27 participants and the super store. {@code largeReadCacheStores} adds the
   * benchmark-only 256 MiB caches of stores 5 and 22 (counted, never hidden).
   */
  public static List<Entry> entries(PathStateDbSettingsConfig settings, boolean rocksdb,
      boolean largeReadCacheBenchmark) {
    List<Entry> entries = new ArrayList<>();
    for (PathStateParticipantDescriptor.StoreIdentity store
        : PathStateParticipantDescriptor.current().getStores()) {
      String profile = PathStatePhysicalStoreSet.storageProfileNameFor(store.getDbName());
      NativeDbConfig config = "giant".equals(profile) ? settings.getGiant()
          : "large".equals(profile) ? settings.getLarge() : settings.getSmall();
      boolean largeCache = PathStatePhysicalStoreSet.useLargeReadCache(store.getStoreId(),
          rocksdb ? PathStateStoreManifest.Engine.ROCKSDB : PathStateStoreManifest.Engine.LEVELDB,
          largeReadCacheBenchmark);
      entries.add(entry(store.getStoreId(), profile, config, rocksdb,
          largeCache ? 256L << 20 : config.getCacheSize()));
    }
    entries.add(entry(0, "small", settings.getSmall(), rocksdb,
        settings.getSmall().getCacheSize()));
    return Collections.unmodifiableList(entries);
  }

  static Entry entry(int storeId, String profile, NativeDbConfig config, boolean rocksdb,
      long cacheBytes) {
    if (!rocksdb) {
      // LevelDB keeps mem_ and imm_ alive at the same time: 2 x write_buffer_size.
      return new Entry(storeId, profile, cacheBytes, 2L * config.getWriteBufferSize(), 0L);
    }
    long memtable = (long) config.getWriteBufferSize() * config.getMaxWriteBufferNumber();
    // pin_l0_filter_and_index_blocks_in_cache=true: up to level0StopWritesTrigger L0 files.
    long pinned = config.getLevel0StopWritesTrigger() * l0MetadataBytes(config);
    return new Entry(storeId, profile, cacheBytes, memtable, pinned);
  }

  /**
   * Index plus filter of one flushed L0 file (~write_buffer_size of data): filter
   * {@code bitsPerKey / 8} per entry at about 100 B per entry, index about 16 B per data block.
   * 64 MiB at 10 bits/key and 4 KiB blocks gives about 1 MiB (§3.8.1 example).
   */
  static long l0MetadataBytes(NativeDbConfig config) {
    long entries = config.getWriteBufferSize() / 100L;
    long filter = entries * config.getBloomBitsPerKey() / 8L;
    long index = config.getWriteBufferSize() / Math.max(1, config.getBlockSize()) * 16L;
    return filter + index;
  }
}
