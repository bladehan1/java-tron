package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class StateArchiveServingCacheTest {
  private static final long CACHE_BYTES = 32L * 1024 * 1024;
  private static final int KEYS = 600_000;
  private static final int READS = 200;
  private static final byte[] VALUE = {1, 2, 3};

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void retainsLargeSstFilterAcrossReadsAndReopen() throws Exception {
    RocksDB.loadLibrary();
    Path directory = temporaryFolder.newFolder().toPath();
    try (BloomFilter filter = new BloomFilter(10, false);
        Options options = options(filter).setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, directory.toString());
        WriteOptions writes = new WriteOptions();
        FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
      for (int first = 0; first < KEYS; first += 1000) {
        try (WriteBatch batch = new WriteBatch()) {
          for (int i = first; i < first + 1000; i++) {
            batch.put(key(i), VALUE);
          }
          db.write(writes, batch);
        }
      }
      db.flush(flush);
      db.compactRange();
    }
    Observation baseline = read(directory, false);
    Observation candidate = read(directory, true);
    Observation reopened = read(directory, true);
    // Counter oracles, not wall-time thresholds: the same SST and keys are used each time.
    assertTrue("Control must reproduce repeated filter misses", baseline.misses >= READS - 2);
    assertTrue("Filter must exceed the old 512KiB shard budget",
        baseline.insertedBytes > baseline.misses * (CACHE_BYTES / 64));
    assertTrue("Candidate should load the filter at most twice", candidate.misses <= 2);
    assertTrue("Candidate should retain the filter", candidate.hits >= READS - 2);
    assertTrue("Reopening must also retain the filter", reopened.hits >= READS - 2);
  }

  private static Options options(BloomFilter filter) {
    return new Options().setDisableAutoCompactions(true).setWriteBufferSize(64L * 1024 * 1024)
        .setTableFormatConfig(new BlockBasedTableConfig().setBlockSize(4096)
            .setFilter(filter).setWholeKeyFiltering(true).setCacheIndexAndFilterBlocks(true)
            .setPinL0FilterAndIndexBlocksInCache(false));
  }

  private static Observation read(Path directory, boolean candidate) throws Exception {
    try (LRUCache cache = candidate ? StateArchiveIndexDatabase.newServingBlockCache(CACHE_BYTES)
        : new LRUCache(CACHE_BYTES, 6, false);
        BloomFilter filter = new BloomFilter(10, false);
        Statistics statistics = new Statistics();
        Options options = options(filter).setStatistics(statistics)) {
      ((BlockBasedTableConfig) options.tableFormatConfig()).setBlockCache(cache);
      options.setTableFormatConfig(options.tableFormatConfig());
      // Runtime lookups use the writer; keep its open path and counter reporting here.
      try (RocksDB db = RocksDB.open(options, directory.toString())) {
        long misses = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);
        long hits = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT);
        long bytes = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT);
        long started = System.nanoTime();
        for (int i = 0; i < READS; i++) {
          assertArrayEquals(VALUE, db.get(key(KEYS / 2 + i % 8)));
        }
        Observation result = new Observation(
            statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS) - misses,
            statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT) - hits,
            statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT) - bytes);
        System.out.println("serving-cache candidate=" + candidate + " misses=" + result.misses
            + " hits=" + result.hits + " filterInsertedBytes=" + result.insertedBytes
            + " readUs=" + (System.nanoTime() - started) / 1000);
        return result;
      }
    }
  }

  private static byte[] key(int i) {
    return ByteBuffer.allocate(Long.BYTES).putLong(i).array();
  }

  private static final class Observation {
    private final long misses;
    private final long hits;
    private final long insertedBytes;

    private Observation(long misses, long hits, long insertedBytes) {
      this.misses = misses;
      this.hits = hits;
      this.insertedBytes = insertedBytes;
    }
  }
}
