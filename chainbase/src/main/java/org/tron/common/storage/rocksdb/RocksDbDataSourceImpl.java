package org.tron.common.storage.rocksdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.prometheus.client.Histogram;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Status;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.tron.common.error.TronDBException;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.storage.WriteOptionsWrapper;
import org.tron.common.storage.metric.DbStat;
import org.tron.common.storage.metric.DbOperationMetrics;
import org.tron.common.utils.FileUtil;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db.common.iterator.RockStoreIterator;
import org.tron.core.db2.common.Instance;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.exception.TronError;


@Slf4j(topic = "DB")
@NoArgsConstructor
public class RocksDbDataSourceImpl extends DbStat implements DbSourceInter<byte[]>,
    Iterable<Map.Entry<byte[], byte[]>>, Instance<RocksDbDataSourceImpl> {

  private String dataBaseName;
  private RocksDB database;
  private volatile boolean alive;
  private String parentPath;
  private ReadWriteLock resetDbLock = new ReentrantReadWriteLock();
  private Options options;
  private Statistics statistics;
  private DbOperationMetrics dbOperationMetrics;
  private final Map<TickerType, Long> tickerSnapshots = new EnumMap<>(TickerType.class);

  public RocksDbDataSourceImpl(String parentPath, String name) {
    this.dataBaseName = name;
    this.parentPath = parentPath;
    this.dbOperationMetrics = DbOperationMetrics.create(ROCKSDB, name);
    initDB();
  }

  public Path getDbPath() {
    return Paths.get(parentPath, dataBaseName);
  }

  public RocksDB getDatabase() {
    return database;
  }

  public boolean isAlive() {
    return alive;
  }

  @Override
  public void closeDB() {
    resetDbLock.writeLock().lock();
    try {
      if (!isAlive()) {
        return;
      }
      database.close();
      if (this.statistics != null) {
        this.statistics.close();
        this.statistics = null;
      }
      if (this.options != null) {
        this.options.close();
        this.options = null;
      }
      alive = false;
    } catch (Exception e) {
      logger.error("Failed to find the dbStore file on the closeDB: {}.", dataBaseName, e);
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  @Override
  public void resetDb() {
    resetDbLock.writeLock().lock();
    try {
      closeDB();
      FileUtil.recursiveDelete(getDbPath().toString());
      initDB();
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  private void throwIfNotAlive() {
    if (!isAlive()) {
      throw new TronDBException("DB " + this.getDBName() + " is closed.");
    }
  }

  /** copy from {@link org.fusesource.leveldbjni.internal#checkArgNotNull} */
  private static void checkArgNotNull(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException("The " + name + " argument cannot be null");
    }
  }

  @Deprecated
  @VisibleForTesting
  @Override
  public Set<byte[]> allKeys() throws RuntimeException {
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      Set<byte[]> result = Sets.newHashSet();
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        result.add(iter.key());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Deprecated
  @VisibleForTesting
  @Override
  public Set<byte[]> allValues() throws RuntimeException {
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      Set<byte[]> result = Sets.newHashSet();
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        result.add(iter.value());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Deprecated
  @VisibleForTesting
  @Override
  public long getTotal() throws RuntimeException {
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      long total = 0;
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        total++;
      }
      return total;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public String getDBName() {
    return this.dataBaseName;
  }

  @Override
  public void setDBName(String name) {
    this.dataBaseName = name;
  }

  private void initDB() {
    resetDbLock.writeLock().lock();
    try {
      if (isAlive()) {
        return;
      }
      if (dataBaseName == null) {
        throw new IllegalArgumentException("No name set to the dbStore");
      }

      try {
        logger.debug("Opening database {}.", dataBaseName);
        final Path dbPath = getDbPath();

        if (!Files.isSymbolicLink(dbPath.getParent())) {
          Files.createDirectories(dbPath.getParent());
        }

        try {
          DbSourceInter.checkOrInitEngine(getEngine(), dbPath.toString(),
              TronError.ErrCode.ROCKSDB_INIT);
          this.options = RocksDbSettings.getOptionsByDbName(dataBaseName);
          tickerSnapshots.clear();
          database = RocksDB.open(this.options, dbPath.toString());
          statistics = this.options.statistics();
        } catch (RocksDBException e) {
          if (Objects.equals(e.getStatus().getCode(), Status.Code.Corruption)) {
            logger.error("Database {} corrupted, please delete database directory({}) "
                + "and restart.", dataBaseName, parentPath, e);
          } else {
            logger.error("Open Database {} failed", dataBaseName, e);
          }

          if (this.options != null) {
            this.options.close();
          }
          throw new TronError(e, TronError.ErrCode.ROCKSDB_INIT);
        }

        alive = true;
      } catch (IOException ioe) {
        throw new RuntimeException(
            String.format("failed to init database: %s", dataBaseName), ioe);
      }

      logger.debug("Init DB {} done.", dataBaseName);
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  @Override
  public void putData(byte[] key, byte[] value) {
    int size = value.length;
    resetDbLock.readLock().lock();
    try (Histogram.Timer timer = dbOperationMetrics.startPut()) {
      throwIfNotAlive();
      checkArgNotNull(key, "key");
      checkArgNotNull(value, "value");
      database.put(key, value);
    } catch (RocksDBException e) {
      throw new RuntimeException(dataBaseName, e);
    } finally {
      resetDbLock.readLock().unlock();
    }
    dbOperationMetrics.observePutBytes(size);
  }

  @Override
  public byte[] getData(byte[] key) {
    resetDbLock.readLock().lock();
    byte[] value;
    try (Histogram.Timer timer = dbOperationMetrics.startGet()) {
      throwIfNotAlive();
      checkArgNotNull(key, "key");
      value = database.get(key);
    } catch (RocksDBException e) {
      throw new RuntimeException(dataBaseName, e);
    } finally {
      resetDbLock.readLock().unlock();
    }
    if (value != null) {
      dbOperationMetrics.observeGetBytes(value.length);
    }
    return value;
  }

  @Override
  public void deleteData(byte[] key) {
    resetDbLock.readLock().lock();
    try (Histogram.Timer timer = dbOperationMetrics.startDelete()) {
      throwIfNotAlive();
      checkArgNotNull(key, "key");
      database.delete(key);
    } catch (RocksDBException e) {
      throw new RuntimeException(dataBaseName, e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public boolean flush() {
    return false;
  }

  /**
   * Returns an iterator over the database.
   *
   * <p><b>CRITICAL:</b> The returned iterator holds native resources and <b>MUST</b> be closed
   * after use to prevent memory leaks. It is strongly recommended to use a try-with-resources
   * statement.
   *
   * <p>Example of correct usage:
   * <pre>{@code
   * try (DBIterator iterator = db.iterator()) {
   *   while (iterator.hasNext()) {
   *     // ... process entry
   *   }
   * }
   * }</pre>
   *
   * @return a new database iterator that must be closed.
   */
  @Override
  public org.tron.core.db.common.iterator.DBIterator iterator() {
    ReadOptions readOptions = getReadOptions();
    return new RockStoreIterator(getRocksIterator(readOptions), readOptions);
  }

  private void updateByBatchInner(Map<byte[], byte[]> rows, WriteOptions options)
      throws Exception {
    try (WriteBatch batch = new WriteBatch()) {
      for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
        checkArgNotNull(entry.getKey(), "key");
        if (entry.getValue() == null) {
          batch.delete(entry.getKey());
        } else {
          batch.put(entry.getKey(), entry.getValue());
        }
      }
      throwIfNotAlive();
      database.write(options, batch);
    }
  }

  @Override
  public void updateByBatch(Map<byte[], byte[]> rows, WriteOptionsWrapper optionsWrapper) {
    this.updateByBatch(rows, optionsWrapper.rocks);
  }

  @Override
  public void updateByBatch(Map<byte[], byte[]> rows) {
    try (WriteOptions writeOptions = new WriteOptions()) {
      this.updateByBatch(rows, writeOptions);
    }
  }

  private void updateByBatch(Map<byte[], byte[]> rows, WriteOptions options) {
    long totalBytes = dbOperationMetrics.enabled()
        ? rows.values().stream().filter(v -> v != null).mapToLong(v -> v.length).sum() : 0;
    resetDbLock.readLock().lock();
    try (Histogram.Timer timer = dbOperationMetrics.startBatch()) {
      updateByBatchInner(rows, options);
    } catch (Exception e) {
      try {
        updateByBatchInner(rows, options);
      } catch (Exception e1) {
        throw new RuntimeException(dataBaseName, e1);
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
    dbOperationMetrics.observeBatchBytes(totalBytes);
  }

  public List<byte[]> getKeysNext(byte[] key, long limit) {
    if (limit <= 0) {
      return new ArrayList<>();
    }
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      List<byte[]> result = new ArrayList<>();
      long i = 0;
      for (iter.seek(key); iter.isValid() && i < limit; iter.next(), i++) {
        result.add(iter.key());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Map<byte[], byte[]> getNext(byte[] key, long limit) {
    if (limit <= 0) {
      return Collections.emptyMap();
    }
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      Map<byte[], byte[]> result = new HashMap<>();
      long i = 0;
      for (iter.seek(key); iter.isValid() && i < limit; iter.next(), i++) {
        result.put(iter.key(), iter.value());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public Map<WrappedByteArray, byte[]> prefixQuery(byte[] key) {
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iterator = getRocksIterator(readOptions)) {
      Map<WrappedByteArray, byte[]> result = new HashMap<>();
      for (iterator.seek(key); iterator.isValid(); iterator.next()) {
        if (Bytes.indexOf(iterator.key(), key) == 0) {
          result.put(WrappedByteArray.of(iterator.key()), iterator.value());
        } else {
          return result;
        }
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Set<byte[]> getlatestValues(long limit) {
    if (limit <= 0) {
      return Sets.newHashSet();
    }
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      Set<byte[]> result = Sets.newHashSet();
      long i = 0;
      for (iter.seekToLast(); iter.isValid() && i < limit; iter.prev(), i++) {
        result.add(iter.value());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Set<byte[]> getValuesNext(byte[] key, long limit) {
    if (limit <= 0) {
      return Sets.newHashSet();
    }
    resetDbLock.readLock().lock();
    try (final ReadOptions readOptions = getReadOptions();
         final RocksIterator iter = getRocksIterator(readOptions)) {
      Set<byte[]> result = Sets.newHashSet();
      long i = 0;
      for (iter.seek(key); iter.isValid() && i < limit; iter.next(), i++) {
        result.add(iter.value());
      }
      return result;
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public void backup(String dir) throws RocksDBException {
    throwIfNotAlive();
    try (Checkpoint cp = Checkpoint.create(database)) {
      cp.createCheckpoint(dir + this.getDBName());
    }
  }

  /**
   * Returns an iterator over the database.
   *
   * <p><b>CRITICAL:</b> The returned iterator holds native resources and <b>MUST</b> be closed
   * after use to prevent memory leaks. It is strongly recommended to use a try-with-resources
   * statement.
   *
   * <p>Example of correct usage:
   * <pre>{@code
   * try ( ReadOptions readOptions = new ReadOptions().setFillCache(false);
   *      RocksIterator iterator = getRocksIterator(readOptions)) {
   *      iterator.seekToFirst();
   *  // do something
   * }
   * }</pre>
   *
   * @return a new database iterator that must be closed.
   */
  private RocksIterator getRocksIterator(ReadOptions readOptions) {
    throwIfNotAlive();
    return database.newIterator(readOptions);
  }

  /**
   * Returns an ReadOptions.
   *
   * <p><b>CRITICAL:</b> The returned ReadOptions holds native resources and <b>MUST</b> be closed
   * after use to prevent memory leaks. It is strongly recommended to use a try-with-resources
   * statement.
   *
   * <p>Example of correct usage:
   * <pre>{@code
   * try (ReadOptions readOptions = getReadOptions();
   *      RocksIterator iterator = getRocksIterator(readOptions)) {
   *      iterator.seekToFirst();
   *  // do something
   * }
   * }</pre>
   *
   * @return a new database iterator that must be closed.
   */
  private ReadOptions getReadOptions() {
    throwIfNotAlive();
    return new ReadOptions().setFillCache(false);
  }

  public boolean deleteDbBakPath(String dir) {
    return FileUtil.deleteDir(new File(dir + this.getDBName()));
  }

  @Override
  public RocksDbDataSourceImpl newInstance() {
    return new RocksDbDataSourceImpl(parentPath, dataBaseName);
  }



  /**
   * Level Files Size(MB)
   * --------------------
   *   0        5       10
   *   1      134      254
   *   2     1311     2559
   *   3     1976     4005
   *   4        0        0
   *   5        0        0
   *   6        0        0
   */
  @Override
  public List<String> getStats() throws Exception {
    resetDbLock.readLock().lock();
    try {
      if (!isAlive()) {
        return Collections.emptyList();
      }
      String stat = database.getProperty("rocksdb.levelstats");
      String[] stats = stat.split("\n");
      return Arrays.stream(stats).skip(2).collect(Collectors.toList());
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public String getEngine() {
    return ROCKSDB;
  }

  @Override
  public String getName() {
    return this.dataBaseName;
  }

  @Override public void stat() {
    this.statProperty();
    this.statMemory();
    this.statRocksProperties();
    this.statRocksTickers();
  }

  /**
   * RocksDB-only approximate memory usage (bytes), exposed as gauges labeled by property:
   * <ul>
   *   <li>{@code block-cache-usage} -- memory occupied by entries in the block cache</li>
   *   <li>{@code cur-size-all-mem-tables} -- size of active and unflushed immutable memtables</li>
   *   <li>{@code estimate-table-readers-mem} -- memory used by index and filter blocks</li>
   * </ul>
   */
  private static final String[] MEMORY_PROPERTIES = {
      "rocksdb.block-cache-usage",
      "rocksdb.cur-size-all-mem-tables",
      "rocksdb.estimate-table-readers-mem"
  };

  private static final String[] ROCKS_PROPERTIES = {
      "rocksdb.estimate-pending-compaction-bytes",
      "rocksdb.num-running-compactions",
      "rocksdb.num-running-flushes",
      "rocksdb.actual-delayed-write-rate",
      "rocksdb.is-write-stopped",
      "rocksdb.num-immutable-mem-table",
      "rocksdb.mem-table-flush-pending",
      "rocksdb.compaction-pending",
      "rocksdb.background-errors"
  };

  private static final TickerType[] ROCKS_TICKERS = {
      TickerType.BLOCK_CACHE_HIT,
      TickerType.BLOCK_CACHE_MISS,
      TickerType.BLOCK_CACHE_DATA_HIT,
      TickerType.BLOCK_CACHE_DATA_MISS,
      TickerType.BLOCK_CACHE_INDEX_HIT,
      TickerType.BLOCK_CACHE_INDEX_MISS,
      TickerType.BLOCK_CACHE_FILTER_HIT,
      TickerType.BLOCK_CACHE_FILTER_MISS,
      TickerType.BLOOM_FILTER_USEFUL,
      TickerType.MEMTABLE_HIT,
      TickerType.MEMTABLE_MISS,
      TickerType.GET_HIT_L0,
      TickerType.GET_HIT_L1,
      TickerType.GET_HIT_L2_AND_UP,
      TickerType.NUMBER_KEYS_READ,
      TickerType.NUMBER_KEYS_WRITTEN,
      TickerType.BYTES_READ,
      TickerType.BYTES_WRITTEN,
      TickerType.COMPACT_READ_BYTES,
      TickerType.COMPACT_WRITE_BYTES,
      TickerType.FLUSH_WRITE_BYTES,
      TickerType.STALL_MICROS
  };

  private void statMemory() {
    resetDbLock.readLock().lock();
    try {
      if (!isAlive()) {
        return;
      }
      for (String property : MEMORY_PROPERTIES) {
        try {
          long value = database.getLongProperty(property);
          Metrics.gaugeSet(MetricKeys.Gauge.DB_MEMORY_BYTES, value,
              getEngine(), getName(), property);
        } catch (RocksDBException e) {
          logger.warn("DB {} get property {} error", getName(), property, e);
        }
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  private void statRocksProperties() {
    resetDbLock.readLock().lock();
    try {
      if (!isAlive()) {
        return;
      }
      for (String property : ROCKS_PROPERTIES) {
        try {
          Metrics.gaugeSet(MetricKeys.Gauge.DB_ROCKSDB_PROPERTY,
              database.getLongProperty(property), getEngine(), getName(), property);
        } catch (RocksDBException e) {
          // Property support differs between the amd64 and aarch64 RocksDB JNI versions.
          logger.debug("DB {} does not expose property {}", getName(), property, e);
        }
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  private void statRocksTickers() {
    resetDbLock.readLock().lock();
    try {
      if (!isAlive() || statistics == null) {
        return;
      }
      for (TickerType ticker : ROCKS_TICKERS) {
        long current = statistics.getTickerCount(ticker);
        long previous = tickerSnapshots.getOrDefault(ticker, 0L);
        long delta = current >= previous ? current - previous : current;
        if (delta > 0) {
          Metrics.counterInc(MetricKeys.Counter.DB_ROCKSDB_TICKER, delta,
              getEngine(), getName(), ticker.name().toLowerCase(Locale.ENGLISH));
        }
        tickerSnapshots.put(ticker, current);
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
  }
}
