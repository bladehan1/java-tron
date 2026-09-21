package org.tron.core.db2.common;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.WriteOptionsWrapper;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;

public class LevelDB implements DB<byte[], byte[]>, Flusher, SnapshotCapableStore {

  @Getter
  private LevelDbDataSourceImpl db;
  private final WriteOptionsWrapper writeOptions = WriteOptionsWrapper.getInstance()
      .sync(CommonParameter.getInstance().getStorage().isDbSync());

  public LevelDB(LevelDbDataSourceImpl db) {
    this.db = db;
  }

  @Override
  public byte[] get(byte[] key) {
    return db.getData(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    db.putData(key, value);
  }

  @Override
  public long size() {
    return db.getTotal();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public void remove(byte[] key) {
    db.deleteData(key);
  }

  @Override
  public String getDbName() {
    return db.getDBName();
  }

  @Override
  public String getSourceIdentity() {
    return db.getSnapshotSourceIdentity();
  }

  @Override
  public StoreSnapshot pin(long blockNumber, byte[] blockHash) {
    if (blockNumber < 0 || blockHash == null || blockHash.length != 32) {
      throw new IllegalArgumentException("Invalid LevelDB snapshot block identity");
    }
    LevelDbDataSourceImpl.PinnedSnapshot pinned = db.pinSnapshot();
    byte[] expectedHash = Arrays.copyOf(blockHash, blockHash.length);
    return new StoreSnapshot() {
      @Override
      public String getDbName() {
        return LevelDB.this.getDbName();
      }

      @Override
      public String getSourceIdentity() {
        return pinned.getSourceIdentity();
      }

      @Override
      public long getBlockNumber() {
        return blockNumber;
      }

      @Override
      public byte[] getBlockHash() {
        return Arrays.copyOf(expectedHash, expectedHash.length);
      }

      @Override
      public byte[] get(byte[] physicalRawKey) {
        return pinned.get(physicalRawKey);
      }

      @Override
      public List<Map.Entry<byte[], byte[]>> range(byte[] lowerInclusive,
          byte[] upperExclusive, int maxEntries) {
        return pinned.range(lowerInclusive, upperExclusive, maxEntries);
      }

      @Override
      public void close() throws IOException {
        pinned.close();
      }
    };
  }

  @Override
  public DBIterator iterator() {
    return db.iterator();
  }

  @Override
  public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
    flush(batch, writeOptions);
  }

  @Override
  public void flushSynced(Map<WrappedByteArray, WrappedByteArray> batch) {
    try (WriteOptionsWrapper synced = WriteOptionsWrapper.getInstance().sync(true)) {
      flush(batch, synced);
    }
  }

  private void flush(Map<WrappedByteArray, WrappedByteArray> batch,
      WriteOptionsWrapper options) {
    Map<byte[], byte[]> rows = batch.entrySet().stream()
        .map(e -> Maps.immutableEntry(e.getKey().getBytes(), e.getValue().getBytes()))
        .collect(HashMap::new, (m, k) -> m.put(k.getKey(), k.getValue()), HashMap::putAll);
    db.updateByBatch(rows, options);
  }

  @Override
  public void close() {
    this.writeOptions.close();
    db.closeDB();
  }

  @Override
  public void reset() {
    db.resetDb();
  }

  @Override
  public LevelDB newInstance() {
    return new LevelDB(db.newInstance());
  }

  @Override
  public void stat() {
    this.db.stat();
  }
}
