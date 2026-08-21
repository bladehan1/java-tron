package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

/** Persistent derived exact-key change index for the narrow historical account query. */
final class AccountChangeIndex implements Closeable {

  static {
    RocksDB.loadLibrary();
  }

  private static final byte DATA_PREFIX = 1;
  private static final byte[] HEAD_KEY = new byte[]{0, 'h', 'e', 'a', 'd'};
  private static final int ADDRESS_LENGTH = HistoricalAccountBalanceReader.ADDRESS_LENGTH;
  private static final int DATA_KEY_LENGTH = 1 + ADDRESS_LENGTH + Long.BYTES;

  private final Options options = new Options().setCreateIfMissing(true);
  private final RocksDB database;
  private final WriteOptions syncWrites = new WriteOptions().setSync(true);

  AccountChangeIndex(Path directory) throws IOException {
    try {
      database = RocksDB.open(options, directory.toString());
    } catch (RocksDBException failure) {
      throw new IOException("Failed to open account change index", failure);
    }
  }

  synchronized void apply(List<BlockReverseDiff> diffs) throws IOException {
    if (diffs.isEmpty()) {
      return;
    }
    long current = getIndexedThrough();
    BlockSnapshotMeta previous = null;
    try (WriteBatch batch = new WriteBatch()) {
      for (BlockReverseDiff diff : diffs) {
        BlockSnapshotMeta meta = diff.getMeta();
        if (current >= 0 && previous == null && meta.getEpoch() != current + 1) {
          throw new ArchivePersistenceException("Account index catch-up is not contiguous");
        }
        if (previous != null && meta.getEpoch() != previous.getEpoch() + 1) {
          throw new ArchivePersistenceException("Account index batch is not contiguous");
        }
        for (DbGroup group : diff.getGroups()) {
          if (!HistoricalAccountBalanceReader.ACCOUNT_DATABASE.equals(group.getDbName())) {
            continue;
          }
          for (Entry entry : group.getEntries()) {
            byte[] address = entry.getKey();
            if (address.length != ADDRESS_LENGTH) {
              continue;
            }
            batch.put(dataKey(address, meta.getEpoch()), new byte[]{1});
          }
        }
        previous = meta;
      }
      batch.put(HEAD_KEY, encodeHead(previous));
      database.write(syncWrites, batch);
    } catch (RocksDBException failure) {
      throw new IOException("Failed to update account change index", failure);
    }
  }

  synchronized void revert(BlockReverseDiff diff, BlockSnapshotMeta newHead) throws IOException {
    if (getIndexedThrough() != diff.getMeta().getEpoch()) {
      throw new ArchivePersistenceException("Account index revert does not target its head");
    }
    try (WriteBatch batch = new WriteBatch()) {
      for (DbGroup group : diff.getGroups()) {
        if (HistoricalAccountBalanceReader.ACCOUNT_DATABASE.equals(group.getDbName())) {
          for (Entry entry : group.getEntries()) {
            if (entry.getKey().length == ADDRESS_LENGTH) {
              batch.delete(dataKey(entry.getKey(), diff.getMeta().getEpoch()));
            }
          }
        }
      }
      if (newHead == null) {
        batch.delete(HEAD_KEY);
      } else {
        batch.put(HEAD_KEY, encodeHead(newHead));
      }
      database.write(syncWrites, batch);
    } catch (RocksDBException failure) {
      throw new IOException("Failed to revert account change index", failure);
    }
  }

  /** Truncates this derived index to the authoritative committed-history head. */
  synchronized void truncateAfter(BlockSnapshotMeta newHead) throws IOException {
    long target = newHead == null ? -1 : newHead.getEpoch();
    try (WriteBatch batch = new WriteBatch(); RocksIterator iterator = database.newIterator()) {
      iterator.seek(new byte[]{DATA_PREFIX});
      while (iterator.isValid()) {
        byte[] key = iterator.key();
        if (key.length != DATA_KEY_LENGTH || key[0] != DATA_PREFIX) {
          break;
        }
        long epoch = ByteBuffer.wrap(key, 1 + ADDRESS_LENGTH, Long.BYTES).getLong();
        if (epoch > target) {
          batch.delete(key);
        }
        iterator.next();
      }
      if (newHead == null) {
        batch.delete(HEAD_KEY);
      } else {
        batch.put(HEAD_KEY, encodeHead(newHead));
      }
      database.write(syncWrites, batch);
    } catch (RocksDBException failure) {
      throw new IOException("Failed to truncate account change index", failure);
    }
  }

  synchronized OptionalLong firstChangeAfter(byte[] address, long target, long upperBound)
      throws IOException {
    if (address == null || address.length != ADDRESS_LENGTH) {
      throw new IllegalArgumentException("TRON account address must be exactly 21 bytes");
    }
    if (target > upperBound || upperBound > getIndexedThrough()) {
      throw new IllegalArgumentException("Account query is outside index coverage");
    }
    if (target == Long.MAX_VALUE) {
      return OptionalLong.empty();
    }
    byte[] seek = dataKey(address, target + 1);
    try (RocksIterator iterator = database.newIterator()) {
      iterator.seek(seek);
      if (!iterator.isValid()) {
        return OptionalLong.empty();
      }
      byte[] key = iterator.key();
      if (key.length != DATA_KEY_LENGTH || key[0] != DATA_PREFIX
          || !Arrays.equals(address, Arrays.copyOfRange(key, 1, 1 + ADDRESS_LENGTH))) {
        return OptionalLong.empty();
      }
      long epoch = ByteBuffer.wrap(key, 1 + ADDRESS_LENGTH, Long.BYTES).getLong();
      return epoch <= upperBound ? OptionalLong.of(epoch) : OptionalLong.empty();
    }
  }

  synchronized long getIndexedThrough() {
    try {
      byte[] encoded = database.get(HEAD_KEY);
      return encoded == null ? -1 : ByteBuffer.wrap(encoded).getLong();
    } catch (RocksDBException failure) {
      throw new ArchivePersistenceException("Failed to read account index head", failure);
    }
  }

  synchronized boolean headMatches(BlockSnapshotMeta meta) {
    try {
      byte[] encoded = database.get(HEAD_KEY);
      return encoded != null
          && encoded.length == Long.BYTES + 32
          && ByteBuffer.wrap(encoded).getLong() == meta.getEpoch()
          && Arrays.equals(Arrays.copyOfRange(encoded, Long.BYTES, encoded.length),
          meta.getBlockHash());
    } catch (RocksDBException failure) {
      throw new ArchivePersistenceException("Failed to validate account index head", failure);
    }
  }

  private static byte[] dataKey(byte[] address, long epoch) {
    if (address.length != ADDRESS_LENGTH || epoch < 0) {
      throw new IllegalArgumentException("Invalid account change-index key");
    }
    return ByteBuffer.allocate(DATA_KEY_LENGTH).put(DATA_PREFIX).put(address).putLong(epoch)
        .array();
  }

  private static byte[] encodeHead(BlockSnapshotMeta meta) {
    return ByteBuffer.allocate(Long.BYTES + 32).putLong(meta.getEpoch()).put(meta.getBlockHash())
        .array();
  }

  @Override
  public synchronized void close() throws IOException {
    syncWrites.close();
    database.close();
    options.close();
  }
}
