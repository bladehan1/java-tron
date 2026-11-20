package org.tron.common.storage.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Architecture-specific WAL sync barrier adapter. The legacy x86 baseline pins
 * RocksDB-JNI 5.15.10 (see root archInfo), which has no {@code syncWal()}.
 * A synced write of an empty batch enters the same write group and honors
 * group-commit semantics: all previously written unsynced records are durable
 * once this call returns. Crash-level (power-loss) proof on 5.15.10 is not
 * covered by CI and remains an explicit validation gap.
 */
public final class RocksDbWal {

  private RocksDbWal() {
  }

  public static void syncWal(RocksDB database) throws RocksDBException {
    try (WriteOptions sync = new WriteOptions().setSync(true);
        WriteBatch empty = new WriteBatch()) {
      database.write(sync, empty);
    }
  }
}
