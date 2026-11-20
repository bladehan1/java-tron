package org.tron.common.storage.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Architecture-specific WAL sync barrier adapter. RocksDB-JNI 9.7.4 (arm64 and
 * pinned x86 candidates) exposes {@link RocksDB#syncWal()}; the legacy x86
 * baseline (5.15.10, see root archInfo) does not.
 */
public final class RocksDbWal {

  private RocksDbWal() {
  }

  public static void syncWal(RocksDB database) throws RocksDBException {
    database.syncWal();
  }
}
