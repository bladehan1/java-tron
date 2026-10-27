package org.tron.core.db2;

import java.nio.file.Path;
import org.tron.common.arch.Arch;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.LevelDB;
import org.tron.core.db2.common.RocksDB;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Runs native checkpoint contracts on the backend supported by the current test platform. */
public final class NativeCheckpointTestSupport {
  private NativeCheckpointTestSupport() {
  }

  public static Engine engine() {
    return Arch.isArm64() || "ROCKSDB".equalsIgnoreCase(System.getProperty("storage.db.engine"))
        ? Engine.ROCKSDB : Engine.LEVELDB;
  }

  public static DB<byte[], byte[]> database(Path directory, String name) {
    return engine() == Engine.ROCKSDB
        ? new RocksDB(new RocksDbDataSourceImpl(directory.toString(), name))
        : new LevelDB(new LevelDbDataSourceImpl(directory.toString(), name));
  }
}
