package org.tron.core.db;

import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.tron.core.config.args.Storage;

public class ManagerArchiveEngineModeTest {

  @Test
  public void rejectsLegacyLevelDbArchiveButAcceptsSingleEngineModes() {
    Storage storage = storage("LEVELDB", true, false);
    assertThrows(IllegalStateException.class,
        () -> Manager.requireSingleEngineArchiveMode(storage));

    Manager.requireSingleEngineArchiveMode(storage("LEVELDB", true, true));
    Manager.requireSingleEngineArchiveMode(storage("ROCKSDB", true, false));
    Manager.requireSingleEngineArchiveMode(storage("LEVELDB", false, false));
  }

  private static Storage storage(String engine, boolean archive, boolean commonCheckpoint) {
    Storage storage = new Storage();
    storage.setDbEngine(engine);
    storage.setStateArchiveEnabled(archive);
    storage.setCommonCheckpointEnabled(commonCheckpoint);
    return storage;
  }
}
