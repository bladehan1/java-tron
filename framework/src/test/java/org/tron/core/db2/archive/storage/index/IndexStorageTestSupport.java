package org.tron.core.db2.archive.storage.index;

import java.nio.file.Path;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Package-private native ownership diagnostics for regression fixtures. */
public final class IndexStorageTestSupport {
  private IndexStorageTestSupport() {}

  public static org.rocksdb.LRUCache newServingBlockCache(long capacity) {
    return StateArchiveIndexDatabase.newServingBlockCache(capacity);
  }

  public static int openReferenceCount(Path directory, Engine engine) {
    return StateArchiveIndexDatabase.openReferenceCount(directory, engine);
  }
}
