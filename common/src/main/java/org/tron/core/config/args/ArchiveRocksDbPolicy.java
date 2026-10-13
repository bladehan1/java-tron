package org.tron.core.config.args;

/** Archive nodes use one native engine; ordinary nodes keep their upstream engine choice. */
public final class ArchiveRocksDbPolicy {
  private ArchiveRocksDbPolicy() {
  }

  public static void validate(Storage storage) {
    if (!storage.isArchiveFeaturesEnabled()) {
      return;
    }
    requireRocks(storage.getDbEngine(), "db.engine", false);
    requireRocks(storage.getPathStateRootEngine(), "pathStateRoot.engine", true);
    if (storage.getStateArchiveHotStoreSettings() != null) {
      requireRocks(storage.getStateArchiveHotStoreSettings().getEngine(),
          "stateArchive.hotStore.engine", true);
    }
    requireRocks(storage.getStateArchiveServingIndexEngine(), "stateArchive.servingIndexEngine",
        true);
  }

  private static void requireRocks(String engine, String field, boolean allowDefault) {
    if (allowDefault && (engine == null || engine.trim().isEmpty())) {
      return;
    }
    if (!"ROCKSDB".equalsIgnoreCase(engine)) {
      throw new IllegalArgumentException("Archive requires ROCKSDB: " + field + "=" + engine);
    }
  }
}
