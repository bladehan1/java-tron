package org.tron.common.storage.rocksdb;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.tron.common.setting.RocksDbSettings;

/** Optional per-level block-cache trace backed by a version-matched RocksDB JNI bridge. */
@Slf4j(topic = "DB")
final class RocksDbBlockCacheTrace implements AutoCloseable {

  private static final Object LOAD_LOCK = new Object();
  private static String loadedLibrary;

  private final RocksDB database;
  private final String databaseName;
  private boolean started;

  private RocksDbBlockCacheTrace(RocksDB database, String databaseName) {
    this.database = database;
    this.databaseName = databaseName;
  }

  static RocksDbBlockCacheTrace create(RocksDB database, String databaseName, Path databasePath) {
    RocksDbSettings settings = RocksDbSettings.getSettings();
    if (!settings.shouldTraceBlockCache(databaseName)) {
      return null;
    }
    requireRocksDbVersion();
    loadLibrary(settings.getBlockCacheTraceNativeLibrary());
    Path outputDirectory = Paths.get(settings.getBlockCacheTraceOutputDirectory())
        .toAbsolutePath().normalize();
    databasePath = databasePath.toAbsolutePath().normalize();
    if (outputDirectory.startsWith(databasePath)) {
      throw new IllegalArgumentException("Block cache trace output must be outside database: "
          + outputDirectory);
    }
    try {
      Files.createDirectories(outputDirectory);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create block cache trace directory", e);
    }
    Path traceFile = outputDirectory.resolve(safeFileName(databaseName) + ".csv");
    RocksDbBlockCacheTrace trace = new RocksDbBlockCacheTrace(database, databaseName);
    String error = startTrace(nativeHandle(database), traceFile.toString(),
        settings.getBlockCacheTraceSampleOneIn(), settings.getBlockCacheTraceMaxBytesPerDb());
    if (error != null) {
      throw new IllegalStateException("Unable to start block cache trace for " + databaseName
          + ": " + error);
    }
    trace.started = true;
    logger.info("Started RocksDB block cache trace db={}, sampleOneIn={}, maxBytes={}, path={}",
        databaseName, settings.getBlockCacheTraceSampleOneIn(),
        settings.getBlockCacheTraceMaxBytesPerDb(), traceFile);
    return trace;
  }

  private static void requireRocksDbVersion() {
    try {
      Object version = RocksDB.class.getMethod("rocksdbVersion").invoke(null);
      Method major = version.getClass().getMethod("getMajor");
      Method minor = version.getClass().getMethod("getMinor");
      int majorValue = ((Number) major.invoke(version)).intValue();
      int minorValue = ((Number) minor.invoke(version)).intValue();
      if (majorValue != 9 || minorValue != 7) {
        throw new IllegalStateException("Block cache trace bridge requires RocksDB 9.7.x, found "
            + majorValue + "." + minorValue);
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to verify RocksDB version for block cache trace", e);
    }
  }

  private static long nativeHandle(RocksDB database) {
    try {
      Method method = database.getClass().getMethod("getNativeHandle");
      return ((Number) method.invoke(database)).longValue();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("RocksDB JNI does not expose getNativeHandle", e);
    }
  }

  private static void loadLibrary(String library) {
    String normalized = Paths.get(library).toAbsolutePath().normalize().toString();
    synchronized (LOAD_LOCK) {
      if (loadedLibrary == null) {
        System.load(normalized);
        loadedLibrary = normalized;
      } else if (!loadedLibrary.equals(normalized)) {
        throw new IllegalStateException("Block cache trace bridge already loaded from "
            + loadedLibrary);
      }
    }
  }

  private static String safeFileName(String databaseName) {
    return databaseName.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  @Override
  public void close() {
    if (!started) {
      return;
    }
    String error = endTrace(nativeHandle(database));
    started = false;
    if (error != null) {
      logger.error("Unable to end RocksDB block cache trace for {}: {}", databaseName, error);
    }
  }

  private static native String startTrace(long databaseHandle, String outputPath,
      long sampleOneIn, long maxBytes);

  private static native String endTrace(long databaseHandle);
}
