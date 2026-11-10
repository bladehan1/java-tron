package org.tron.core.db2.archive.storage.index;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Native index storage protocol shared by checkpoint, hot history, and derived serving indexes. */
public final class ArchiveIndexStorage {
  public static final String DEFAULT_COLUMN_FAMILY =
      StateArchiveIndexDatabase.DEFAULT_COLUMN_FAMILY;
  public static final String HOT_BLOCKS_COLUMN_FAMILY =
      StateArchiveIndexDatabase.HOT_BLOCKS_COLUMN_FAMILY;

  private ArchiveIndexStorage() {}

  /** Resolves the configured native engine at archive construction boundaries. */
  public static Engine configuredEngine() {
    org.tron.core.config.args.Storage storage = CommonParameter.getInstance().getStorage();
    if (storage == null) {
      return Engine.ROCKSDB;
    }
    String configured = storage.getStateArchiveServingIndexEngine();
    if (configured == null) {
      configured = storage.getDbEngine();
    }
    if (configured == null) {
      return Engine.ROCKSDB;
    }
    return Engine.valueOf(configured.trim().toUpperCase(Locale.ROOT));
  }

  public static Reader openReader(Path directory, Engine engine) throws IOException {
    return StateArchiveIndexDatabase.openReader(directory, engine);
  }

  public static Reader openReader(Path directory, Engine engine, NativeDbConfig suppliedConfig)
      throws IOException {
    return StateArchiveIndexDatabase.openReader(directory, engine, suppliedConfig);
  }

  public static Reader openImmutableReader(Path directory, Engine engine) throws IOException {
    return StateArchiveIndexDatabase.openImmutableReader(directory, engine);
  }

  public static Writer openWriter(Path directory, Engine engine) throws IOException {
    return StateArchiveIndexDatabase.openWriter(directory, engine);
  }

  public static Writer openWriter(Path directory, Engine engine, NativeDbConfig suppliedConfig)
      throws IOException {
    return StateArchiveIndexDatabase.openWriter(directory, engine, suppliedConfig);
  }

  public static Reader openHotReader(Path directory, Engine engine, NativeDbConfig suppliedConfig)
      throws IOException {
    return StateArchiveIndexDatabase.openHotReader(directory, engine, suppliedConfig);
  }

  public static Writer openHotWriter(Path directory, Engine engine, NativeDbConfig suppliedConfig)
      throws IOException {
    return StateArchiveIndexDatabase.openHotWriter(directory, engine, suppliedConfig);
  }

  public static String hotStoreColumnFamily(String dbName) {
    return StateArchiveIndexDatabase.hotStoreColumnFamily(dbName);
  }

  public static List<String> hotColumnFamilies() {
    return StateArchiveIndexDatabase.hotColumnFamilies();
  }

  public static void checkpoint(Path source, Path target, Engine engine) throws IOException {
    StateArchiveIndexDatabase.checkpoint(source, target, engine);
  }

  public static void checkpointImmutable(Path source, Path target, Engine engine)
      throws IOException {
    StateArchiveIndexDatabase.checkpointImmutable(source, target, engine);
  }

  public static Mutation put(byte[] key, byte[] value) {
    return StateArchiveIndexDatabase.put(key, value);
  }

  public static Mutation put(String columnFamily, byte[] key, byte[] value) {
    return StateArchiveIndexDatabase.put(columnFamily, key, value);
  }

  public static Mutation delete(byte[] key) {
    return StateArchiveIndexDatabase.delete(key);
  }

  public static Mutation delete(String columnFamily, byte[] key) {
    return StateArchiveIndexDatabase.delete(columnFamily, key);
  }

  public interface Reader extends Closeable {

    default byte[] get(byte[] key) throws IOException {
      return get(DEFAULT_COLUMN_FAMILY, key);
    }

    byte[] get(String columnFamily, byte[] key) throws IOException;

    default KeyValue seek(byte[] key) throws IOException {
      return seek(DEFAULT_COLUMN_FAMILY, key);
    }

    KeyValue seek(String columnFamily, byte[] key) throws IOException;

    Cursor cursor() throws IOException;

    OptionalLong readLongProperty(String name) throws IOException;
  }

  public interface Writer extends Closeable {
    default Reader openSnapshot() throws IOException {
      throw new UnsupportedOperationException("native writer snapshot is unavailable");
    }

    default byte[] get(byte[] key) throws IOException {
      return get(DEFAULT_COLUMN_FAMILY, key);
    }

    byte[] get(String columnFamily, byte[] key) throws IOException;

    void write(List<Mutation> mutations) throws IOException;

    void write(List<Mutation> mutations, boolean sync) throws IOException;
  }

  public interface Cursor extends Closeable {

    void seek(byte[] key) throws IOException;

    KeyValue next() throws IOException;

    /** Advances one row without requiring its value to be copied into Java. */
    default byte[] nextKey() throws IOException {
      KeyValue row = next();
      return row == null ? null : row.getKey();
    }
  }

  public static final class Mutation {
    final String columnFamily;
    final byte[] key;
    final byte[] value;

    public long estimatedBytes() {
      return 32L + key.length + (value == null ? 0 : value.length);
    }

    Mutation(String columnFamily, byte[] key, byte[] value) {
      this.columnFamily = StateArchiveIndexDatabase.requireColumnFamily(columnFamily);
      this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }
  }

  public static final class KeyValue {
    final byte[] key;
    final byte[] value;

    KeyValue(byte[] key, byte[] value) {
      this.key = Arrays.copyOf(key, key.length);
      this.value = Arrays.copyOf(value, value.length);
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public byte[] getValue() {
      return Arrays.copyOf(value, value.length);
    }
  }
}
