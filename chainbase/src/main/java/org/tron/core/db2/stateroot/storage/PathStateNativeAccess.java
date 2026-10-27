package org.tron.core.db2.stateroot.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Generic native key/value engine protocol; concrete handles stay package-owned. */
public interface PathStateNativeAccess extends Closeable {
  int READ_SAMPLE_EVERY = PathStateNativeGetPerf.SAMPLE_EVERY;

  static PathStateNativeAccess open(Path directory, Engine engine) throws IOException {
    return PathStateNativeNodeStore.open(directory, engine);
  }

  static PathStateNativeAccess open(
      Path directory, Engine engine, String storageProfile, NativeDbConfig config)
      throws IOException {
    return PathStateNativeNodeStore.open(directory, engine, storageProfile, config);
  }

  static PathStateNativeAccess open(
      Path directory,
      Engine engine,
      String storageProfile,
      NativeDbConfig config,
      int cacheShardBits)
      throws IOException {
    return PathStateNativeNodeStore.open(directory, engine, storageProfile, config, cacheShardBits);
  }

  static PathStateNativeAccess open(
      Path directory,
      Engine engine,
      String storageProfile,
      NativeDbConfig config,
      int cacheShardBits,
      boolean readStatistics)
      throws IOException {
    return PathStateNativeNodeStore.open(
        directory, engine, storageProfile, config, cacheShardBits, readStatistics);
  }

  static PathStateNativeAccess open(
      Path directory,
      Engine engine,
      String storageProfile,
      NativeDbConfig config,
      int cacheShardBits,
      boolean readStatistics,
      long cacheBytes)
      throws IOException {
    return PathStateNativeNodeStore.open(
        directory, engine, storageProfile, config, cacheShardBits, readStatistics, cacheBytes);
  }

  Map<String, Long> readStatistics();

  Map<String, Long> readPerfStatistics();

  byte[] get(byte[] key);

  void put(byte[] key, byte[] value);

  void delete(byte[] key);

  void writeBatch(List<BatchMutation> mutations);

  void writeBatch(List<BatchMutation> mutations, boolean sync);

  void writeBatchUnsynced(List<BatchMutation> mutations);

  long getWriteBatchCalls();

  long getWriteBatchMutations();

  long getSyncedWriteBatchCalls();

  long getUnsyncedWriteBatchCalls();

  List<KeyValue> scanPrefix(byte[] prefix) throws IOException;

  void scanPrefix(byte[] prefix, EntryConsumer consumer) throws IOException;

  List<KeyValue> scanAll() throws IOException;

  void scanAll(EntryConsumer consumer) throws IOException;

  Path getDirectory();

  Engine getEngine();

  String getStorageProfile();

  final class BatchMutation {

    final byte[] key;
    final byte[] value;

    private BatchMutation(byte[] key, byte[] value) {
      this.key = PathStateNativeNodeStore.nonEmpty(key, "key");
      this.value = value == null ? null : PathStateNativeNodeStore.nonEmpty(value, "value");
    }

    public static BatchMutation put(byte[] key, byte[] value) {
      return new BatchMutation(key, Objects.requireNonNull(value, "value"));
    }

    public static BatchMutation delete(byte[] key) {
      return new BatchMutation(key, null);
    }
  }

  final class KeyValue {

    final byte[] key;
    final byte[] value;

    KeyValue(byte[] key, byte[] value) {
      this.key = PathStateNativeNodeStore.nonEmpty(key, "key");
      this.value = PathStateNativeNodeStore.nonEmpty(value, "value");
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public byte[] getValue() {
      return Arrays.copyOf(value, value.length);
    }
  }

  @FunctionalInterface
  interface EntryConsumer {

    void accept(KeyValue entry) throws IOException;
  }
}
