package org.tron.core.db2.stateroot;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteOptions;
import org.rocksdb.RocksDBException;
import org.tron.common.utils.DbOptionalsUtils;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Package-owned LevelDB/RocksDB key/value engine shared by namespaced path-node views. */
final class PathStateNativeNodeStore implements Closeable {

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  private final Path directory;
  private final Engine engine;
  private final Delegate delegate;
  private boolean closed;

  private PathStateNativeNodeStore(Path directory, Engine engine, Delegate delegate) {
    this.directory = directory;
    this.engine = engine;
    this.delegate = delegate;
  }

  /** Opens one independent node database; every mutation is synchronously WAL-backed. */
  static PathStateNativeNodeStore open(Path directory, Engine engine) throws IOException {
    Path path = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    Engine selected = Objects.requireNonNull(engine, "engine");
    if (Files.isSymbolicLink(path)) {
      throw new IOException("path-state node database must not be a symbolic link: " + path);
    }
    Files.createDirectories(path);
    if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("path-state node database is not a directory: " + path);
    }
    Delegate opened = selected == Engine.LEVELDB ? new LevelDelegate(path)
        : new RocksDelegate(path);
    return new PathStateNativeNodeStore(path, selected, opened);
  }

  synchronized byte[] get(byte[] key) {
    requireOpen();
    byte[] ownedKey = nonEmpty(key, "key");
    byte[] value = delegate.get(ownedKey);
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  synchronized void put(byte[] key, byte[] value) {
    writeBatch(Collections.singletonList(BatchMutation.put(key, value)));
  }

  synchronized void delete(byte[] key) {
    writeBatch(Collections.singletonList(BatchMutation.delete(key)));
  }

  synchronized void writeBatch(List<BatchMutation> mutations) {
    requireOpen();
    List<BatchMutation> supplied = Objects.requireNonNull(mutations, "mutations");
    if (supplied.isEmpty()) {
      throw new IllegalArgumentException("path-state native batch must not be empty");
    }
    for (BatchMutation mutation : supplied) {
      Objects.requireNonNull(mutation, "mutation");
    }
    delegate.writeBatch(supplied);
  }

  synchronized List<KeyValue> scanPrefix(byte[] prefix) throws IOException {
    List<KeyValue> entries = new ArrayList<>();
    scanPrefix(prefix, entries::add);
    return entries;
  }

  synchronized List<KeyValue> scanAll() throws IOException {
    List<KeyValue> entries = new ArrayList<>();
    scanAll(entries::add);
    return entries;
  }

  synchronized void scanPrefix(byte[] prefix, EntryConsumer consumer) throws IOException {
    requireOpen();
    delegate.scanPrefix(nonEmpty(prefix, "prefix"),
        Objects.requireNonNull(consumer, "consumer"));
  }

  synchronized void scanAll(EntryConsumer consumer) throws IOException {
    requireOpen();
    delegate.scanAll(Objects.requireNonNull(consumer, "consumer"));
  }

  Path getDirectory() {
    return directory;
  }

  Engine getEngine() {
    return engine;
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      delegate.close();
    }
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("path-state node database is closed: " + directory);
    }
  }

  private static byte[] nonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  private interface Delegate extends Closeable {

    byte[] get(byte[] key);

    void writeBatch(List<BatchMutation> mutations);

    void scanPrefix(byte[] prefix, EntryConsumer consumer) throws IOException;

    void scanAll(EntryConsumer consumer) throws IOException;
  }

  private static final class LevelDelegate implements Delegate {

    private final org.iq80.leveldb.Options options = DbOptionalsUtils.createDefaultDbOptions();
    private final WriteOptions syncWrites = new WriteOptions().sync(true);
    private final DB database;

    private LevelDelegate(Path directory) throws IOException {
      database = factory.open(directory.toFile(), options);
    }

    @Override
    public byte[] get(byte[] key) {
      return database.get(key);
    }

    @Override
    public void writeBatch(List<BatchMutation> mutations) {
      try (org.iq80.leveldb.WriteBatch batch = database.createWriteBatch()) {
        for (BatchMutation mutation : mutations) {
          if (mutation.value == null) {
            batch.delete(mutation.key);
          } else {
            batch.put(mutation.key, mutation.value);
          }
        }
        database.write(batch, syncWrites);
      } catch (IOException failure) {
        throw new IllegalStateException("failed to apply path-state LevelDB node batch", failure);
      }
    }

    @Override
    public void scanPrefix(byte[] prefix, EntryConsumer consumer) throws IOException {
      try (org.iq80.leveldb.DBIterator iterator = database.iterator()) {
        iterator.seek(prefix);
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          if (!startsWith(entry.getKey(), prefix)) {
            break;
          }
          consumer.accept(new KeyValue(entry.getKey(), entry.getValue()));
        }
      }
    }

    @Override
    public void scanAll(EntryConsumer consumer) throws IOException {
      try (org.iq80.leveldb.DBIterator iterator = database.iterator()) {
        iterator.seekToFirst();
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          consumer.accept(new KeyValue(entry.getKey(), entry.getValue()));
        }
      }
    }

    @Override
    public void close() throws IOException {
      database.close();
    }
  }

  private static final class RocksDelegate implements Delegate {

    private final org.rocksdb.Options options =
        new org.rocksdb.Options().setCreateIfMissing(true).setParanoidChecks(true);
    private final org.rocksdb.WriteOptions syncWrites =
        new org.rocksdb.WriteOptions().setSync(true);
    private final org.rocksdb.RocksDB database;

    private RocksDelegate(Path directory) throws IOException {
      try {
        database = org.rocksdb.RocksDB.open(options, directory.toString());
      } catch (RocksDBException failure) {
        syncWrites.close();
        options.close();
        throw new IOException("failed to open path-state RocksDB node database", failure);
      }
    }

    @Override
    public byte[] get(byte[] key) {
      try {
        return database.get(key);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to read path-state RocksDB node", failure);
      }
    }

    @Override
    public void writeBatch(List<BatchMutation> mutations) {
      try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
        for (BatchMutation mutation : mutations) {
          if (mutation.value == null) {
            batch.delete(mutation.key);
          } else {
            batch.put(mutation.key, mutation.value);
          }
        }
        database.write(syncWrites, batch);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to apply path-state RocksDB node batch", failure);
      }
    }

    @Override
    public void scanPrefix(byte[] prefix, EntryConsumer consumer) throws IOException {
      try (org.rocksdb.RocksIterator iterator = database.newIterator()) {
        iterator.seek(prefix);
        while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
          consumer.accept(new KeyValue(iterator.key(), iterator.value()));
          iterator.next();
        }
        iterator.status();
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to scan path-state RocksDB nodes", failure);
      }
    }

    @Override
    public void scanAll(EntryConsumer consumer) throws IOException {
      try (org.rocksdb.RocksIterator iterator = database.newIterator()) {
        iterator.seekToFirst();
        while (iterator.isValid()) {
          consumer.accept(new KeyValue(iterator.key(), iterator.value()));
          iterator.next();
        }
        iterator.status();
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to scan all path-state RocksDB nodes", failure);
      }
    }

    @Override
    public void close() {
      syncWrites.close();
      database.close();
      options.close();
    }
  }

  static final class BatchMutation {

    private final byte[] key;
    private final byte[] value;

    private BatchMutation(byte[] key, byte[] value) {
      this.key = nonEmpty(key, "key");
      this.value = value == null ? null : nonEmpty(value, "value");
    }

    static BatchMutation put(byte[] key, byte[] value) {
      return new BatchMutation(key, Objects.requireNonNull(value, "value"));
    }

    static BatchMutation delete(byte[] key) {
      return new BatchMutation(key, null);
    }
  }

  static final class KeyValue {

    private final byte[] key;
    private final byte[] value;

    private KeyValue(byte[] key, byte[] value) {
      this.key = nonEmpty(key, "key");
      this.value = nonEmpty(value, "value");
    }

    byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    byte[] getValue() {
      return Arrays.copyOf(value, value.length);
    }
  }

  @FunctionalInterface
  interface EntryConsumer {

    void accept(KeyValue entry) throws IOException;
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    return value.length >= prefix.length
        && Arrays.equals(Arrays.copyOf(value, prefix.length), prefix);
  }
}
