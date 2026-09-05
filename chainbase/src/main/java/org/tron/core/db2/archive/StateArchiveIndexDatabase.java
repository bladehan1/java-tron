package org.tron.core.db2.archive;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.tron.common.utils.DbOptionalsUtils;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Engine-neutral native store for the common-checkpoint Archive serving index. */
final class StateArchiveIndexDatabase {

  private static final Map<Path, SharedLevelDatabase> LEVEL_DATABASES = new HashMap<>();

  private StateArchiveIndexDatabase() {
  }

  static Reader openReader(Path directory, Engine engine) throws IOException {
    Path path = normalize(directory);
    return engine == Engine.LEVELDB ? new LevelReader(acquireLevel(path, false))
        : new RocksReader(path);
  }

  static Writer openWriter(Path directory, Engine engine) throws IOException {
    Path path = normalize(directory);
    return engine == Engine.LEVELDB ? new LevelWriter(acquireLevel(path, true))
        : new RocksWriter(path);
  }

  static Mutation put(byte[] key, byte[] value) {
    return new Mutation(key, value);
  }

  private static Path normalize(Path directory) {
    return Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
  }

  private static synchronized SharedLevelDatabase acquireLevel(Path directory, boolean create)
      throws IOException {
    SharedLevelDatabase shared = LEVEL_DATABASES.get(directory);
    if (shared == null) {
      org.iq80.leveldb.Options options = DbOptionalsUtils.createDefaultDbOptions()
          .createIfMissing(create);
      try {
        shared = new SharedLevelDatabase(directory, factory.open(directory.toFile(), options));
      } catch (IOException | RuntimeException failure) {
        if (failure instanceof IOException) {
          throw (IOException) failure;
        }
        throw failure;
      }
      LEVEL_DATABASES.put(directory, shared);
    }
    shared.references++;
    return shared;
  }

  private static synchronized void releaseLevel(SharedLevelDatabase shared) throws IOException {
    if (--shared.references != 0) {
      return;
    }
    LEVEL_DATABASES.remove(shared.directory);
    shared.database.close();
  }

  interface Reader extends Closeable {

    byte[] get(byte[] key) throws IOException;

    KeyValue seek(byte[] key) throws IOException;
  }

  interface Writer extends Closeable {

    byte[] get(byte[] key) throws IOException;

    void write(List<Mutation> mutations) throws IOException;
  }

  static final class Mutation {
    private final byte[] key;
    private final byte[] value;

    private Mutation(byte[] key, byte[] value) {
      this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
      this.value = Arrays.copyOf(Objects.requireNonNull(value, "value"), value.length);
    }
  }

  static final class KeyValue {
    private final byte[] key;
    private final byte[] value;

    private KeyValue(byte[] key, byte[] value) {
      this.key = Arrays.copyOf(key, key.length);
      this.value = Arrays.copyOf(value, value.length);
    }

    byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    byte[] getValue() {
      return Arrays.copyOf(value, value.length);
    }
  }

  private static final class SharedLevelDatabase {
    private final Path directory;
    private final DB database;
    private int references;

    private SharedLevelDatabase(Path directory, DB database) {
      this.directory = directory;
      this.database = database;
    }
  }

  private static final class LevelReader implements Reader {
    private final SharedLevelDatabase shared;
    private final Snapshot snapshot;
    private final ReadOptions reads;
    private boolean closed;

    private LevelReader(SharedLevelDatabase shared) throws IOException {
      this.shared = shared;
      Snapshot openedSnapshot = null;
      ReadOptions openedReads = null;
      try {
        openedSnapshot = shared.database.getSnapshot();
        openedReads = new ReadOptions().fillCache(false).snapshot(openedSnapshot);
      } catch (RuntimeException failure) {
        if (openedSnapshot != null) {
          openedSnapshot.close();
        }
        releaseLevel(shared);
        throw failure;
      }
      this.snapshot = openedSnapshot;
      this.reads = openedReads;
    }

    @Override
    public byte[] get(byte[] key) {
      return shared.database.get(key, reads);
    }

    @Override
    public KeyValue seek(byte[] key) throws IOException {
      try (DBIterator iterator = shared.database.iterator(reads)) {
        iterator.seek(key);
        if (!iterator.hasNext()) {
          return null;
        }
        Map.Entry<byte[], byte[]> entry = iterator.next();
        return new KeyValue(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        snapshot.close();
        releaseLevel(shared);
      }
    }
  }

  private static final class LevelWriter implements Writer {
    private final SharedLevelDatabase shared;
    private final org.iq80.leveldb.WriteOptions writes =
        new org.iq80.leveldb.WriteOptions().sync(true);
    private boolean closed;

    private LevelWriter(SharedLevelDatabase shared) {
      this.shared = shared;
    }

    @Override
    public byte[] get(byte[] key) {
      return shared.database.get(key);
    }

    @Override
    public void write(List<Mutation> mutations) throws IOException {
      try (org.iq80.leveldb.WriteBatch batch = shared.database.createWriteBatch()) {
        for (Mutation mutation : mutations) {
          batch.put(mutation.key, mutation.value);
        }
        shared.database.write(batch, writes);
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        releaseLevel(shared);
      }
    }
  }

  private static final class RocksReader implements Reader {
    static {
      org.rocksdb.RocksDB.loadLibrary();
    }

    private final org.rocksdb.Options options =
        new org.rocksdb.Options().setCreateIfMissing(false);
    private final org.rocksdb.RocksDB database;
    private boolean closed;

    private RocksReader(Path directory) throws IOException {
      try {
        database = org.rocksdb.RocksDB.openReadOnly(options, directory.toString());
      } catch (org.rocksdb.RocksDBException | RuntimeException failure) {
        options.close();
        throw new IOException("Failed to open RocksDB Archive serving index", failure);
      }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
      try {
        return database.get(key);
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to read RocksDB Archive serving index", failure);
      }
    }

    @Override
    public KeyValue seek(byte[] key) throws IOException {
      try (org.rocksdb.ReadOptions reads = new org.rocksdb.ReadOptions();
          org.rocksdb.RocksIterator iterator = database.newIterator(reads)) {
        iterator.seek(key);
        if (!iterator.isValid()) {
          iterator.status();
          return null;
        }
        return new KeyValue(iterator.key(), iterator.value());
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to seek RocksDB Archive serving index", failure);
      }
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        database.close();
        options.close();
      }
    }
  }

  private static final class RocksWriter implements Writer {
    static {
      org.rocksdb.RocksDB.loadLibrary();
    }

    private final org.rocksdb.Options options =
        new org.rocksdb.Options().setCreateIfMissing(true)
            .setCompressionType(org.rocksdb.CompressionType.NO_COMPRESSION);
    private final org.rocksdb.WriteOptions writes = new org.rocksdb.WriteOptions().setSync(true);
    private final org.rocksdb.RocksDB database;
    private boolean closed;

    private RocksWriter(Path directory) throws IOException {
      try {
        database = org.rocksdb.RocksDB.open(options, directory.toString());
      } catch (org.rocksdb.RocksDBException | RuntimeException failure) {
        writes.close();
        options.close();
        throw new IOException("Failed to open RocksDB Archive serving index", failure);
      }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
      try {
        return database.get(key);
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to read RocksDB Archive serving index", failure);
      }
    }

    @Override
    public void write(List<Mutation> mutations) throws IOException {
      try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
        for (Mutation mutation : mutations) {
          batch.put(mutation.key, mutation.value);
        }
        database.write(writes, batch);
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to write RocksDB Archive serving index", failure);
      }
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        database.close();
        writes.close();
        options.close();
      }
    }
  }
}
