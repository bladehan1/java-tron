package org.tron.core.db2.archive;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.tron.common.utils.DbOptionalsUtils;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Engine-neutral native store for Archive serving indexes. */
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

  static void checkpoint(Path source, Path target, Engine engine) throws IOException {
    Path from = normalize(source);
    Path to = normalize(target);
    if (engine == Engine.ROCKSDB) {
      RocksCheckpoint.create(from, to);
      return;
    }
    checkpointLevel(from, to);
  }

  private static void checkpointLevel(Path source, Path target) throws IOException {
    SharedLevelDatabase shared = acquireLevel(source, false);
    boolean suspended = false;
    try {
      shared.database.suspendCompactions();
      suspended = true;
      Files.createDirectory(target);
      try (java.util.stream.Stream<Path> entries = Files.list(source)) {
        for (Path entry : (Iterable<Path>) entries::iterator) {
          if (!Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
            continue;
          }
          String name = entry.getFileName().toString();
          if ("LOCK".equals(name) || "LOG".equals(name) || "LOG.old".equals(name)) {
            continue;
          }
          Path destination = target.resolve(name);
          if (name.endsWith(".sst") || name.endsWith(".ldb")) {
            Files.createLink(destination, entry);
          } else {
            Files.copy(entry, destination, StandardCopyOption.COPY_ATTRIBUTES);
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                destination, java.nio.file.StandardOpenOption.WRITE)) {
              channel.force(true);
            }
          }
        }
      }
      HistorySegmentStore.syncDirectory(target);
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while checkpointing LevelDB Archive index", failure);
    } finally {
      if (suspended) {
        shared.database.resumeCompactions();
      }
      releaseLevel(shared);
    }
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

    Cursor cursor() throws IOException;

    OptionalLong readLongProperty(String name) throws IOException;
  }

  interface Writer extends Closeable {

    byte[] get(byte[] key) throws IOException;

    void write(List<Mutation> mutations) throws IOException;

    void write(List<Mutation> mutations, boolean sync) throws IOException;
  }

  interface Cursor extends Closeable {

    void seek(byte[] key) throws IOException;

    KeyValue next() throws IOException;
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
    public Cursor cursor() {
      return new LevelCursor(shared.database.iterator(reads));
    }

    @Override
    public OptionalLong readLongProperty(String name) {
      return OptionalLong.empty();
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
      write(mutations, true);
    }

    @Override
    public void write(List<Mutation> mutations, boolean sync) throws IOException {
      try (org.iq80.leveldb.WriteBatch batch = shared.database.createWriteBatch()) {
        for (Mutation mutation : mutations) {
          batch.put(mutation.key, mutation.value);
        }
        shared.database.write(batch, new org.iq80.leveldb.WriteOptions().sync(sync));
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

  private static final class LevelCursor implements Cursor {
    private final DBIterator iterator;

    private LevelCursor(DBIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public void seek(byte[] key) {
      iterator.seek(key);
    }

    @Override
    public KeyValue next() {
      if (!iterator.hasNext()) {
        return null;
      }
      Map.Entry<byte[], byte[]> entry = iterator.next();
      return new KeyValue(entry.getKey(), entry.getValue());
    }

    @Override
    public void close() throws IOException {
      iterator.close();
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
    public Cursor cursor() {
      return new RocksCursor(database, new org.rocksdb.ReadOptions());
    }

    @Override
    public OptionalLong readLongProperty(String name) {
      try {
        return OptionalLong.of(database.getLongProperty(name));
      } catch (org.rocksdb.RocksDBException | IllegalArgumentException failure) {
        return OptionalLong.empty();
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
    private final org.rocksdb.RocksDB database;
    private boolean closed;

    private RocksWriter(Path directory) throws IOException {
      try {
        database = org.rocksdb.RocksDB.open(options, directory.toString());
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
    public void write(List<Mutation> mutations) throws IOException {
      write(mutations, true);
    }

    @Override
    public void write(List<Mutation> mutations, boolean sync) throws IOException {
      try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
        for (Mutation mutation : mutations) {
          batch.put(mutation.key, mutation.value);
        }
        try (org.rocksdb.WriteOptions selected = new org.rocksdb.WriteOptions().setSync(sync)) {
          database.write(selected, batch);
        }
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to write RocksDB Archive serving index", failure);
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

  private static final class RocksCursor implements Cursor {
    private final org.rocksdb.ReadOptions reads;
    private final org.rocksdb.RocksIterator iterator;

    private RocksCursor(org.rocksdb.RocksDB database, org.rocksdb.ReadOptions reads) {
      this.reads = reads;
      this.iterator = database.newIterator(reads);
    }

    @Override
    public void seek(byte[] key) {
      iterator.seek(key);
    }

    @Override
    public KeyValue next() throws IOException {
      if (!iterator.isValid()) {
        try {
          iterator.status();
        } catch (org.rocksdb.RocksDBException failure) {
          throw new IOException("Failed to scan RocksDB Archive serving index", failure);
        }
        return null;
      }
      KeyValue value = new KeyValue(iterator.key(), iterator.value());
      iterator.next();
      return value;
    }

    @Override
    public void close() {
      iterator.close();
      reads.close();
    }
  }

  private static final class RocksCheckpoint {
    static {
      org.rocksdb.RocksDB.loadLibrary();
    }

    private static void create(Path source, Path target) throws IOException {
      try (org.rocksdb.Options options = new org.rocksdb.Options().setCreateIfMissing(false);
          org.rocksdb.RocksDB database = org.rocksdb.RocksDB.open(options, source.toString());
          org.rocksdb.Checkpoint checkpoint = org.rocksdb.Checkpoint.create(database)) {
        checkpoint.createCheckpoint(target.toString());
      } catch (org.rocksdb.RocksDBException failure) {
        throw new IOException("Failed to checkpoint RocksDB Archive serving index", failure);
      }
    }
  }
}
