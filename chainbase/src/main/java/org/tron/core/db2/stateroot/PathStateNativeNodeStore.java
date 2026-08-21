package org.tron.core.db2.stateroot;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
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
    requireOpen();
    delegate.put(nonEmpty(key, "key"), nonEmpty(value, "value"));
  }

  synchronized void delete(byte[] key) {
    requireOpen();
    delegate.delete(nonEmpty(key, "key"));
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

    void put(byte[] key, byte[] value);

    void delete(byte[] key);
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
    public void put(byte[] key, byte[] value) {
      database.put(key, value, syncWrites);
    }

    @Override
    public void delete(byte[] key) {
      database.delete(key, syncWrites);
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
    public void put(byte[] key, byte[] value) {
      try {
        database.put(syncWrites, key, value);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to write path-state RocksDB node", failure);
      }
    }

    @Override
    public void delete(byte[] key) {
      try {
        database.delete(syncWrites, key);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to delete path-state RocksDB node", failure);
      }
    }

    @Override
    public void close() {
      syncWrites.close();
      database.close();
      options.close();
    }
  }
}
