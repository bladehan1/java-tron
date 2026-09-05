package org.tron.core.db2.stateroot;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.tron.common.arch.Arch;

/** Test-only LevelDB/RocksDB evidence for the backend-neutral TASK-016 node boundary. */
public class PathNodeStoreEngineTest {

  private static final String LEVELDB = "LEVELDB";
  private static final String ROCKSDB = "ROCKSDB";
  private static final PathStateParticipant ABI = participant(1, "abi");
  private static final PathStateParticipant ACCOUNT = participant(4, "account");
  private static final PathStateParticipant ASSET_ISSUE = participant(6, "asset-issue");
  private static final PathStateParticipant ASSET_ISSUE_V2 = participant(7, "asset-issue-v2");
  private static final PathStateParticipant STORAGE = participant(22, "storage-row");

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void levelDbAndRocksDbProduceIdenticalRootAndPathNodes() throws Exception {
    org.junit.Assume.assumeFalse(Arch.isArm64());
    EngineFixture level = new EngineFixture(LEVELDB,
        temporaryFolder.newFolder("path-state-level"));
    EngineFixture rocks = new EngineFixture(ROCKSDB,
        temporaryFolder.newFolder("path-state-rocks"));
    try {
      List<PathStateMutation> mutations = mutations();
      level.root.apply(mutations);
      List<PathStateMutation> reversed = new ArrayList<>(mutations);
      Collections.reverse(reversed);
      rocks.root.apply(reversed);

      byte[] expected = Hex.decode(
          "16a59be5527b6c746e4bc2b0a67046989116f7f855a10ae0fb65263e9fb7bfda");
      assertEquals(Hex.toHexString(expected), Hex.toHexString(level.root.rootHash()));
      assertArrayEquals(expected, rocks.root.rootHash());
      level.root.verifyNodeStores();
      rocks.root.verifyNodeStores();
      assertEquals(level.snapshotNodes(), rocks.snapshotNodes());

      byte[] originalSuperRootNode = rocks.superStore.get(new byte[0]);
      rocks.superStore.put(new byte[0], new byte[]{1});
      assertThrows(IllegalStateException.class, rocks.root::verifyNodeStores);
      rocks.superStore.put(new byte[0], originalSuperRootNode);
      rocks.root.verifyNodeStores();
    } finally {
      rocks.close();
      level.close();
    }
  }

  @Test
  public void engineAdaptersPreserveRootPathBytesAcrossReopen() throws Exception {
    for (String engine : availableEngines()) {
      File parent = temporaryFolder.newFolder("path-reopen-" + engine.toLowerCase());
      byte[] rootPath = new byte[0];
      byte[] encodedNode = Hex.decode("c22080");
      EnginePathNodeStore first = open(engine, parent, "root");
      first.put(rootPath, encodedNode);
      first.close();

      EnginePathNodeStore reopened = open(engine, parent, "root");
      try {
        assertArrayEquals(encodedNode, reopened.get(rootPath));
        reopened.delete(rootPath);
        assertNull(reopened.get(rootPath));
      } finally {
        reopened.close();
      }
    }
  }

  private static List<String> availableEngines() {
    return Arch.isArm64() ? Collections.singletonList(ROCKSDB)
        : Arrays.asList(LEVELDB, ROCKSDB);
  }

  private static List<PathStateParticipant> participants() {
    return Arrays.asList(ABI, ACCOUNT, ASSET_ISSUE, ASSET_ISSUE_V2, STORAGE);
  }

  private static List<PathStateMutation> mutations() {
    return Arrays.asList(
        mutation("abi", "contract", "abi-v1"),
        mutation("asset-issue", "asset", "legacy"),
        mutation("asset-issue-v2", "asset", "v2"),
        mutation("account", "address", "account-value"),
        mutation("storage-row", "slot", "storage-value"));
  }

  private static PathStateMutation mutation(String dbName, String key, String value) {
    return PathStateMutation.put(dbName, bytes(key), bytes(value));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static PathStateParticipant participant(int storeId, String dbName) {
    return new PathStateParticipant(storeId, dbName, 1);
  }

  private static EnginePathNodeStore open(String engine, File parent, String dbName) {
    Path directory = new File(parent, dbName).toPath();
    return LEVELDB.equals(engine) ? new LevelPathNodeStore(directory)
        : new RocksPathNodeStore(directory);
  }

  private static final class EngineFixture implements AutoCloseable {

    private final String engine;
    private final File parent;
    private final Map<String, EnginePathNodeStore> participantStores = new LinkedHashMap<>();
    private final List<EnginePathNodeStore> stores = new ArrayList<>();
    private final EnginePathNodeStore superStore;
    private final PathStateRoot root;

    private EngineFixture(String engine, File parent) {
      this.engine = engine;
      this.parent = parent;
      PathStateParticipantScope scope = new PathStateParticipantScope(participants());
      superStore = create("super");
      root = new PathStateRoot(scope, participant -> {
        EnginePathNodeStore store = create("store-" + participant.getStoreId());
        participantStores.put(participant.getDbName(), store);
        return store;
      }, superStore);
    }

    private EnginePathNodeStore create(String dbName) {
      EnginePathNodeStore store = open(engine, parent, dbName);
      stores.add(store);
      return store;
    }

    private Map<String, Map<String, String>> snapshotNodes() {
      Map<String, Map<String, String>> snapshot = new LinkedHashMap<>();
      for (Map.Entry<String, EnginePathNodeStore> entry : participantStores.entrySet()) {
        snapshot.put(entry.getKey(), entry.getValue().snapshot());
      }
      snapshot.put("super", superStore.snapshot());
      return snapshot;
    }

    @Override
    public void close() {
      for (int i = stores.size() - 1; i >= 0; i--) {
        stores.get(i).close();
      }
    }
  }

  private abstract static class EnginePathNodeStore implements PathNodeStore, AutoCloseable {

    abstract Map<String, String> snapshot();

    @Override
    public abstract void close();
  }

  private static final class LevelPathNodeStore extends EnginePathNodeStore {

    private final Options options = new Options().createIfMissing(true);
    private final org.iq80.leveldb.DB database;

    private LevelPathNodeStore(Path directory) {
      try {
        Files.createDirectories(directory);
        database = factory.open(directory.toFile(), options);
      } catch (IOException failure) {
        throw new IllegalStateException("failed to open test LevelDB", failure);
      }
    }

    @Override
    public byte[] get(byte[] path) {
      return database.get(path);
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      database.put(path, encodedNode);
    }

    @Override
    public void delete(byte[] path) {
      database.delete(path);
    }

    @Override
    Map<String, String> snapshot() {
      Map<String, String> nodes = new LinkedHashMap<>();
      try (DBIterator iterator = database.iterator()) {
        iterator.seekToFirst();
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          nodes.put(Hex.toHexString(entry.getKey()), Hex.toHexString(entry.getValue()));
        }
      } catch (IOException failure) {
        throw new IllegalStateException("failed to iterate test LevelDB", failure);
      }
      return nodes;
    }

    @Override
    public void close() {
      try {
        database.close();
      } catch (IOException failure) {
        throw new IllegalStateException("failed to close test LevelDB", failure);
      }
    }
  }

  private static final class RocksPathNodeStore extends EnginePathNodeStore {

    private final org.rocksdb.Options options = new org.rocksdb.Options().setCreateIfMissing(true);
    private final org.rocksdb.RocksDB database;

    private RocksPathNodeStore(Path directory) {
      try {
        Files.createDirectories(directory);
        database = org.rocksdb.RocksDB.open(options, directory.toString());
      } catch (IOException | RocksDBException failure) {
        options.close();
        throw new IllegalStateException("failed to open test RocksDB", failure);
      }
    }

    @Override
    public byte[] get(byte[] path) {
      try {
        return database.get(path);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to read test RocksDB", failure);
      }
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      try {
        database.put(path, encodedNode);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to write test RocksDB", failure);
      }
    }

    @Override
    public void delete(byte[] path) {
      try {
        database.delete(path);
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to delete test RocksDB", failure);
      }
    }

    @Override
    Map<String, String> snapshot() {
      Map<String, String> nodes = new LinkedHashMap<>();
      try (RocksIterator iterator = database.newIterator()) {
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
          nodes.put(Hex.toHexString(iterator.key()), Hex.toHexString(iterator.value()));
        }
        iterator.status();
      } catch (RocksDBException failure) {
        throw new IllegalStateException("failed to iterate test RocksDB", failure);
      }
      return nodes;
    }

    @Override
    public void close() {
      database.close();
      options.close();
    }
  }
}
