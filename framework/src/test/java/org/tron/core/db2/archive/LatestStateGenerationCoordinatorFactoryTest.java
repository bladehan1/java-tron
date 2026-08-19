package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.tron.common.TestConstants.TEST_CONF;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.config.args.Args;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.LevelDB;
import org.tron.core.db2.common.RocksDB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;

public class LatestStateGenerationCoordinatorFactoryTest {

  private final List<Registry> openRegistries = new ArrayList<>();

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  @BeforeClass
  public static void initConfiguration() {
    Args.setParam(new String[0], TEST_CONF);
  }

  @AfterClass
  public static void clearConfiguration() {
    Args.clearParam();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void closeEngines() {
    openRegistries.forEach(Registry::close);
    openRegistries.clear();
  }

  @Test
  public void assemblesExactMixedEnginesAndIgnoresDerivedStore() throws Exception {
    Registry registry = registry(true);
    Path readerVisible = temporaryFolder.newFile("reader-visible").toPath();
    storeReader(readerVisible, 1, registry.participants);

    try (LatestStateGenerationCoordinator coordinator =
            LatestStateGenerationCoordinatorFactory.create(registry.manager, readerVisible);
        LatestStateGenerationCoordinator.Candidate candidate =
            coordinator.acquire("generation-1")) {
      assertEquals(ArchiveStoreScope.getStateDatabases().size(), totalPins(registry));
    }
    assertEquals(ArchiveStoreScope.getStateDatabases().size(), totalCloses(registry));
  }

  @Test
  public void rejectsMissingDuplicateAndNonCapableStateRoots() throws Exception {
    Path readerVisible = temporaryFolder.newFile("invalid-reader-visible").toPath();
    Registry missing = registry(false);
    missing.manager.getDbs().removeIf(database -> "witness".equals(database.getDbName()));
    assertThrows(ArchivePersistenceException.class,
        () -> LatestStateGenerationCoordinatorFactory.create(missing.manager, readerVisible));

    Registry duplicate = registry(false);
    duplicate.manager.getDbs().add(new Chainbase(new SnapshotRoot(
        duplicate.engines.get("account"))));
    assertThrows(ArchivePersistenceException.class,
        () -> LatestStateGenerationCoordinatorFactory.create(duplicate.manager, readerVisible));

    SnapshotManager nonCapable = new SnapshotManager("");
    for (String participant : sortedParticipants()) {
      nonCapable.getDbs().add(new Chainbase(new SnapshotRoot(nonCapable(participant))));
    }
    assertThrows(ArchivePersistenceException.class,
        () -> LatestStateGenerationCoordinatorFactory.create(nonCapable, readerVisible));
  }

  @Test
  public void rejectsReaderSetMismatchAndReleasesPartialAcquire() throws Exception {
    Registry registry = registry(false);
    Path readerVisible = temporaryFolder.newFile("partial-reader-visible").toPath();
    List<String> unexpectedDerived = new ArrayList<>(registry.participants);
    unexpectedDerived.add("accountTrie");
    java.util.Collections.sort(unexpectedDerived);
    storeReader(readerVisible, 1, unexpectedDerived);

    try (LatestStateGenerationCoordinator coordinator =
        LatestStateGenerationCoordinatorFactory.create(registry.manager, readerVisible)) {
      assertThrows(ArchivePersistenceException.class,
          () -> coordinator.acquire("generation-mismatch"));
      assertEquals(0, totalPins(registry));

      storeReader(readerVisible, 1, registry.participants);
      registry.probes.get("properties").failPin.set(true);
      assertThrows(IllegalStateException.class,
          () -> coordinator.acquire("generation-partial"));
      assertTrue(totalPins(registry) > 0);
      assertEquals(totalPins(registry), totalCloses(registry));
    }
    AtomicBoolean reentered = new AtomicBoolean();
    registry.manager.withArchiveStateBarrier(() -> reentered.set(true));
    assertTrue(reentered.get());
  }

  @Test
  public void readerDriftClosesEveryMixedEngineSnapshot() throws Exception {
    Registry registry = registry(false);
    Path readerVisible = temporaryFolder.newFile("drifting-reader-visible").toPath();
    storeReader(readerVisible, 1, registry.participants);
    AtomicBoolean changed = new AtomicBoolean();
    registry.probes.firstEntry().getValue().onPin = () -> {
      if (changed.compareAndSet(false, true)) {
        try {
          storeReader(readerVisible, 2, registry.participants);
        } catch (java.io.IOException failure) {
          throw new UncheckedIOException(failure);
        }
      }
    };

    try (LatestStateGenerationCoordinator coordinator =
        LatestStateGenerationCoordinatorFactory.create(registry.manager, readerVisible)) {
      assertThrows(ArchivePersistenceException.class,
          () -> coordinator.acquire("generation-drift"));
    }
    assertEquals(ArchiveStoreScope.getStateDatabases().size(), totalPins(registry));
    assertEquals(totalPins(registry), totalCloses(registry));
  }

  private Registry registry(boolean includeDerived) {
    SnapshotManager manager = new SnapshotManager("");
    TreeMap<String, Probe> probes = new TreeMap<>();
    TreeMap<String, DB<byte[], byte[]>> engines = new TreeMap<>();
    List<String> participants = sortedParticipants();
    for (int index = 0; index < participants.size(); index++) {
      String participant = participants.get(index);
      Probe probe = new Probe(participant, (index & 1) == 0 ? "leveldb" : "rocksdb");
      DB<byte[], byte[]> engine = (index & 1) == 0
          ? new FakeLevelDB(probe) : new FakeRocksDB(probe);
      probes.put(participant, probe);
      engines.put(participant, engine);
      manager.getDbs().add(new Chainbase(new SnapshotRoot(engine)));
    }
    if (includeDerived) {
      manager.getDbs().add(new Chainbase(new SnapshotRoot(nonCapable("accountTrie"))));
    }
    Registry registry = new Registry(manager, participants, probes, engines);
    openRegistries.add(registry);
    return registry;
  }

  @SuppressWarnings("unchecked")
  private static DB<byte[], byte[]> nonCapable(String dbName) {
    DB<byte[], byte[]> database = mock(DB.class);
    when(database.getDbName()).thenReturn(dbName);
    return database;
  }

  private static List<String> sortedParticipants() {
    String[] participants = ArchiveStoreScope.getStateDatabases().toArray(new String[0]);
    Arrays.sort(participants);
    return Arrays.asList(participants);
  }

  private static void storeReader(Path path, int epoch, List<String> participants)
      throws java.io.IOException {
    new ArchiveProgressFile(path, new ArchiveProgressEnvelopeCodec()).store(
        new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null, epoch, hash(epoch), new byte[16],
            new byte[32], participants));
  }

  private static int totalPins(Registry registry) {
    return registry.probes.values().stream().mapToInt(probe -> probe.pins.get()).sum();
  }

  private static int totalCloses(Registry registry) {
    return registry.probes.values().stream().mapToInt(probe -> probe.closes.get()).sum();
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class Registry {
    private final SnapshotManager manager;
    private final List<String> participants;
    private final TreeMap<String, Probe> probes;
    private final Map<String, DB<byte[], byte[]>> engines;

    private Registry(SnapshotManager manager, List<String> participants,
        TreeMap<String, Probe> probes, Map<String, DB<byte[], byte[]>> engines) {
      this.manager = manager;
      this.participants = participants;
      this.probes = probes;
      this.engines = engines;
    }

    private void close() {
      engines.values().forEach(DB::close);
    }
  }

  private static final class Probe {
    private final String dbName;
    private final String sourceIdentity;
    private final AtomicBoolean failPin = new AtomicBoolean();
    private final AtomicInteger pins = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private Runnable onPin = () -> { };

    private Probe(String dbName, String engine) {
      this.dbName = dbName;
      this.sourceIdentity = engine + ":" + dbName + ":00000000-0000-0000-0000-000000000001";
    }

    private StoreSnapshot pin(long blockNumber, byte[] blockHash) {
      if (failPin.get()) {
        throw new IllegalStateException("injected engine pin failure: " + dbName);
      }
      pins.incrementAndGet();
      onPin.run();
      byte[] expectedHash = Arrays.copyOf(blockHash, blockHash.length);
      return new StoreSnapshot() {
        private boolean closed;

        @Override
        public String getDbName() {
          return dbName;
        }

        @Override
        public String getSourceIdentity() {
          return sourceIdentity;
        }

        @Override
        public long getBlockNumber() {
          return blockNumber;
        }

        @Override
        public byte[] getBlockHash() {
          return Arrays.copyOf(expectedHash, expectedHash.length);
        }

        @Override
        public byte[] get(byte[] physicalRawKey) {
          return null;
        }

        @Override
        public void close() {
          if (!closed) {
            closed = true;
            closes.incrementAndGet();
          }
        }
      };
    }
  }

  private static final class FakeLevelDB extends LevelDB {
    private final Probe probe;

    private FakeLevelDB(Probe probe) {
      super(mock(LevelDbDataSourceImpl.class));
      this.probe = probe;
    }

    @Override
    public String getDbName() {
      return probe.dbName;
    }

    @Override
    public String getSourceIdentity() {
      return probe.sourceIdentity;
    }

    @Override
    public StoreSnapshot pin(long blockNumber, byte[] blockHash) {
      return probe.pin(blockNumber, blockHash);
    }
  }

  private static final class FakeRocksDB extends RocksDB {
    private final Probe probe;

    private FakeRocksDB(Probe probe) {
      super(mock(RocksDbDataSourceImpl.class));
      this.probe = probe;
    }

    @Override
    public String getDbName() {
      return probe.dbName;
    }

    @Override
    public String getSourceIdentity() {
      return probe.sourceIdentity;
    }

    @Override
    public StoreSnapshot pin(long blockNumber, byte[] blockHash) {
      return probe.pin(blockNumber, blockHash);
    }
  }
}
