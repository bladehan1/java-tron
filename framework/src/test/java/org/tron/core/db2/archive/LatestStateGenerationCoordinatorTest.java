package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.core.SnapshotManager;

public class LatestStateGenerationCoordinatorTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "properties");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void acquiresEveryStoreInsideBarrierAndPublishesBoundGeneration() throws Exception {
    Path root = temporaryFolder.newFolder("coordinator").toPath();
    FakeBarrier barrier = new FakeBarrier();
    FakeStore account = new FakeStore("account", "rocksdb:account", bytes("account-v1"), barrier);
    FakeStore properties = new FakeStore("properties", "leveldb:properties",
        bytes("properties-v1"), barrier);
    Map<String, SnapshotCapableStore> stores = stores(account, properties);
    AtomicReference<ArchiveProgressEnvelope> authority = new AtomicReference<>();

    try (ArchiveHistoryWriter writer = writer(root.resolve("archive"));
        LatestStateGenerationCoordinator coordinator = new LatestStateGenerationCoordinator(
            PARTICIPANTS, stores, barrier::run, authority::get)) {
      writer.accept(diff(1, "account", "key", "old"));
      authority.set(reader(writer.committedHead()));
      try (LatestStateGenerationCoordinator.Candidate candidate =
              coordinator.acquire("generation-1");
          PersistentServingKeyIndexGeneration serving = writer.buildServingGeneration(
              root.resolve("generation-1"), "generation-1",
              candidate.getSourceIdentityDigest())) {
        assertFalse(barrier.active.get());
        assertTrue(coordinator.publish(null, candidate, serving));
        account.value = bytes("account-live-v2");
        try (PinnedLatestState pinned = coordinator.pin(serving)) {
          assertArrayEquals(bytes("account-v1"),
              pinned.get("account", bytes("key")).getValue());
          assertArrayEquals(candidate.getSourceIdentityDigest(),
              pinned.getSourceIdentityDigest());
          assertEquals(1, coordinator.getReferenceCount("generation-1"));
        }
      }
    }
    assertEquals(1, account.closedSnapshots.get());
    assertEquals(1, properties.closedSnapshots.get());
  }

  @Test
  public void authorityDriftAndPartialAcquireReleaseEverySnapshot() throws Exception {
    FakeBarrier barrier = new FakeBarrier();
    FakeStore account = new FakeStore("account", "rocksdb:account", bytes("account"), barrier);
    FakeStore properties = new FakeStore("properties", "leveldb:properties",
        bytes("properties"), barrier);
    AtomicInteger reads = new AtomicInteger();
    LatestStateGenerationCoordinator drifting = new LatestStateGenerationCoordinator(
        PARTICIPANTS, stores(account, properties), barrier::run,
        () -> reads.getAndIncrement() == 0 ? reader(1) : reader(2));
    try {
      assertThrows(ArchivePersistenceException.class,
          () -> drifting.acquire("generation-1"));
      assertFalse(barrier.active.get());
      assertEquals(1, account.closedSnapshots.get());
      assertEquals(1, properties.closedSnapshots.get());
    } finally {
      drifting.close();
    }

    properties.failPin.set(true);
    LatestStateGenerationCoordinator partial = new LatestStateGenerationCoordinator(
        PARTICIPANTS, stores(account, properties), barrier::run, () -> reader(1));
    try {
      assertThrows(IOException.class, () -> partial.acquire("generation-2"));
      assertFalse(barrier.active.get());
      assertEquals(2, account.closedSnapshots.get());
      assertEquals(1, properties.closedSnapshots.get());
    } finally {
      partial.close();
    }
  }

  @Test
  public void pinnedOldGenerationSurvivesReplacementUntilLastReaderCloses() throws Exception {
    Path root = temporaryFolder.newFolder("replacement").toPath();
    FakeBarrier barrier = new FakeBarrier();
    FakeStore account = new FakeStore("account", "rocksdb:account", bytes("account-v1"), barrier);
    FakeStore properties = new FakeStore("properties", "leveldb:properties",
        bytes("properties-v1"), barrier);
    AtomicReference<ArchiveProgressEnvelope> authority = new AtomicReference<>();
    LatestStateGenerationCoordinator coordinator = new LatestStateGenerationCoordinator(
        PARTICIPANTS, stores(account, properties), barrier::run, authority::get);
    PinnedLatestState oldPin = null;
    PinnedLatestState newPin = null;
    try (ArchiveHistoryWriter writer = writer(root.resolve("archive"))) {
      writer.accept(diff(1, "account", "key", "old-1"));
      authority.set(reader(writer.committedHead()));
      try (LatestStateGenerationCoordinator.Candidate first = coordinator.acquire("generation-1");
          PersistentServingKeyIndexGeneration serving1 = writer.buildServingGeneration(
              root.resolve("generation-1"), "generation-1", first.getSourceIdentityDigest())) {
        assertTrue(coordinator.publish(null, first, serving1));
        oldPin = coordinator.pin(serving1);
      }

      account.value = bytes("account-v2");
      properties.value = bytes("properties-v2");
      writer.accept(diff(2, "properties", "key", "old-2"));
      authority.set(reader(writer.committedHead()));
      try (LatestStateGenerationCoordinator.Candidate second = coordinator.acquire("generation-2");
          PersistentServingKeyIndexGeneration serving2 = writer.buildServingGeneration(
              root.resolve("generation-2"), "generation-2", second.getSourceIdentityDigest())) {
        assertTrue(coordinator.publish("generation-1", second, serving2));
        newPin = coordinator.pin(serving2);
        assertArrayEquals(bytes("account-v1"),
            oldPin.get("account", bytes("key")).getValue());
        assertArrayEquals(bytes("account-v2"),
            newPin.get("account", bytes("key")).getValue());
        assertThrows(IOException.class, coordinator::close);
      }

      oldPin.close();
      oldPin = null;
      assertEquals(1, account.closedSnapshots.get());
      assertEquals(1, properties.closedSnapshots.get());
      newPin.close();
      newPin = null;
      coordinator.close();
      assertEquals(2, account.closedSnapshots.get());
      assertEquals(2, properties.closedSnapshots.get());
    } finally {
      if (oldPin != null) {
        oldPin.close();
      }
      if (newPin != null) {
        newPin.close();
      }
      coordinator.close();
    }
  }

  @Test
  public void snapshotManagerBarrierReleasesPartialAcquireAndMonitor() throws Exception {
    SnapshotManager manager = new SnapshotManager("");
    manager.enable();
    SnapshotManagerBarrier barrier = new SnapshotManagerBarrier(manager);
    FakeStore account = new FakeStore("account", "rocksdb:account", bytes("account"),
        barrier.active);
    FakeStore properties = new FakeStore("properties", "leveldb:properties", bytes("properties"),
        barrier.active);
    properties.failPin.set(true);

    try (LatestStateGenerationCoordinator coordinator = new LatestStateGenerationCoordinator(
        PARTICIPANTS, stores(account, properties), barrier::run, () -> reader(1))) {
      assertThrows(IOException.class, () -> coordinator.acquire("generation-1"));
      assertFalse(barrier.active.get());
      assertEquals(1, account.closedSnapshots.get());
      assertEquals(0, properties.closedSnapshots.get());
    }
    try (org.tron.core.db2.ISession ignored = manager.buildSession()) {
      assertEquals(1, manager.size());
    }
    assertEquals(0, manager.size());
  }

  private static ArchiveHistoryWriter writer(Path archive) throws IOException {
    return new ArchiveHistoryWriter(archive, 4096, new LinkedHashSet<>(PARTICIPANTS));
  }

  private static BlockReverseDiff diff(int block, String database, String key, String oldValue) {
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block), hash(block - 1),
        block * 1_000L), Collections.singletonList(new BlockReverseDiff.DbGroup(database,
        Collections.singletonList(new BlockReverseDiff.Entry(bytes(key),
            OldValue.present(bytes(oldValue)))))));
  }

  private static ArchiveProgressEnvelope reader(HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope reader(int epoch) {
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, epoch,
        hash(epoch), new byte[16], new byte[32], PARTICIPANTS);
  }

  private static Map<String, SnapshotCapableStore> stores(FakeStore... stores) {
    Map<String, SnapshotCapableStore> indexed = new LinkedHashMap<>();
    for (FakeStore store : stores) {
      indexed.put(store.dbName, store);
    }
    return indexed;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class FakeBarrier {
    private final AtomicBoolean active = new AtomicBoolean();

    private void run(ArchiveStateBarrier.ArchiveStateAction action) throws IOException {
      if (!active.compareAndSet(false, true)) {
        throw new IllegalStateException("barrier is already active");
      }
      try {
        action.run();
      } finally {
        if (!active.compareAndSet(true, false)) {
          throw new IllegalStateException("barrier is not active");
        }
      }
    }
  }

  private static final class SnapshotManagerBarrier {
    private final SnapshotManager manager;
    private final AtomicBoolean active = new AtomicBoolean();

    private SnapshotManagerBarrier(SnapshotManager manager) {
      this.manager = manager;
    }

    private void run(ArchiveStateBarrier.ArchiveStateAction action) throws IOException {
      manager.withArchiveStateBarrier(() -> {
        if (!active.compareAndSet(false, true)) {
          throw new IllegalStateException("barrier is already active");
        }
        try {
          action.run();
        } finally {
          if (!active.compareAndSet(true, false)) {
            throw new IllegalStateException("barrier is not active");
          }
        }
      });
    }
  }

  private static final class FakeStore implements SnapshotCapableStore {
    private final String dbName;
    private final String sourceIdentity;
    private final AtomicBoolean barrierActive;
    private final AtomicBoolean failPin = new AtomicBoolean();
    private final AtomicInteger closedSnapshots = new AtomicInteger();
    private byte[] value;

    private FakeStore(String dbName, String sourceIdentity, byte[] value, FakeBarrier barrier) {
      this(dbName, sourceIdentity, value, barrier.active);
    }

    private FakeStore(String dbName, String sourceIdentity, byte[] value,
        AtomicBoolean barrierActive) {
      this.dbName = dbName;
      this.sourceIdentity = sourceIdentity;
      this.value = value;
      this.barrierActive = barrierActive;
    }

    @Override
    public String getDbName() {
      return dbName;
    }

    @Override
    public String getSourceIdentity() {
      return sourceIdentity;
    }

    @Override
    public StoreSnapshot pin(long blockNumber, byte[] blockHash) throws IOException {
      if (!barrierActive.get()) {
        throw new AssertionError("Store snapshot acquired outside global barrier");
      }
      if (failPin.get()) {
        throw new IOException("injected Store pin failure");
      }
      byte[] pinnedValue = Arrays.copyOf(value, value.length);
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
          return Arrays.copyOf(blockHash, blockHash.length);
        }

        @Override
        public byte[] get(byte[] physicalRawKey) {
          return Arrays.copyOf(pinnedValue, pinnedValue.length);
        }

        @Override
        public void close() {
          if (!closed) {
            closed = true;
            closedSnapshots.incrementAndGet();
          }
        }
      };
    }
  }
}
