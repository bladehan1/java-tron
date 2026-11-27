package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.Descriptor;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.source.ServingHistorySource;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.Committed;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.CommittedBlock;
import org.tron.core.db2.archive.serving.status.ServingFailure.Reason;
import org.tron.core.db2.archive.serving.status.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.StateArchiveIndexEngineManifest;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * ServingIndexStore (ch04 WP-S2): creation and validation, the state record, normal batches,
 * key-atomic solo chunks with R-IDEM (T5 component part, S-INV-13), mixed 32/48-byte
 * storage-row keys (T17), and the planner's deterministic selection (T4/T6 component part).
 * RocksDB only: LevelDB JNI is unavailable on arm64 (the LevelDB variant needs an x86 run).
 */
public class ServingIndexStoreTest {

  private static final long B0 = 101;


  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void freshStoreWritesInitialDescriptorCoverageAndStateRecord() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, true)) {
      assertTrue(store.isInitialized());
      assertEquals(B0 - 1, store.indexedFrom());
      assertEquals(B0 - 1, store.indexedThrough());
      assertArrayEquals(hash(B0 - 1), store.headHash());
      assertTrue(store.isRebuildInProgress());
      assertEquals(B0, store.stateRecord().getHistoryStartBlock());
    }
    try (ServingIndexStore reopened = open(directory, false)) {
      assertFalse(reopened.isInitialized());
      assertTrue("the durable flag survives a restart", reopened.isRebuildInProgress());
      reopened.clearRebuildFlag();
    }
    try (ServingIndexStore reopened = open(directory, false)) {
      assertFalse(reopened.isRebuildInProgress());
    }
    ServingFaultException identity = assertThrows(ServingFaultException.class,
        () -> ServingIndexStore.open(directory, Engine.ROCKSDB, B0, hash(7), false, 1));
    assertEquals(Reason.IDENTITY, identity.getReason());
    ServingFaultException otherStart = assertThrows(ServingFaultException.class,
        () -> ServingIndexStore.open(directory, Engine.ROCKSDB, B0 + 1, hash(B0), false, 1));
    assertEquals(Reason.IDENTITY, otherStart.getReason());
  }

  @Test
  public void legacyTailOnlyDatabaseIsInitializedAndTheKeyIsDeleted() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    Files.createDirectories(directory);
    StateArchiveIndexEngineManifest.openOrCreate(directory, Engine.ROCKSDB);
    try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(ArchiveIndexStorage.put(
          PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5, new byte[]{1, 2, 3})), true);
    }
    try (ServingIndexStore store = open(directory, false)) {
      assertTrue(store.isInitialized());
      assertEquals(B0 - 1, store.indexedThrough());
      try (ArchiveIndexStorage.Reader snapshot = store.snapshot()) {
        assertNull(snapshot.get(PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5));
      }
    }
  }

  @Test
  public void dataWithoutDescriptorAndForeignEngineAreClassified() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    Files.createDirectories(directory);
    StateArchiveIndexEngineManifest.openOrCreate(directory, Engine.ROCKSDB);
    try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(ArchiveIndexStorage.put(new byte[]{3, 0, 1},
          new byte[]{1})), true);
    }
    ServingFaultException corrupt = assertThrows(ServingFaultException.class,
        () -> open(directory, false));
    assertEquals(Reason.INDEX_CORRUPT, corrupt.getReason());
    ServingFaultException engine = assertThrows(ServingFaultException.class,
        () -> ServingIndexStore.open(directory, Engine.LEVELDB, B0, hash(B0 - 1), false, 1));
    assertEquals(Reason.INDEX_INCOMPATIBLE, engine.getReason());
    Path legacy = temporaryFolder.newFolder().toPath();
    Files.createDirectories(legacy.resolve("current"));
    ServingFaultException current = assertThrows(ServingFaultException.class,
        () -> open(legacy.resolve("single-v1"), false));
    assertEquals(Reason.INDEX_INCOMPATIBLE, current.getReason());
    assertFalse(Files.exists(legacy.resolve("single-v1")));
  }

  @Test
  public void corruptDescriptorIsIndexCorrupt() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, false)) {
      store.commit(plan(store, blocks(B0, 2, 3)), false);
    }
    try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      byte[] encoded = raw.get(PersistentServingKeyIndexGeneration.PROGRESS_KEY);
      encoded[encoded.length - 1] ^= 1;
      raw.write(Collections.singletonList(ArchiveIndexStorage.put(
          PersistentServingKeyIndexGeneration.PROGRESS_KEY, encoded)), true);
    }
    ServingFaultException corrupt = assertThrows(ServingFaultException.class,
        () -> open(directory, false));
    assertEquals(Reason.INDEX_CORRUPT, corrupt.getReason());
  }

  @Test
  public void atomicBatchReopensWithAllDataAndProgress() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, false)) {
      store.commit(plan(store, blocks(B0, 3, 40)), false);
      assertEquals(B0 + 2, store.indexedThrough());
    }
    try (ServingIndexStore reopened = open(directory, false)) {
      assertEquals(B0 + 2, reopened.indexedThrough());
      assertEquals(120, reopened.keyChanges());
    }
  }

  /** Old versions could stop between key groups at I+1; the new atomic writer must replay it. */
  @Test(timeout = 120_000)
  public void interruptedLegacySoloConvergesWithNewAtomicWriter() throws Exception {
    int keys = 8;
    // Residue keys cover absent/inline/inline-to-page and partial/full page transitions.
    int[] prior = {0, 1, 4, 5, 511, 512, 513, 520};
    List<BlockReverseDiff> prefix = new ArrayList<>();
    for (int round = 0; round < 520; round++) {
      List<byte[]> changed = new ArrayList<>();
      for (int key = 0; key < keys; key++) {
        if (round < prior[key]) {
          changed.add(key(key));
        }
      }
      prefix.add(diff(B0 + round, changed));
    }
    long solo = B0 + prefix.size();
    BlockReverseDiff soloDiff = blocks(solo, 1, keys).get(0);
    for (int written = 1; written < keys; written++) {
      Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
      try (ServingIndexStore store = open(directory, false)) {
        store.commit(plan(store, prefix), false);
      }
      try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
          directory.resolve("keys"), Engine.ROCKSDB)) {
        for (int key = 0; key < written; key++) {
          List<ArchiveIndexStorage.Mutation> mutations = new ArrayList<>();
          PersistentServingKeyIndexGeneration.appendExactChanges(raw, mutations,
              new PersistentServingKeyIndexGeneration.ExactKey("code", key(key)),
              Collections.singletonList(solo), solo - 1);
          raw.write(mutations, true); // No progress/coverage: exact old solo residue.
        }
      }
      try (ServingIndexStore reopened = open(directory, false)) {
        assertEquals(solo - 1, reopened.indexedThrough());
        try (ArchiveIndexStorage.Reader snapshot = reopened.snapshot();
            PersistentServingKeyIndexGeneration view =
                PersistentServingKeyIndexGeneration.overSnapshot(directory,
                    ServingIndexStore.readDescriptor(snapshot), Engine.ROCKSDB, snapshot)) {
          assertEquals(OptionalLong.empty(),
              view.firstChangeAfter("code", key(0), solo - 1, solo - 1));
        }
        reopened.commit(plan(reopened, Collections.singletonList(soloDiff)), false);
        Oracle complete = new Oracle();
        complete.recordAll(prefix);
        complete.record(soloDiff);
        complete.verify(reopened, directory, keys, solo);
      }
    }
  }

  @Test
  public void ridemRejectsAKeyMetaAheadOfTheSoloResidue() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, false)) {
      store.commit(plan(store, blocks(B0, 1, 2)), false);
    }
    // Forge a KeyMeta whose last epoch is I + 2: never produced by serving.
    try (ServingIndexStore other = open(temporaryFolder.newFolder().toPath()
        .resolve("single-v1"), false)) {
      other.commit(plan(other, blocks(B0, 3, 2)), false);
      byte[] metaKey = PersistentServingKeyIndexGeneration.keyMetaKey("code", key(0));
      byte[] forged;
      try (ArchiveIndexStorage.Reader snapshot = other.snapshot()) {
        forged = snapshot.get(metaKey);
      }
      try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
          directory.resolve("keys"), Engine.ROCKSDB)) {
        raw.write(Collections.singletonList(ArchiveIndexStorage.put(metaKey, forged)),
            true);
      }
    }
    try (ServingIndexStore store = open(directory, false)) {
      assertThrows(ArchivePersistenceException.class,
          () -> store.commit(plan(store, blocks(B0 + 1, 1, 2)), false));
    }
  }

  /** T17: 32- and 48-byte storage-row keys of one contract keep independent epoch lists. */
  @Test
  public void legacyAndOptimizedStorageRowKeysAreIndependent() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    byte[] legacy = new byte[32];
    Arrays.fill(legacy, (byte) 3);
    byte[] optimized = Arrays.copyOf(legacy, 48);
    optimized[47] = 9;
    List<BlockReverseDiff> diffs = new ArrayList<>();
    diffs.add(storage(B0, legacy));
    diffs.add(storage(B0 + 1, optimized));
    diffs.add(storage(B0 + 2, legacy, optimized));
    try (ServingIndexStore store = open(directory, false)) {
      store.commit(plan(store, diffs), false);
      try (ArchiveIndexStorage.Reader snapshot = store.snapshot();
          PersistentServingKeyIndexGeneration view =
              PersistentServingKeyIndexGeneration.overSnapshot(directory,
                  ServingIndexStore.readDescriptor(snapshot), Engine.ROCKSDB, snapshot)) {
        long i = B0 + 2;
        assertEquals(OptionalLong.of(B0), view.firstChangeAfter("storage-row", legacy,
            B0 - 1, i));
        assertEquals(OptionalLong.of(B0 + 2), view.firstChangeAfter("storage-row", legacy,
            B0, i));
        assertEquals(OptionalLong.of(B0 + 1), view.firstChangeAfter("storage-row", optimized,
            B0 - 1, i));
        assertEquals(OptionalLong.of(B0 + 2), view.firstChangeAfter("storage-row", optimized,
            B0 + 1, i));
      }
    }
  }

  /** The read window admits an oversized first block and bounds following blocks. */
  @Test
  public void plannerSelectsByAccumulatedBytes() throws Exception {
    ServingIndexService.Config config = ServingIndexService.Config.defaults()
        .setMaxSourceBytes(32L * 1024 * 1024).setMaxBlocks(1000);
    ServingBatchPlanner planner = new ServingBatchPlanner(config);
    long mib = 1024L * 1024;
    assertEquals("RANGE(11,11)", planner.select(sizes(10 * mib, 40 * mib), null, 10, 12)
        .toString());
    assertEquals("RANGE(11,11)", planner.select(sizes(40 * mib, mib), null, 10, 12).toString());
    assertEquals("RANGE(11,12)", planner.select(sizes(17 * mib, 15 * mib), null, 10, 12)
        .toString());
    // T4: 2 x 17 MiB live range splits into two batches, never a failure.
    assertEquals("RANGE(11,11)", planner.select(sizes(17 * mib, 17 * mib), null, 10, 12)
        .toString());
    assertNull(planner.select(sizes(), null, 12, 12));
  }

  private static ServingHistorySource sizes(long... sizes) {
    return new ServingHistorySource() {
      @Override
      public Committed committed() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long[] bundleBytes(Committed view, long first, long last) {
        return Arrays.copyOf(sizes, (int) (last - first + 1));
      }

      @Override
      public List<CommittedBlock> read(Committed view, long first, long last) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] blockHash(Committed view, long block) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static ServingIndexStore open(Path directory, boolean rebuild) throws IOException {
    return ServingIndexStore.open(directory, Engine.ROCKSDB, B0, hash(B0 - 1), rebuild, 1);
  }

  static ServingIndexIncrementalPlan plan(ServingIndexStore store,
      List<BlockReverseDiff> diffs) {
    return ServingIndexIncrementalPlan.planCommittedDiffs(store.indexedThrough(),
        store.headHash(), diffs);
  }

  static List<BlockReverseDiff> blocks(long first, int count, int keysPerBlock) {
    List<BlockReverseDiff> result = new ArrayList<>();
    for (long block = first; block < first + count; block++) {
      List<byte[]> keys = new ArrayList<>();
      for (int key = 0; key < keysPerBlock; key++) {
        keys.add(key(key));
      }
      result.add(diff(block, keys));
    }
    return result;
  }

  static BlockReverseDiff diff(long block, List<byte[]> keys) {
    List<BlockReverseDiff.Entry> entries = new ArrayList<>();
    for (byte[] key : keys) {
      entries.add(new BlockReverseDiff.Entry(key, OldValue.present(hash(block))));
    }
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L), entries.isEmpty() ? Collections.emptyList()
        : Collections.singletonList(new BlockReverseDiff.DbGroup("code", entries)));
  }

  private static BlockReverseDiff storage(long block, byte[]... keys) {
    List<BlockReverseDiff.Entry> entries = new ArrayList<>();
    List<byte[]> sorted = new ArrayList<>(Arrays.asList(keys));
    sorted.sort(com.google.common.primitives.UnsignedBytes.lexicographicalComparator());
    for (byte[] key : sorted) {
      entries.add(new BlockReverseDiff.Entry(key, OldValue.present(hash(block))));
    }
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L), Collections.singletonList(
            new BlockReverseDiff.DbGroup("storage-row", entries)));
  }

  static byte[] key(int key) {
    return ByteBuffer.allocate(4).putInt(key).array();
  }

  static byte[] hash(long value) {
    return ByteBuffer.allocate(32).putLong(value).array();
  }

  /** Overrides the build-thread count; the returned value restores the JVM-supplied one. */
  static String setBuildThreads(int threads) {
    String previous = System.getProperty(ServingIndexParallelAppender.THREADS_PROPERTY);
    System.setProperty(ServingIndexParallelAppender.THREADS_PROPERTY,
        Integer.toString(threads));
    return previous;
  }

  static void restoreBuildThreads(String previous) {
    if (previous == null) {
      System.clearProperty(ServingIndexParallelAppender.THREADS_PROPERTY);
    } else {
      System.setProperty(ServingIndexParallelAppender.THREADS_PROPERTY, previous);
    }
  }

  /** Expected epoch lists per key. */
  private static final class Oracle {
    private final Map<Integer, List<Long>> epochs = new TreeMap<>();

    void recordAll(List<BlockReverseDiff> diffs) {
      diffs.forEach(this::record);
    }

    void record(BlockReverseDiff diff) {
      for (BlockReverseDiff.DbGroup group : diff.getGroups()) {
        for (BlockReverseDiff.Entry entry : group.getEntries()) {
          epochs.computeIfAbsent(ByteBuffer.wrap(entry.getKey()).getInt(),
              ignored -> new ArrayList<>()).add(diff.getMeta().getBlockNumber());
        }
      }
    }

    void verify(ServingIndexStore store, Path directory, int keys, long through)
        throws IOException {
      try (ArchiveIndexStorage.Reader snapshot = store.snapshot();
          PersistentServingKeyIndexGeneration view =
              PersistentServingKeyIndexGeneration.overSnapshot(directory,
                  ServingIndexStore.readDescriptor(snapshot), Engine.ROCKSDB, snapshot)) {
        for (int key = 0; key < keys; key++) {
          List<Long> expected = epochs.getOrDefault(key, Collections.emptyList());
          // Probe every boundary of this key, plus the window ends.
          List<Long> targets = new ArrayList<>(expected);
          targets.add(B0 - 1);
          targets.add(through - 1);
          for (long target : targets) {
            if (target < B0 - 1 || target > through) {
              continue;
            }
            Long next = null;
            for (long epoch : expected) {
              if (epoch > target) {
                next = epoch;
                break;
              }
            }
            OptionalLong actual = view.firstChangeAfter("code", key(key), target, through);
            assertEquals("key " + key + " after " + target,
                next == null ? OptionalLong.empty() : OptionalLong.of(next), actual);
          }
        }
        assertNotNull(view.getHeadHash());
      }
    }
  }

}
