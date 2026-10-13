package org.tron.core.db2.archive;

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
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.Descriptor;
import org.tron.core.db2.archive.ServingFailure.Reason;
import org.tron.core.db2.archive.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.ServingHistorySource.Committed;
import org.tron.core.db2.archive.ServingHistorySource.CommittedBlock;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/**
 * ServingIndexStore (ch04 WP-S2): creation and validation, the state record, normal batches,
 * key-atomic solo chunks with R-IDEM (T5 component part, S-INV-13), mixed 32/48-byte
 * storage-row keys (T17), and the planner's deterministic selection (T4/T6 component part).
 * RocksDB only: LevelDB JNI is unavailable on arm64 (the LevelDB variant needs an x86 run).
 */
public class ServingIndexStoreTest {

  private static final long B0 = 101;
  private static final ServingIndexStore.WriteBudget LARGE =
      new ServingIndexStore.WriteBudget(500_000, 64L * 1024 * 1024);
  private static final ServingIndexStore.WriteBudget TINY =
      new ServingIndexStore.WriteBudget(64, 64 * 1024);

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
    try (StateArchiveIndexDatabase.Writer raw = StateArchiveIndexDatabase.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(StateArchiveIndexDatabase.put(
          PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5, new byte[]{1, 2, 3})), true);
    }
    try (ServingIndexStore store = open(directory, false)) {
      assertTrue(store.isInitialized());
      assertEquals(B0 - 1, store.indexedThrough());
      try (StateArchiveIndexDatabase.Reader snapshot = store.snapshot()) {
        assertNull(snapshot.get(PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5));
      }
    }
  }

  @Test
  public void dataWithoutDescriptorAndForeignEngineAreClassified() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    Files.createDirectories(directory);
    StateArchiveIndexEngineManifest.openOrCreate(directory, Engine.ROCKSDB);
    try (StateArchiveIndexDatabase.Writer raw = StateArchiveIndexDatabase.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(StateArchiveIndexDatabase.put(new byte[]{3, 0, 1},
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
      assertTrue(store.commit(plan(store, blocks(B0, 2, 3)), LARGE, false));
    }
    try (StateArchiveIndexDatabase.Writer raw = StateArchiveIndexDatabase.openWriter(
        directory.resolve("keys"), Engine.ROCKSDB)) {
      byte[] encoded = raw.get(PersistentServingKeyIndexGeneration.PROGRESS_KEY);
      encoded[encoded.length - 1] ^= 1;
      raw.write(Collections.singletonList(StateArchiveIndexDatabase.put(
          PersistentServingKeyIndexGeneration.PROGRESS_KEY, encoded)), true);
    }
    ServingFaultException corrupt = assertThrows(ServingFaultException.class,
        () -> open(directory, false));
    assertEquals(Reason.INDEX_CORRUPT, corrupt.getReason());
  }

  @Test
  public void normalBatchOverWriteBudgetWritesNothing() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, false)) {
      ServingIndexIncrementalPlan plan = plan(store, blocks(B0, 3, 40));
      assertFalse(store.commit(plan, TINY, false));
      assertEquals(B0 - 1, store.indexedThrough());
      assertTrue(store.commit(plan, LARGE, false));
      assertEquals(B0 + 2, store.indexedThrough());
      assertEquals(120, store.keyChanges());
    }
  }

  /**
   * T5 (component): a solo batch larger than the write budget is chunked by key. Interrupting it
   * between any two chunks leaves pages and KeyMeta of every key together (S-INV-13), hides
   * {@code I + 1} from queries, and a replay converges through R-IDEM. Covers inline, inline to
   * paged, partial tail-page rewrite and full-page append on both sides of chunk boundaries.
   */
  @Test(timeout = 120_000)
  public void interruptedSoloConvergesAtEveryChunkBoundary() throws Exception {
    int keys = 240;
    // Prior history: key k has k % 7 earlier changes, keys >= 200 have 511/512/513+ changes.
    List<BlockReverseDiff> prefix = new ArrayList<>();
    long block = B0;
    for (int round = 0; round < 520; round++, block++) {
      List<byte[]> changed = new ArrayList<>();
      for (int key = 0; key < keys; key++) {
        int prior = key >= 200 ? 505 + (key - 200) % 9 : key % 7;
        if (round < prior) {
          changed.add(key(key));
        }
      }
      prefix.add(diff(block, changed));
    }
    long solo = block;
    List<byte[]> all = new ArrayList<>();
    for (int key = 0; key < keys; key++) {
      all.add(key(key));
    }
    BlockReverseDiff soloDiff = diff(solo, all);
    int chunkCount = -1;
    for (int interruptAt = 1; ; interruptAt++) {
      Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
      try (ServingIndexStore store = open(directory, false)) {
        for (int start = 0; start < prefix.size(); start += 100) {
          assertTrue(store.commit(plan(store, prefix.subList(start,
              Math.min(prefix.size(), start + 100))), LARGE, false));
        }
        int stopAt = interruptAt;
        int[] seen = new int[1];
        IOException halt = assertThrowsOrNull(() -> store.commitSolo(
            plan(store, Collections.singletonList(soloDiff)), TINY, false, (index, count) -> {
              seen[0] = count;
              if (index == stopAt) {
                throw new IOException("injected halt before chunk " + index);
              }
            }));
        chunkCount = seen[0];
        assertTrue("solo must need several chunks", chunkCount > 3);
        if (halt == null) {
          break;
        }
        assertEquals(solo - 1, store.indexedThrough());
      }
      try (ServingIndexStore reopened = open(directory, false)) {
        assertEquals(solo - 1, reopened.indexedThrough());
        // Key atomicity: every key either fully has the solo epoch or not at all.
        Descriptor peek = peekDescriptor(reopened, solo);
        int written = 0;
        try (StateArchiveIndexDatabase.Reader snapshot = reopened.snapshot();
            PersistentServingKeyIndexGeneration raw =
                PersistentServingKeyIndexGeneration.overSnapshot(directory, peek,
                    Engine.ROCKSDB, snapshot)) {
          for (int key = 0; key < keys; key++) {
            OptionalLong seen = raw.firstChangeAfter("code", key(key), solo - 1, solo);
            if (seen.isPresent()) {
              assertEquals(solo, seen.getAsLong());
              written++;
            }
          }
        }
        assertTrue(written > 0 && written < keys);
        // Queries bounded by I never see the residue.
        try (StateArchiveIndexDatabase.Reader snapshot = reopened.snapshot();
            PersistentServingKeyIndexGeneration view =
                PersistentServingKeyIndexGeneration.overSnapshot(directory,
                    ServingIndexStore.readDescriptor(snapshot), Engine.ROCKSDB, snapshot)) {
          for (int key = 0; key < keys; key++) {
            assertEquals(OptionalLong.empty(),
                view.firstChangeAfter("code", key(key), solo - 1, solo - 1));
          }
        }
        reopened.commitSolo(plan(reopened, Collections.singletonList(soloDiff)), TINY, false,
            ServingIndexStore.ChunkHook.NONE);
        assertEquals(solo, reopened.indexedThrough());
        Oracle complete = new Oracle();
        complete.recordAll(prefix);
        complete.record(soloDiff);
        complete.verify(reopened, directory, keys, solo);
      }
      if (interruptAt >= chunkCount - 1) {
        break;
      }
    }
  }

  @Test
  public void ridemRejectsAKeyMetaAheadOfTheSoloResidue() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = open(directory, false)) {
      assertTrue(store.commit(plan(store, blocks(B0, 1, 2)), LARGE, false));
    }
    // Forge a KeyMeta whose last epoch is I + 2: never produced by serving.
    try (ServingIndexStore other = open(temporaryFolder.newFolder().toPath()
        .resolve("single-v1"), false)) {
      assertTrue(other.commit(plan(other, blocks(B0, 3, 2)), LARGE, false));
      byte[] metaKey = PersistentServingKeyIndexGeneration.keyMetaKey("code", key(0));
      byte[] forged;
      try (StateArchiveIndexDatabase.Reader snapshot = other.snapshot()) {
        forged = snapshot.get(metaKey);
      }
      try (StateArchiveIndexDatabase.Writer raw = StateArchiveIndexDatabase.openWriter(
          directory.resolve("keys"), Engine.ROCKSDB)) {
        raw.write(Collections.singletonList(StateArchiveIndexDatabase.put(metaKey, forged)),
            true);
      }
    }
    try (ServingIndexStore store = open(directory, false)) {
      assertThrows(ArchivePersistenceException.class,
          () -> store.commit(plan(store, blocks(B0 + 1, 1, 2)), LARGE, false));
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
      assertTrue(store.commit(plan(store, diffs), LARGE, false));
      try (StateArchiveIndexDatabase.Reader snapshot = store.snapshot();
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

  /** §3.5.3: SOLO is decided by the accumulated bytes, not by the last size read. */
  @Test
  public void plannerSelectsByAccumulatedBytes() throws Exception {
    ServingIndexService.Config config = ServingIndexService.Config.defaults()
        .setMaxSourceBytes(32L * 1024 * 1024).setMaxBlocks(1000);
    ServingBatchPlanner planner = new ServingBatchPlanner(config);
    long mib = 1024L * 1024;
    assertEquals("RANGE(11,11)", planner.select(sizes(10 * mib, 40 * mib), null, 10, 12)
        .toString());
    assertEquals("SOLO(11)", planner.select(sizes(40 * mib, mib), null, 10, 12).toString());
    assertEquals("RANGE(11,12)", planner.select(sizes(17 * mib, 15 * mib), null, 10, 12)
        .toString());
    // T4: 2 x 17 MiB live range splits into two batches, never a failure.
    assertEquals("RANGE(11,11)", planner.select(sizes(17 * mib, 17 * mib), null, 10, 12)
        .toString());
    assertNull(planner.select(sizes(), null, 12, 12));
  }

  /** T6: a byte-small batch with too many plan entries is cut at a block boundary. */
  @Test
  public void planPrefixCutsAtTheEntryBudget() {
    ServingIndexService.Config config = ServingIndexService.Config.defaults()
        .setMaxPlanEntries(25);
    ServingBatchPlanner planner = new ServingBatchPlanner(config);
    List<CommittedBlock> blocks = new ArrayList<>();
    for (BlockReverseDiff diff : blocks(B0, 5, 10)) {
      blocks.add(new CommittedBlock(diff, hash(1)));
    }
    assertEquals(2, planner.planPrefix(blocks));
    config.setMaxPlanEntries(5);
    assertEquals(0, planner.planPrefix(blocks));
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

  private static IOException assertThrowsOrNull(ThrowingRunnable runnable) {
    try {
      runnable.run();
      return null;
    } catch (IOException failure) {
      return failure;
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws IOException;
  }

  private static Descriptor peekDescriptor(ServingIndexStore store, long through)
      throws IOException {
    try (StateArchiveIndexDatabase.Reader snapshot = store.snapshot()) {
      Descriptor current = ServingIndexStore.readDescriptor(snapshot);
      return new Descriptor(current.formatVersion(), current.scopeIdentity(),
          current.generationId(), current.indexedFrom(), through, current.headHash(),
          current.sourceDigest(), current.latestSourceIdentityDigest(),
          current.participants(), current.keyChanges());
    }
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
    sorted.sort(BlockReverseDiff::compareUnsigned);
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
      try (StateArchiveIndexDatabase.Reader snapshot = store.snapshot();
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
