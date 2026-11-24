package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.blocks;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.diff;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.hash;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Writer;
import org.tron.core.db2.archive.serving.ServingIndexService.Hooks;
import org.tron.core.db2.archive.serving.ServingIndexServiceTest.History;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.Committed;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.CommittedBlock;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.KeyValue;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * Geth-style cross-window accumulation: one change table per round, one flush per threshold or
 * round target, and the flush intent protocol that keeps an interrupted multi-batch flush
 * recoverable and old-I queries correct while data batches are already durable.
 */
public class ServingIndexAccumulationTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /** INLINE, promotion, paged appends, an empty window, a page roll and one late fresh key. */
  private static List<List<BlockReverseDiff>> windows() {
    List<List<BlockReverseDiff>> windows = new ArrayList<>();
    windows.add(blocks(1, 4, 12));
    windows.add(blocks(5, 2, 12));
    windows.add(blocks(7, 3, 12));
    windows.add(blocks(10, 1, 0));
    windows.add(blocks(11, 5000, 1));
    List<BlockReverseDiff> late = new ArrayList<>(blocks(5011, 2, 4));
    List<byte[]> first = new ArrayList<>();
    for (int key = 0; key < 4; key++) {
      first.add(intKey(key));
    }
    first.add(intKey(200));
    late.set(0, diff(5011, first));
    windows.add(late);
    return windows;
  }

  @Test
  public void accumulatedFlushMatchesPerWindowCommitsRowByRow() throws Exception {
    Path perWindowDirectory = temporaryFolder.newFolder().toPath();
    Path atomicDirectory = temporaryFolder.newFolder().toPath();
    Path streamedDirectory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore perWindow = open(perWindowDirectory)) {
      for (List<BlockReverseDiff> window : windows()) {
        perWindow.commit(plan(perWindow, window), false);
      }
    }
    try (ServingIndexStore atomic = open(atomicDirectory)) {
      atomic.commit(accumulateWindows(atomic, windows()), false, Long.MAX_VALUE);
    }
    // A flush streamed through intent plus data batches must leave the identical database,
    // including the absence of any leftover flush intent key.
    try (ServingIndexStore streamed = open(streamedDirectory)) {
      streamed.commit(accumulateWindows(streamed, windows()), false, 1);
    }
    reopenAndCompare(perWindowDirectory, atomicDirectory);
    reopenAndCompare(perWindowDirectory, streamedDirectory);
  }

  @Test
  public void roundAccumulatesWindowsAndFlushesOnceAtTheTarget() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    history.keysPerBlock = 3;
    List<Long> committed = new CopyOnWriteArrayList<>();
    try (Writer ignored = history.writer;
        ServingIndexService service = service(history, root,
            new ServingIndexService.Config().setMaxBlocks(1), new Hooks() {
              @Override
              public void afterCommit(long first, long last) {
                committed.add(last);
              }
            })) {
      history.publishWithoutNotify(1, 5);
      service.start();
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      assertEquals(Collections.singletonList(5L), committed);
      assertEquals(0, service.batches(2));
      history.assertOracle(service);
    }
    try (ServingIndexStore reopened = ServingIndexStore.open(
        history.servingRoot.resolve("single-v1"), Engine.ROCKSDB, 1, hash(0), false, 1)) {
      assertEquals(5, reopened.indexedThrough());
      assertNull(reopened.pendingFlushIntent());
    }
  }

  @Test
  public void flushThresholdFlushesMidRoundOnBlockBoundaries() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    history.keysPerBlock = 3;
    List<Long> committed = new CopyOnWriteArrayList<>();
    try (Writer ignored = history.writer;
        ServingIndexService service = service(history, root,
            new ServingIndexService.Config().setMaxBlocks(1).setFlushChanges(4), new Hooks() {
              @Override
              public void afterCommit(long first, long last) {
                committed.add(last);
              }
            })) {
      history.publishWithoutNotify(1, 4);
      service.start();
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      // 3 changes per block, threshold 4: the first round flushes after the second block,
      // the second after the fourth; blocks never split and nothing is re-read.
      assertEquals(Arrays.asList(2L, 4L), committed);
      history.assertOracle(service);
    }
  }

  @Test
  public void interruptedMultiBatchFlushRecoversExactly() throws Exception {
    for (InjectionPoint point : InjectionPoint.values()) {
      interruptedFlushRecoversAt(point);
    }
  }

  private void interruptedFlushRecoversAt(InjectionPoint point) throws Exception {
    Path cleanDirectory = temporaryFolder.newFolder().toPath();
    Path crashedDirectory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore clean = open(cleanDirectory)) {
      clean.commit(plan(clean, blocks(1, 511, 8)), false);
      clean.commit(accumulate(clean, flushWindow()), false, Long.MAX_VALUE);
    }
    try (ServingIndexStore crashed = open(crashedDirectory)) {
      crashed.commit(plan(crashed, blocks(1, 511, 8)), false);
      ServingIndexStore.GroupedAppend append = accumulate(crashed, flushWindow());
      crashed.setFlushFaultHook(point.hook());
      try {
        crashed.commit(append, false, 1);
        fail("expected the injected " + point + " failure");
      } catch (IOException expected) {
        assertEquals("injected " + point, expected.getMessage());
      }
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(crashedDirectory,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      ServingIndexStore.FlushIntent intent = recovered.pendingFlushIntent();
      assertNotNull(intent);
      assertEquals(511, intent.baseThrough());
      assertEquals(514, intent.targetThrough());
      assertEquals(25, intent.keyChanges());
      recovered.recoverFlush(accumulate(recovered, flushWindow()), intent,
          Long.MAX_VALUE);
      assertNull(recovered.pendingFlushIntent());
      assertEquals(514, recovered.indexedThrough());
    }
    reopenAndCompare(cleanDirectory, crashedDirectory);
  }

  @Test
  public void recoveryItselfMayBeInterruptedAndRepeatsIdempotently() throws Exception {
    Path cleanDirectory = temporaryFolder.newFolder().toPath();
    Path crashedDirectory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore clean = open(cleanDirectory)) {
      clean.commit(plan(clean, blocks(1, 511, 8)), false);
      clean.commit(accumulate(clean, flushWindow()), false, Long.MAX_VALUE);
    }
    try (ServingIndexStore crashed = open(crashedDirectory)) {
      crashed.commit(plan(crashed, blocks(1, 511, 8)), false);
      ServingIndexStore.GroupedAppend append = accumulate(crashed, flushWindow());
      crashed.setFlushFaultHook(InjectionPoint.AFTER_FIRST_DATA.hook());
      try {
        crashed.commit(append, false, 1);
        fail("expected the injected crash");
      } catch (IOException expected) {
        assertEquals("injected AFTER_FIRST_DATA", expected.getMessage());
      }
    }
    // The first recovery also crashes after its first data batch, leaving later keys unwritten.
    try (ServingIndexStore recovering = ServingIndexStore.open(crashedDirectory,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      ServingIndexStore.FlushIntent intent = recovering.pendingFlushIntent();
      assertNotNull(intent);
      recovering.setFlushFaultHook(new ServingIndexStore.FlushFaultHook() {
        @Override
        public void afterDataBatch(int index) throws IOException {
          if (index == 1) {
            throw new IOException("injected recovery AFTER_SECOND_DATA");
          }
        }
      });
      try {
        recovering.recoverFlush(accumulate(recovering, flushWindow()), intent, 1);
        fail("expected the injected recovery crash");
      } catch (IOException expected) {
        assertEquals("injected recovery AFTER_SECOND_DATA", expected.getMessage());
      }
    }
    // The second recovery verifies both the original and the recovery writes and finishes.
    try (ServingIndexStore recovered = ServingIndexStore.open(crashedDirectory,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      ServingIndexStore.FlushIntent intent = recovered.pendingFlushIntent();
      assertNotNull(intent);
      recovered.recoverFlush(accumulate(recovered, flushWindow()), intent,
          Long.MAX_VALUE);
      assertNull(recovered.pendingFlushIntent());
      assertEquals(514, recovered.indexedThrough());
    }
    reopenAndCompare(cleanDirectory, crashedDirectory);
  }

  @Test
  public void queriesDuringStreamedDataBatchesStayBoundedByDurableI() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = open(directory)) {
      store.commit(plan(store, blocks(1, 20, 4)), false);
      assertEquals(OptionalLong.of(1L), firstChangeAfter(store, directory, intKey(0), 0, 20));
      store.setFlushFaultHook(new ServingIndexStore.FlushFaultHook() {
        @Override
        public void beforeFinalPublish() throws IOException {
          // Every data batch is durable and pages already hold epochs above I; the old
          // descriptor still bounds queries: answers come from it, and anything above I
          // is rejected by the coverage guard before a page is even read.
          try (ArchiveIndexStorage.Reader snapshot = store.snapshot()) {
            assertEquals(20, ServingIndexStore.readDescriptor(snapshot).indexedThrough());
            assertEquals(OptionalLong.of(1L),
                view(directory, snapshot).firstChangeAfter("code", intKey(0), 0, 20));
            assertThrows(IllegalArgumentException.class,
                () -> view(directory, snapshot).firstChangeAfter("code", intKey(0), 21, 20));
          }
          throw new IOException("injected BEFORE_FINAL");
        }
      });
      try {
        store.commit(accumulate(store, blocks(21, 3, 4)), false, 1);
        fail("expected the injected crash");
      } catch (IOException expected) {
        assertEquals("injected BEFORE_FINAL", expected.getMessage());
      }
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1);
        ArchiveIndexStorage.Reader before = recovered.snapshot()) {
      ServingIndexStore.FlushIntent intent = recovered.pendingFlushIntent();
      assertNotNull(intent);
      assertEquals(20, ServingIndexStore.readDescriptor(before).indexedThrough());
      assertEquals(OptionalLong.of(1L),
          view(directory, before).firstChangeAfter("code", intKey(0), 0, 20));
      recovered.recoverFlush(accumulate(recovered, blocks(21, 3, 4)), intent,
          Long.MAX_VALUE);
      assertEquals(23, recovered.indexedThrough());
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1);
        ArchiveIndexStorage.Reader after = recovered.snapshot()) {
      assertEquals(OptionalLong.of(21L),
          view(directory, after).firstChangeAfter("code", intKey(0), 20, 23));
      assertEquals(OptionalLong.of(1L),
          view(directory, after).firstChangeAfter("code", intKey(0), 0, 20));
    }
  }

  @Test
  public void serviceRecoversAPendingFlushOnOpen() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    history.keysPerBlock = 8;
    try (Writer ignored = history.writer) {
      history.publishWithoutNotify(1, 8);
      try (ServingIndexService first = service(history, root,
          new ServingIndexService.Config(), Hooks.NONE)) {
        first.start();
        assertTrue(first.awaitLive(Duration.ofSeconds(30)));
        assertEquals(8, first.status().getIndexedThrough());
      }
      // Simulate an interrupted flush of the next blocks exactly as the build thread would
      // have accumulated them, then crash it after its first data batch.
      history.publishWithoutNotify(9, 10);
      Path serving = history.servingRoot.resolve("single-v1");
      try (ServingIndexStore store = ServingIndexStore.open(serving, Engine.ROCKSDB,
          1, hash(0), false, 1)) {
        assertEquals(8, store.indexedThrough());
        ServingChangeAccumulator accumulator = accumulator(store);
        Committed view = history.source.committed();
        for (CommittedBlock block : history.source.read(view, 9, 10)) {
          accumulator.addBlock(block.getDiff(), block.getStepDigest());
        }
        store.setFlushFaultHook(InjectionPoint.AFTER_FIRST_DATA.hook());
        try {
          store.commit(accumulator.seal(), false, 1);
          fail("expected the injected crash");
        } catch (IOException expected) {
          assertEquals("injected AFTER_FIRST_DATA", expected.getMessage());
        }
      }
      try (ServingIndexService recovered = service(history, root,
          new ServingIndexService.Config(), Hooks.NONE)) {
        recovered.start();
        assertTrue(recovered.awaitLive(Duration.ofSeconds(30)));
        assertEquals(10, recovered.status().getIndexedThrough());
        history.assertOracle(recovered);
      }
      try (ServingIndexStore store = ServingIndexStore.open(
          history.servingRoot.resolve("single-v1"), Engine.ROCKSDB, 1, hash(0), false, 1)) {
        assertNull(store.pendingFlushIntent());
      }
    }
  }

  @Test
  public void recoveryVerifiesInlineSuffixAndPageSpanningResidue() throws Exception {
    // INLINE residue with a real predecessor: two keys stay INLINE through the flush.
    Path inlineClean = temporaryFolder.newFolder().toPath();
    Path inlineCrashed = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore clean = open(inlineClean)) {
      clean.commit(plan(clean, blocks(1, 3, 2)), false);
      clean.commit(accumulate(clean, blocks(4, 1, 2)), false, Long.MAX_VALUE);
    }
    try (ServingIndexStore crashed = open(inlineCrashed)) {
      crashed.commit(plan(crashed, blocks(1, 3, 2)), false);
      ServingIndexStore.GroupedAppend append =
          accumulate(crashed, blocks(4, 1, 2));
      crashed.setFlushFaultHook(InjectionPoint.BEFORE_FINAL.hook());
      try {
        crashed.commit(append, false, 1);
        fail("expected the injected crash");
      } catch (IOException expected) {
        assertEquals("injected BEFORE_FINAL", expected.getMessage());
      }
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(inlineCrashed,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      recovered.recoverFlush(accumulate(recovered, blocks(4, 1, 2)),
          recovered.pendingFlushIntent(), Long.MAX_VALUE);
      assertEquals(4, recovered.indexedThrough());
    }
    reopenAndCompare(inlineClean, inlineCrashed);

    // Paged residue whose replayed epochs roll into a further page across the boundary.
    Path rollClean = temporaryFolder.newFolder().toPath();
    Path rollCrashed = temporaryFolder.newFolder().toPath();
    List<BlockReverseDiff> rollWindow = new ArrayList<>(blocks(4097, 4100, 1));
    rollWindow.set(0, diff(4097, Arrays.asList(intKey(0), intKey(200))));
    try (ServingIndexStore clean = open(rollClean)) {
      clean.commit(plan(clean, blocks(1, 4096, 1)), false);
      clean.commit(accumulate(clean, rollWindow), false, Long.MAX_VALUE);
    }
    try (ServingIndexStore crashed = open(rollCrashed)) {
      crashed.commit(plan(crashed, blocks(1, 4096, 1)), false);
      ServingIndexStore.GroupedAppend append = accumulate(crashed, rollWindow);
      crashed.setFlushFaultHook(InjectionPoint.BEFORE_FINAL.hook());
      try {
        crashed.commit(append, false, 1);
        fail("expected the injected crash");
      } catch (IOException expected) {
        assertEquals("injected BEFORE_FINAL", expected.getMessage());
      }
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(rollCrashed,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      recovered.recoverFlush(accumulate(recovered, rollWindow),
          recovered.pendingFlushIntent(), Long.MAX_VALUE);
      assertEquals(8196, recovered.indexedThrough());
    }
    reopenAndCompare(rollClean, rollCrashed);
  }

  @Test
  public void legacySoloResidueRecoveryUsesIdemRuleInsteadOfCorruption() throws Exception {
    Path cleanDirectory = temporaryFolder.newFolder().toPath();
    Path crashedDirectory = temporaryFolder.newFolder().toPath();
    // The clean path lets the normal R-IDEM rule absorb the residue while committing.
    seedLegacySoloResidue(cleanDirectory);
    try (ServingIndexStore clean = open(cleanDirectory)) {
      clean.commit(plan(clean, blocks(5, 2, 2)), false);
    }
    // The crashed path interrupts the first streamed flush right after its intent; recovery
    // must still absorb the residue instead of misreading it as flush corruption.
    seedLegacySoloResidue(crashedDirectory);
    try (ServingIndexStore crashed = open(crashedDirectory)) {
      ServingIndexStore.GroupedAppend append =
          accumulate(crashed, blocks(5, 2, 2));
      crashed.setFlushFaultHook(InjectionPoint.AFTER_INTENT.hook());
      try {
        crashed.commit(append, false, 1);
        fail("expected the injected crash");
      } catch (IOException expected) {
        assertEquals("injected AFTER_INTENT", expected.getMessage());
      }
    }
    try (ServingIndexStore recovered = ServingIndexStore.open(crashedDirectory,
        Engine.ROCKSDB, 1, hash(0), false, 1)) {
      assertNotNull(recovered.pendingFlushIntent());
      recovered.recoverFlush(accumulate(recovered, blocks(5, 2, 2)),
          recovered.pendingFlushIntent(), Long.MAX_VALUE);
      assertEquals(6, recovered.indexedThrough());
    }
    reopenAndCompare(cleanDirectory, crashedDirectory);
  }

  /** A key-0 KeyMeta residue at block 5 with durable progress still at 4 (solo crash). */
  private static void seedLegacySoloResidue(Path directory) throws IOException {
    try (ServingIndexStore store = open(directory)) {
      store.commit(plan(store, blocks(1, 4, 1)), false);
    }
    try (ArchiveIndexStorage.Writer writer = ArchiveIndexStorage.openWriter(
        directory.resolve(ServingIndexStore.DATABASE), Engine.ROCKSDB)) {
      List<org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation> mutations =
          new ArrayList<>();
      PersistentServingKeyIndexGeneration.appendExactChanges(writer, mutations,
          new org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey(
              "code", intKey(0)),
          Collections.singletonList(5L), 4);
      writer.write(mutations, true);
    }
  }

  @Test
  public void parallelStreamedFlushMatchesAndSinkFailureStopsWorkers() throws Exception {
    Path cleanDirectory = temporaryFolder.newFolder().toPath();
    Path parallelDirectory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore clean = open(cleanDirectory)) {
      clean.commit(plan(clean, blocks(1, 511, 8)), false);
      clean.commit(accumulate(clean, flushWindow()), false, Long.MAX_VALUE);
    }
    System.setProperty(ServingIndexParallelAppender.THREADS_PROPERTY, "4");
    try {
      // A streamed flush assembled by parallel workers matches the serial atomic result.
      try (ServingIndexStore parallel = open(parallelDirectory)) {
        parallel.commit(plan(parallel, blocks(1, 511, 8)), false);
        parallel.commit(accumulate(parallel, flushWindow()), false, 1);
      }
      reopenAndCompare(cleanDirectory, parallelDirectory);

      // A sink-side failure under parallelism must not leave spinning workers behind.
      try (ServingIndexStore crashed = open(parallelDirectory)) {
        crashed.commit(accumulate(crashed, blocks(515, 64, 8)), false,
            Long.MAX_VALUE);
        ServingIndexStore.GroupedAppend append =
            accumulate(crashed, blocks(579, 8, 16));
        crashed.setFlushFaultHook(InjectionPoint.AFTER_INTENT.hook());
        long started = System.nanoTime();
        try {
          crashed.commit(append, false, 1);
          fail("expected the injected failure");
        } catch (IOException expected) {
          assertEquals("injected AFTER_INTENT", expected.getMessage());
        }
        assertTrue("sink failure did not stop workers promptly",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
      }
    } finally {
      System.clearProperty(ServingIndexParallelAppender.THREADS_PROPERTY);
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (Thread.getAllStackTraces().keySet().stream()
        .anyMatch(thread -> thread.getName().startsWith("serving-index-build-"))) {
      assertTrue("worker threads did not stop", System.nanoTime() < deadline);
      Thread.sleep(20);
    }
  }

  @Test
  public void legacyIndexReopensAndContinuesThroughStreamedParallelFlush() throws Exception {
    // A PR #55 index: one atomic commit per window, fixed 512-entry pages. Key 0/1 end with two
    // full pages plus a 3-entry tail page; key 200 with exactly two full pages; keys 300-302
    // INLINE with 1/2/3 epochs; keys 2-7 with a 514-entry partial page.
    List<List<BlockReverseDiff>> legacyWindows = new ArrayList<>();
    legacyWindows.add(blocks(1, 511, 8));
    legacyWindows.add(blocks(512, 3, 8));
    legacyWindows.add(blocks(515, 513, 2));
    List<BlockReverseDiff> fullPages = new ArrayList<>();
    for (long block = 1028; block < 2052; block++) {
      fullPages.add(diff(block, Collections.singletonList(intKey(200))));
    }
    legacyWindows.add(fullPages);
    legacyWindows.add(blocks(2052, 1, 0));
    legacyWindows.add(Arrays.asList(diff(2053, Collections.singletonList(intKey(300))),
        diff(2054, Collections.singletonList(intKey(300))),
        diff(2055, Collections.singletonList(intKey(301))),
        diff(2056, Collections.singletonList(intKey(302))),
        diff(2057, Collections.singletonList(intKey(302))),
        diff(2058, Collections.singletonList(intKey(302)))));

    // The continuation exercises every old-format transition on the new path: append to a
    // partial tail page, open a new page after a full one, stay INLINE, and promote INLINE.
    List<BlockReverseDiff> continuation = new ArrayList<>(blocks(2059, 5, 4));
    continuation.add(diff(2064, Collections.singletonList(intKey(200))));
    continuation.add(diff(2065, Collections.singletonList(intKey(300))));
    continuation.add(diff(2066, Collections.singletonList(intKey(302))));
    continuation.add(diff(2067, Collections.singletonList(intKey(302))));

    Path legacyDirectory = temporaryFolder.newFolder().toPath();
    Path cleanDirectory = temporaryFolder.newFolder().toPath();
    List<String> legacyRows;
    long legacyChanges;
    try (ServingIndexStore legacy = open(legacyDirectory)) {
      for (List<BlockReverseDiff> window : legacyWindows) {
        legacy.commit(plan(legacy, window), false);
      }
      legacyChanges = legacy.keyChanges();
      try (ArchiveIndexStorage.Reader snapshot = legacy.snapshot()) {
        legacyRows = dataDump(snapshot);
      }
    }

    System.setProperty(ServingIndexParallelAppender.THREADS_PROPERTY, "4");
    try {
      try (ServingIndexStore continued = open(legacyDirectory)) {
        // Reopening an existing index neither rewrites nor re-indexes anything.
        assertEquals(2058, continued.indexedThrough());
        assertEquals(legacyChanges, continued.keyChanges());
        try (ArchiveIndexStorage.Reader snapshot = continued.snapshot()) {
          assertEquals(legacyRows, dataDump(snapshot));
          assertEquals(OptionalLong.of(1L),
              view(legacyDirectory, snapshot).firstChangeAfter("code", intKey(0), 0, 2058));
          assertEquals(OptionalLong.of(1028L),
              view(legacyDirectory, snapshot).firstChangeAfter("code", intKey(200), 1027, 2058));
        }
        continued.commit(accumulate(continued, continuation), false, 1);
        assertEquals(2067, continued.indexedThrough());
        assertNull(continued.pendingFlushIntent());
      }
      try (ServingIndexStore clean = open(cleanDirectory)) {
        List<List<BlockReverseDiff>> all = new ArrayList<>(legacyWindows);
        all.add(continuation);
        for (List<BlockReverseDiff> window : all) {
          clean.commit(accumulate(clean, window), false, Long.MAX_VALUE);
        }
      }
      reopenAndCompare(cleanDirectory, legacyDirectory);
      try (ServingIndexStore continued = open(legacyDirectory);
          ArchiveIndexStorage.Reader snapshot = continued.snapshot()) {
        PersistentServingKeyIndexGeneration index = view(legacyDirectory, snapshot);
        assertEquals(OptionalLong.of(2064L),
            index.firstChangeAfter("code", intKey(200), 2058, 2067));
        assertEquals(OptionalLong.of(2066L),
            index.firstChangeAfter("code", intKey(302), 2058, 2067));
        assertEquals(OptionalLong.of(2059L),
            index.firstChangeAfter("code", intKey(3), 2058, 2067));
        assertEquals(OptionalLong.empty(),
            index.firstChangeAfter("code", intKey(7), 2058, 2067));
      }
    } finally {
      System.clearProperty(ServingIndexParallelAppender.THREADS_PROPERTY);
    }
  }

  /** The flush window: eight hot keys plus one late fresh INLINE key on the first block. */
  private static List<BlockReverseDiff> flushWindow() {
    List<BlockReverseDiff> flush = new ArrayList<>(blocks(512, 3, 8));
    List<byte[]> first = new ArrayList<>();
    for (int key = 0; key < 8; key++) {
      first.add(intKey(key));
    }
    first.add(intKey(200));
    flush.set(0, diff(512, first));
    return flush;
  }

  private static ServingIndexStore.GroupedAppend accumulateWindows(ServingIndexStore store,
      List<List<BlockReverseDiff>> windows) throws IOException {
    ServingChangeAccumulator accumulator = accumulator(store);
    for (List<BlockReverseDiff> window : windows) {
      addWindow(accumulator, store, window);
    }
    return accumulator.seal();
  }

  private static ServingIndexStore.GroupedAppend accumulate(ServingIndexStore store,
      List<BlockReverseDiff> window) throws IOException {
    ServingChangeAccumulator accumulator = accumulator(store);
    addWindow(accumulator, store, window);
    return accumulator.seal();
  }

  private static void addWindow(ServingChangeAccumulator accumulator, ServingIndexStore store,
      List<BlockReverseDiff> window) {
    for (BlockReverseDiff diff : window) {
      accumulator.addBlock(diff, stepDigest(diff));
    }
  }

  /** The plan path's per-diff step digest: SHA-256 of the complete diff encoding. */
  private static byte[] stepDigest(BlockReverseDiff diff) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256")
          .digest(new org.tron.core.db2.archive.history.codec.BlockHistoryCodec().encode(diff));
    } catch (java.security.NoSuchAlgorithmException unavailable) {
      throw new IllegalStateException("SHA-256 is unavailable", unavailable);
    }
  }

  private static ServingChangeAccumulator accumulator(ServingIndexStore store) {
    return new ServingChangeAccumulator(store.indexedThrough(), store.headHash(),
        store.sourceDigest(), store.latestIdentityDigest(), store.participantsList());
  }

  private static ServingIndexService service(History history, Path root,
      ServingIndexService.Config config, Hooks hooks) {
    config.setPollMillis(10).setCloseTimeoutMillis(10_000);
    return new ServingIndexService(history.servingRoot, root, null, Engine.ROCKSDB,
        history.source, config, hooks, System::currentTimeMillis);
  }

  private static ServingIndexStore open(Path directory) throws IOException {
    return ServingIndexStore.open(directory, Engine.ROCKSDB, 1, hash(0), false, 1);
  }

  private static byte[] intKey(int key) {
    return ByteBuffer.allocate(4).putInt(key).array();
  }

  private static PersistentServingKeyIndexGeneration view(Path directory,
      ArchiveIndexStorage.Reader snapshot) throws IOException {
    return PersistentServingKeyIndexGeneration.overSnapshot(directory,
        ServingIndexStore.readDescriptor(snapshot), Engine.ROCKSDB, snapshot);
  }

  private static OptionalLong firstChangeAfter(ServingIndexStore store, Path directory,
      byte[] rawKey, long target, long upperBound) throws IOException {
    try (ArchiveIndexStorage.Reader snapshot = store.snapshot()) {
      return view(directory, snapshot).firstChangeAfter("code", rawKey, target, upperBound);
    }
  }

  private static void reopenAndCompare(Path expected, Path actual) throws IOException {
    try (ServingIndexStore left = open(expected);
        ServingIndexStore right = open(actual);
        ArchiveIndexStorage.Reader leftSnapshot = left.snapshot();
        ArchiveIndexStorage.Reader rightSnapshot = right.snapshot()) {
      // Data rows (meta, pages) are batch-independent; coverage and descriptor legitimately
      // embed the per-commit generation identity, so digests are compared directly instead.
      assertEquals(dataDump(leftSnapshot), dataDump(rightSnapshot));
      assertArrayEquals(ServingIndexStore.readDescriptor(leftSnapshot).sourceDigest(),
          ServingIndexStore.readDescriptor(rightSnapshot).sourceDigest());
      assertArrayEquals(ServingIndexStore.readDescriptor(leftSnapshot)
          .latestSourceIdentityDigest(),
          ServingIndexStore.readDescriptor(rightSnapshot).latestSourceIdentityDigest());
      assertEquals(left.indexedThrough(), right.indexedThrough());
      assertEquals(left.keyChanges(), right.keyChanges());
      assertNull(right.pendingFlushIntent());
    }
  }

  private static List<String> dataDump(ArchiveIndexStorage.Reader snapshot) throws IOException {
    List<String> rows = new ArrayList<>();
    try (ArchiveIndexStorage.Cursor cursor = snapshot.cursor()) {
      cursor.seek(new byte[0]);
      KeyValue row;
      while ((row = cursor.next()) != null) {
        String hex = Hex.toHexString(row.getKey());
        if (hex.startsWith("03") || hex.startsWith("04")) {
          rows.add(hex + "=" + Hex.toHexString(row.getValue()));
        }
      }
    }
    return rows;
  }

  private enum InjectionPoint {
    AFTER_INTENT, AFTER_FIRST_DATA, BEFORE_FINAL;

    ServingIndexStore.FlushFaultHook hook() {
      InjectionPoint point = this;
      return new ServingIndexStore.FlushFaultHook() {
        @Override
        public void afterIntent() throws IOException {
          if (point == AFTER_INTENT) {
            throw new IOException("injected AFTER_INTENT");
          }
        }

        @Override
        public void afterDataBatch(int index) throws IOException {
          if (point == AFTER_FIRST_DATA && index == 0) {
            throw new IOException("injected AFTER_FIRST_DATA");
          }
        }

        @Override
        public void beforeFinalPublish() throws IOException {
          if (point == BEFORE_FINAL) {
            throw new IOException("injected BEFORE_FINAL");
          }
        }
      };
    }
  }
}
