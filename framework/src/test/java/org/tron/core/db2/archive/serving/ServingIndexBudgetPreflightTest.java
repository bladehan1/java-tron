package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.blocks;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.hash;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.plan;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.PreparedExactChanges;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.CommittedBlock;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming.Stage;
import org.tron.core.db2.archive.serving.telemetry.ServingTimingTestAccess;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

public class ServingIndexBudgetPreflightTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final ServingIndexStore.WriteBudget LARGE =
      new ServingIndexStore.WriteBudget(500_000, 64L * 1024 * 1024);
  private static final ServingIndexStore.WriteBudget TINY =
      new ServingIndexStore.WriteBudget(64, 64 * 1024);

  @Test
  public void selectsMaximalWholeBlockPrefixBeforeEncodingAnyPages() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      assertTrue(store.commit(plan(store, blocks(1, 300, 16)), LARGE, false));
      List<BlockReverseDiff> diffs = blocks(301, 300, 16);
      List<CommittedBlock> source = new ArrayList<>();
      for (BlockReverseDiff diff : diffs) {
        source.add(new CommittedBlock(diff, hash(diff.getMeta().getBlockNumber())));
      }
      ServingIndexStore.Preparation preparation = store.preparation();
      int k;
      try (ServingIndexTiming timing = new ServingIndexTiming("prefix", 300, 300)) {
        k = store.fittingPrefix(source, new ServingIndexService.Config(), TINY, 600, preparation);
        assertEquals(16, ServingTimingTestAccess.calls(timing, Stage.META_GET));
        assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
        assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.BUILD));
        assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.WRITE_SYNC));
      }
      assertTrue(k > 0 && k < source.size());
      assertEquals(300, store.indexedThrough());
      assertFalse(store.commit(plan(store, diffs.subList(0, k + 1)), TINY, false, preparation));
      assertTrue(store.commit(plan(store, diffs.subList(0, k)), TINY, false, preparation));
      assertEquals(300 + k, store.indexedThrough());
      // The same decoded suffix continues against the newly published metadata.
      int remaining = store.fittingPrefix(source.subList(k, source.size()),
          new ServingIndexService.Config().setMaxPlanEntries(16), LARGE, 600,
          store.preparation());
      assertEquals(1, remaining);
    }
  }

  @Test
  public void predictedSizesMatchCodecsAcrossInlineAndPageBoundaries() throws Exception {
    try (ArchiveIndexStorage.Writer writer = ArchiveIndexStorage.openWriter(
        temporaryFolder.newFolder().toPath(), Engine.ROCKSDB)) {
      int id = 0;
      for (int prior : new int[]{0, 1, 4, 5, 511, 512, 513}) {
        for (int added : new int[]{1, 4, 5, 512, 513}) {
          ExactKey key = new ExactKey("code", ServingIndexStoreTest.key(id++));
          if (prior > 0) {
            List<Mutation> seed = new ArrayList<>();
            PersistentServingKeyIndexGeneration.appendExactChanges(writer, seed, key,
                epochs(1, prior), Long.MAX_VALUE);
            writer.write(seed, true);
          }
          PreparedExactChanges prepared = PersistentServingKeyIndexGeneration.prepareExactChanges(
              writer, key, epochs(prior + 1, added), prior, new HashMap<>());
          List<Mutation> actual = new ArrayList<>();
          prepared.appendTo(writer, actual);
          assertEquals("prior=" + prior + " added=" + added,
              actual.size(), prepared.mutationCount());
          assertEquals(actual.stream().mapToLong(Mutation::estimatedBytes).sum(),
              prepared.estimatedBytes());
        }
      }
    }
  }

  private static List<Long> epochs(int first, int count) {
    List<Long> epochs = new ArrayList<>();
    for (long epoch = first; epoch < first + count; epoch++) {
      epochs.add(epoch);
    }
    return epochs;
  }

  @Test
  public void rejectedPrefixesNeverBuildPagesAndReuseMetadataUntilPublication() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      assertTrue(store.commit(plan(store, blocks(1, 300, 16)), LARGE, false));
      List<BlockReverseDiff> source = blocks(301, 300, 16);
      ServingIndexStore.Preparation preparation = store.preparation();
      long reads = 0;
      int rejected = 0;
      int k = source.size();
      while (true) {
        ServingIndexIncrementalPlan prefix = plan(store, source.subList(0, k));
        try (ServingIndexTiming timing = new ServingIndexTiming("preflight", 300, 300 + k)) {
          boolean committed = store.commit(prefix, TINY, false, preparation);
          reads += ServingTimingTestAccess.calls(timing, Stage.META_GET);
          if (committed) {
            assertEquals(16, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
            assertEquals(1, ServingTimingTestAccess.calls(timing, Stage.WRITE_SYNC));
            break;
          }
          rejected++;
          assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.BUILD));
          assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
          assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.WRITE_SYNC));
          assertEquals(300, store.indexedThrough());
          k /= 2;
          assertTrue(k > 0);
        }
      }
      assertTrue(rejected > 0);
      assertEquals("each source key metadata is read at most once", 16, reads);
      assertEquals(300 + k, store.indexedThrough());
      ServingIndexIncrementalPlan next = plan(store, blocks(301 + k, 1, 16));
      assertThrows(IllegalStateException.class,
          () -> store.commit(next, LARGE, false, preparation));
      assertTrue(store.commit(next, LARGE, false));
    }
  }

  @Test
  public void rejectedSingleBlockReusesMetadataForSoloAndReopens() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      assertTrue(store.commit(plan(store, blocks(1, 5, 80)), LARGE, false));
      ServingIndexIncrementalPlan next = plan(store, blocks(6, 1, 80));
      ServingIndexStore.Preparation preparation = store.preparation();
      try (ServingIndexTiming timing = new ServingIndexTiming("solo-preflight", 5, 6)) {
        assertFalse(store.commit(next, TINY, false, preparation));
        assertEquals(0, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
        assertTrue(store.commitSolo(next, TINY, false, ServingIndexStore.ChunkHook.NONE,
            preparation) > 1);
        assertEquals(80, ServingTimingTestAccess.calls(timing, Stage.META_GET));
        assertEquals(80, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
      }
    }
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      assertEquals(6, store.indexedThrough());
      assertEquals(480, store.keyChanges());
    }
  }
}
