package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.blocks;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.hash;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.plan;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Writer;
import org.tron.core.db2.archive.serving.ServingIndexServiceTest.History;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming.Stage;
import org.tron.core.db2.archive.serving.telemetry.ServingTimingTestAccess;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** One atomic commit per selected History read window, without write-size estimation. */
public class ServingIndexWindowCommitTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void wholeSelectionCommitsOnceIncludingDenseBlocks() throws Exception {
    run(new ServingIndexService.Config(), 3, Arrays.asList(5L));
  }

  @Test
  public void maxBlocksOneAccumulatesIntoOneFlushPerRound() throws Exception {
    run(new ServingIndexService.Config().setMaxBlocks(1), 3, Arrays.asList(5L));
  }

  @Test
  public void noChangeBlocksPublishProgressAtWindowEnd() throws Exception {
    run(new ServingIndexService.Config(), 0, Arrays.asList(5L));
  }

  @Test
  public void commitReadsEachDistinctKeyOnce() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      store.commit(plan(store, blocks(1, 511, 8)), false);
      ServingIndexIncrementalPlan.Builder builder =
          new ServingIndexIncrementalPlan.Builder(511, hash(511));
      try (ServingIndexTiming timing = new ServingIndexTiming("window-commit", 511, 514)) {
        for (BlockReverseDiff diff : blocks(512, 3, 8)) {
          builder.add(diff, hash(diff.getMeta().getBlockNumber()));
        }
        store.commit(builder.build(), false);
        assertEquals(8, ServingTimingTestAccess.calls(timing, Stage.META_GET));
        assertEquals(8, ServingTimingTestAccess.calls(timing, Stage.TAIL_GET));
        assertEquals(1, ServingTimingTestAccess.calls(timing, Stage.WRITE_SYNC));
      }
    }
    try (ServingIndexStore reopened = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      assertEquals(514, reopened.indexedThrough());
      assertEquals(514 * 8, reopened.keyChanges());
    }
  }

  private void run(ServingIndexService.Config config, int keys, List<Long> expected)
      throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    history.keysPerBlock = keys;
    List<Long> committed = new CopyOnWriteArrayList<>();
    config.setPollMillis(10).setCloseTimeoutMillis(10_000);
    try (Writer ignored = history.writer;
        ServingIndexService service = new ServingIndexService(history.servingRoot, root, null,
            Engine.ROCKSDB, history.source, config, new ServingIndexService.Hooks() {
              @Override
              public void afterCommit(long first, long last) {
                committed.add(last);
              }
            }, System::currentTimeMillis)) {
      history.publishWithoutNotify(1, 5);
      service.start();
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      assertEquals(expected, committed);
      assertEquals(0, service.batches(2));
      history.assertOracle(service);
    }
    try (ServingIndexStore reopened = ServingIndexStore.open(
        history.servingRoot.resolve("single-v1"), Engine.ROCKSDB, 1, hash(0), false, 1)) {
      assertEquals(5, reopened.indexedThrough());
    }
  }
}
