package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.tron.core.db2.NativeCheckpointTestSupport.database;
import static org.tron.core.db2.NativeCheckpointTestSupport.engine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.config.args.Args;
import org.tron.core.db2.common.WrappedByteArray;

/**
 * Semantics and local cost evidence for the per-batch consolidation barrier
 * ({@link SnapshotRoot#syncCheckpointBarrier()}): unsynced checkpoint writes become durable
 * points on every store before the coordinator prunes, without rewriting data. Timing numbers
 * are local mechanism evidence only, not node or production predictions.
 */
public class PerBatchSyncBarrierSemanticsTest {

  private static final int COST_STORES = 16;
  private static final int COST_WARMUP_ROUNDS = 3;
  private static final int COST_ROUNDS = 21;

  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Args.setParam(new String[]{"--output-directory", "output_sync_barrier_test"},
        "config-test.conf");
  }

  @Test
  public void unsyncedWritesSurviveReopenAfterBarrier() throws Exception {
    List<Path> directories = createDirectories("sem", 4);
    List<Chainbase> stores = openOn(directories);
    try {
      for (int index = 0; index < stores.size(); index++) {
        SnapshotRoot root = (SnapshotRoot) stores.get(index).getHead().getRoot();
        root.applyCheckpointMutations(batch((byte) (index + 1)), false);
        root.syncCheckpointBarrier();
      }
      for (int index = 0; index < stores.size(); index++) {
        assertArrayEquals(new byte[]{(byte) (index + 11)},
            stores.get(index).getUnchecked(new byte[]{(byte) (index + 1)}));
      }
      // A barrier over stores with no new writes is an accepted no-op (idempotent seal).
      for (Chainbase store : stores) {
        ((SnapshotRoot) store.getHead().getRoot()).syncCheckpointBarrier();
      }
    } finally {
      closeStores(stores);
    }
    List<Chainbase> reopened = openOn(directories);
    try {
      for (int index = 0; index < reopened.size(); index++) {
        assertArrayEquals(new byte[]{(byte) (index + 11)},
            reopened.get(index).getUnchecked(new byte[]{(byte) (index + 1)}));
      }
    } finally {
      closeStores(reopened);
    }
  }

  @Test
  public void logsParallelBarrierCostAgainstPerStoreSyncedWrites() throws Exception {
    List<Chainbase> stores = openOn(createDirectories("cost", COST_STORES));
    List<Long> barrierRounds = new ArrayList<>();
    List<Long> syncedRounds = new ArrayList<>();
    try {
      for (int round = 0; round < COST_WARMUP_ROUNDS + COST_ROUNDS; round++) {
        boolean measured = round >= COST_WARMUP_ROUNDS;
        long start = System.nanoTime();
        for (int index = 0; index < stores.size(); index++) {
          ((SnapshotRoot) stores.get(index).getHead().getRoot())
              .applyCheckpointMutations(batch((byte) (round + 1)), false);
        }
        parallelBarriers(stores);
        if (measured) {
          barrierRounds.add((System.nanoTime() - start) / 1_000_000L);
        }
        long syncedStart = System.nanoTime();
        for (int index = 0; index < stores.size(); index++) {
          ((SnapshotRoot) stores.get(index).getHead().getRoot())
              .applyCheckpointMutations(batch((byte) (round + 2)), true);
        }
        if (measured) {
          syncedRounds.add((System.nanoTime() - syncedStart) / 1_000_000L);
        }
      }
    } finally {
      closeStores(stores);
    }
    System.err.println("BARRIER-COST backend=" + engine()
        + " stores=" + COST_STORES
        + " barrierMs p50=" + percentile(barrierRounds, 0.5)
        + " mean=" + mean(barrierRounds)
        + " | perStoreSyncedWriteMs p50=" + percentile(syncedRounds, 0.5)
        + " mean=" + mean(syncedRounds));
  }

  private void parallelBarriers(List<Chainbase> stores) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (Chainbase store : stores) {
        SnapshotRoot root = (SnapshotRoot) store.getHead().getRoot();
        futures.add(executor.submit(root::syncCheckpointBarrier));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdown();
    }
  }

  private List<Path> createDirectories(String tag, int count) throws Exception {
    List<Path> directories = new ArrayList<>();
    for (int index = 0; index < count; index++) {
      directories.add(temporary.newFolder("barrier-store-" + tag + "-" + index).toPath());
    }
    return directories;
  }

  private List<Chainbase> openOn(List<Path> directories) {
    List<Chainbase> stores = new ArrayList<>();
    for (Path directory : directories) {
      stores.add(new Chainbase(new SnapshotRoot(database(directory, "store"))));
    }
    return stores;
  }

  private static void closeStores(List<Chainbase> stores) throws Exception {
    for (Chainbase store : stores) {
      store.close();
    }
  }

  private static Map<WrappedByteArray, WrappedByteArray> batch(byte value) {
    Map<WrappedByteArray, WrappedByteArray> batch = new LinkedHashMap<>();
    batch.put(WrappedByteArray.of(new byte[]{value}),
        WrappedByteArray.of(new byte[]{(byte) (value + 10)}));
    return batch;
  }

  private static long percentile(List<Long> samples, double quantile) {
    List<Long> sorted = new ArrayList<>(samples);
    sorted.sort(Long::compareTo);
    return sorted.get((int) Math.ceil(quantile * sorted.size()) - 1);
  }

  private static long mean(List<Long> samples) {
    long total = 0;
    for (long sample : samples) {
      total += sample;
    }
    return total / samples.size();
  }
}
