package org.tron.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.config.args.StorageConfig.PathStateDbSettingsConfig;
import org.tron.core.db2.stateroot.storage.PathStateNativeBudget;

/** WP8-7 (design v2 ch08 §3.8, E9): the budget equation and its inputs. */
public class NativeMemoryBudgetTest {

  private static final long GIB = NativeMemoryBudget.GIB;
  private static final long MIB = NativeMemoryBudget.MIB;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void commonRedoIsCountedAlongsidePathStateAndServing() throws Exception {
    org.tron.core.config.args.Storage storage = new org.tron.core.config.args.Storage();
    storage.setDbEngine("ROCKSDB");
    storage.setDbDirectory("database");
    storage.setCommonCheckpointEnabled(true);
    storage.setPathStateRootDbSettings(new PathStateDbSettingsConfig());
    NativeMemoryBudget.Result result = NativeMemoryBudget.checkAtStartup(storage,
        temporaryFolder.newFolder().toPath());
    assertEquals(32L * MIB, (long) result.items.get("common-redo.cache"));
    assertEquals(128L * MIB, (long) result.items.get("common-redo.memtable"));
    assertTrue(result.items.containsKey("pathState.cache"));
    assertTrue(result.items.containsKey("serving.cache"));
  }

  @Test
  public void legacyHotCountsSharedCachesAndPerColumnMemtables() throws Exception {
    org.tron.core.config.args.Storage storage = new org.tron.core.config.args.Storage();
    storage.setDbEngine("ROCKSDB");
    storage.setDbDirectory("database");
    storage.setCommonCheckpointEnabled(true);
    org.tron.core.config.args.StorageConfig.StateArchiveHotStoreConfig hot =
        new org.tron.core.config.args.StorageConfig.StateArchiveHotStoreConfig();
    hot.setEnabled(true);
    storage.setStateArchiveHotStoreSettings(hot);
    NativeMemoryBudget.Result result = NativeMemoryBudget.checkAtStartup(storage,
        temporaryFolder.newFolder().toPath());
    assertEquals(9 * 32L * MIB, (long) result.items.get("legacy-hot.cache"));
    assertEquals(29 * 128L * MIB, (long) result.items.get("legacy-hot.memtable"));
  }

  /** §3.8.2: AMD-002 class, all LevelDB, old core-node JVM flags, about 27.8 GiB. */
  @Test
  public void reproducesTheDesignRecomputation() {
    NativeMemoryBudget.Jvm jvm = new NativeMemoryBudget.Jvm(18 * GIB, GIB, GIB / 2, GIB / 4,
        "G1", 0.05d);
    List<NativeMemoryBudget.Db> dbs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dbs.add(NativeMemoryBudget.levelDb("chainbase", 32 * MIB, 64 * MIB));
    }
    for (int i = 0; i < 35; i++) {
      dbs.add(NativeMemoryBudget.levelDb("chainbase", 32 * MIB, 16 * MIB));
    }
    for (PathStateNativeBudget.Entry entry : PathStateNativeBudget.entries(
        new PathStateDbSettingsConfig(), false, false)) {
      dbs.add(new NativeMemoryBudget.Db("pathState", entry.getCacheBytes(),
          entry.getMemtableBytes(), entry.getPinnedBytes()));
    }
    dbs.add(NativeMemoryBudget.levelDb("serving", 32 * MIB, 64 * MIB));
    NativeMemoryBudget.Result result = NativeMemoryBudget.compute(jvm, dbs, 22 * GIB,
        26 * GIB, 4 * GIB);
    // LevelDB memtables count twice: 2 x (5 x 64 + 35 x 16) MiB = 1.72 GiB (1,760 MiB).
    assertEquals(2 * (5 * 64 + 35 * 16) * MIB, (long) result.items.get("chainbase.memtable"));
    assertEquals(40 * 32 * MIB, (long) result.items.get("chainbase.cache"));
    assertEquals((22 * 32 + 3 * 32 + 3 * 64) * MIB,
        (long) result.items.get("pathState.cache"));
    double total = result.total / (double) GIB;
    assertTrue("A_cap=" + total, total > 27.5 && total < 28.1);
    assertEquals("OVER_BUDGET", result.verdict);
    assertTrue(result.describe().contains("verdict=OVER_BUDGET"));
  }

  @Test
  public void rocksDbPinnedOverflowAndBenchmarkCachesAreCounted() {
    PathStateDbSettingsConfig settings = new PathStateDbSettingsConfig();
    long plain = 0;
    long overflow = 0;
    for (PathStateNativeBudget.Entry entry : PathStateNativeBudget.entries(settings, true,
        false)) {
      plain += entry.getCacheBytes();
      overflow += StrictMathWrapper.max(0, entry.getPinnedBytes() - entry.getCacheBytes());
      if ("large".equals(entry.getProfile())) {
        // 36 L0 files x ~1 MiB of index+filter exceed the 32 MiB large-tier cache (§3.8.1).
        assertTrue(entry.getPinnedBytes() > entry.getCacheBytes());
      }
      // RocksDB memtable bound: write_buffer_size x max_write_buffer_number.
      assertEquals(0, entry.getMemtableBytes() % 2);
    }
    assertTrue(overflow > 0);
    long withBenchmark = 0;
    for (PathStateNativeBudget.Entry entry : PathStateNativeBudget.entries(settings, true,
        true)) {
      withBenchmark += entry.getCacheBytes();
    }
    // Stores 5 and 22: 64 MiB -> 256 MiB each, +384 MiB in total, never hidden.
    assertEquals(384 * MIB, withBenchmark - plain);

    NativeMemoryBudget.Db pinned = NativeMemoryBudget.rocksDb("pathState", 32 * MIB, 64 * MIB, 2,
        40 * MIB);
    NativeMemoryBudget.Result result = NativeMemoryBudget.compute(
        new NativeMemoryBudget.Jvm(GIB, GIB, GIB / 4, GIB / 4, "G1", 0.05d),
        java.util.Collections.singletonList(pinned), null, null, 4 * GIB);
    assertEquals(8 * MIB, (long) result.items.get("pathState.pinnedOverflow"));
    assertEquals("UNKNOWN", result.verdict);
  }

  @Test
  public void unsetMaxDirectMemoryEqualsTheMaxHeap() {
    NativeMemoryBudget.Jvm jvm = NativeMemoryBudget.effectiveJvm(8 * GIB, 0L, -1L, GIB / 4,
        "G1");
    assertEquals(8 * GIB, jvm.maxDirect);
    assertEquals(NativeMemoryBudget.METASPACE_PLACEHOLDER, jvm.maxMetaspace);
    assertEquals(GIB, NativeMemoryBudget.effectiveJvm(8 * GIB, GIB, GIB / 2, GIB / 4, "ZGC")
        .maxDirect);
    assertEquals("G1", NativeMemoryBudget.gcName("G1 Young Generation", "unknown"));
    assertEquals("ZGC", NativeMemoryBudget.gcName("ZGC Cycles", "unknown"));
    assertEquals("CMS", NativeMemoryBudget.gcName("ConcurrentMarkSweep", "unknown"));
    assertEquals("Parallel", NativeMemoryBudget.gcName("PS Scavenge", "unknown"));
    // The live JVM is read without failing.
    assertTrue(NativeMemoryBudget.readJvm().maxHeap > 0);
  }

  @Test
  public void cgroupV2LimitsAreReadAndMissingFilesDegradeToUnknown() throws Exception {
    Path proc = temporaryFolder.newFile("cgroup").toPath();
    Path root = temporaryFolder.newFolder("sys").toPath();
    Files.write(proc, "0::/system.slice/java-tron.service\n".getBytes(StandardCharsets.UTF_8));
    Path group = Files.createDirectories(root.resolve("system.slice/java-tron.service"));
    Files.write(group.resolve("memory.high"), "23622320128\n".getBytes(StandardCharsets.UTF_8));
    Files.write(group.resolve("memory.max"), "max\n".getBytes(StandardCharsets.UTF_8));
    Long[] limits = NativeMemoryBudget.readCgroup(proc, root);
    assertEquals(Long.valueOf(23622320128L), limits[0]);
    assertNull(limits[1]);

    Long[] missing = NativeMemoryBudget.readCgroup(root.resolve("absent"), root);
    assertNull(missing[0]);
    assertNull(missing[1]);
  }

  @Test
  public void chainbaseDatabasesAreListedFromTheDatabaseDirectory() throws Exception {
    Path database = temporaryFolder.newFolder("database").toPath();
    Files.createDirectories(database.resolve("account"));
    Files.createDirectories(database.resolve("block"));
    Files.createDirectories(database.resolve("checkpoint"));
    assertEquals(java.util.Arrays.asList("account", "block"),
        NativeMemoryBudget.chainbaseDatabases(database));
  }
}
