package org.tron.common.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.tron.common.TestConstants.TEST_CONF;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.setting.RocksDbThreadBudget;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.config.args.Args;

/**
 * WP8-4 (design v2 ch08 §3.3, E6, G8-4 component part). Runs on the 9.7.4 line of this host; the
 * 5.15.10 adapter logic (high-water mark and inverted pool ids) is covered through
 * {@link RocksDbEnvHighWater}, which that adapter delegates to.
 */
public class RocksDbThreadBudgetTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    RocksDB.loadLibrary();
    RocksDbThreadBudget.resetForTest(null);
    Args.setParam(new String[] {"--output-directory",
        temporaryFolder.newFolder("out").toString()}, TEST_CONF);
    CommonParameter.getInstance().getStorage().setDbEngine("ROCKSDB");
  }

  @After
  public void tearDown() {
    RocksDbThreadBudget.resetForTest(null);
    Args.clearParam();
  }

  @Test
  public void inactiveBudgetNeverTouchesTheEnv() throws Exception {
    try (Options ignored = RocksDbSettings.getOptionsByDbName("account")) {
      RocksDbThreadBudget.afterChainbaseOptions();
    }
    RocksDbThreadBudget.beforeAuxiliaryOpen(8, 4);
    RocksDbThreadBudget.afterAuxiliaryOpen();
    RocksDbThreadBudget.periodicCheck();
    assertFalse(RocksDbThreadBudget.isActive());
    assertEquals(0, RocksDbThreadBudget.getEnvCallCount());
  }

  /** Read-back is only possible on the 9.7.4 line (x86 5.15.10 has no getter, §3.3.5). */
  @Test
  public void activationRaisesTheProcessPoolsAndChainbaseOptionsCannotLowerThem()
      throws Exception {
    Assume.assumeTrue(RocksDbEnvBudget.canReadPoolSizes());
    RocksDbThreadBudget.activate(4, 2, 0, false);
    assertTrue(RocksDbEnvBudget.compactionPoolSize() >= 4);
    assertTrue(RocksDbEnvBudget.flushPoolSize() >= 2);

    // Chainbase Options construction calls setIncreaseParallelism(1), which resets the pools.
    long loweredBefore = RocksDbThreadBudget.getLoweredCount();
    Options options = RocksDbSettings.getOptionsByDbName("account");
    options.close();
    assertTrue(RocksDbEnvBudget.compactionPoolSize() >= 4);
    assertTrue(RocksDbEnvBudget.flushPoolSize() >= 2);
    assertTrue(RocksDbThreadBudget.getLoweredCount() > loweredBefore);

    // Auxiliary job limits join the targets, only upwards.
    RocksDbThreadBudget.beforeAuxiliaryOpen(2, 1);
    assertEquals(4, RocksDbThreadBudget.getCompactionTarget());
    RocksDbThreadBudget.beforeAuxiliaryOpen(6, 3);
    assertEquals(6, RocksDbThreadBudget.getCompactionTarget());
    assertEquals(3, RocksDbThreadBudget.getFlushTarget());
    assertTrue(RocksDbEnvBudget.compactionPoolSize() >= 6);
    assertTrue(RocksDbEnvBudget.flushPoolSize() >= 3);

    // Periodic check restores pools lowered behind the budget's back (same API on both lines).
    try (Options lowering = new Options().setIncreaseParallelism(1)) {
      assertEquals(1, RocksDbEnvBudget.flushPoolSize());
    }
    RocksDbThreadBudget.periodicCheck();
    assertTrue(RocksDbEnvBudget.compactionPoolSize() >= 6);
    assertTrue(RocksDbEnvBudget.flushPoolSize() >= 3);
  }

  @Test
  public void chainbaseCompactThreadsAreNeverLoweredByTheBudget() {
    List<int[]> calls = new ArrayList<>();
    RocksDbThreadBudget.resetForTest(recording(calls, false));
    RocksDbThreadBudget.activate(4, 2, 16, false);
    assertArrayEquals(new int[] {16, 2}, calls.get(calls.size() - 1));
    RocksDbThreadBudget.afterChainbaseOptions();
    // 5.15.10 cannot read back: every re-check re-applies the high-water mark and is counted.
    assertEquals(1, RocksDbThreadBudget.getReassertionCount());
    assertArrayEquals(new int[] {16, 2}, calls.get(calls.size() - 1));
  }

  @Test
  public void line51510HighWaterUsesInvertedPoolIdsAndNeverLowers() {
    RocksDbEnvHighWater highWater = new RocksDbEnvHighWater();
    List<int[]> sets = new ArrayList<>();
    RocksDbEnvHighWater.PoolSetter setter = (threads, pool) -> sets.add(new int[] {threads, pool});
    highWater.raiseAndApply(4, 2, setter);
    // Compaction (C++ LOW) goes through Java FLUSH_POOL (0), flush (C++ HIGH) through
    // COMPACTION_POOL (1): RocksJava 5.15.10 env.cc:29-41.
    assertArrayEquals(new int[] {4, 0}, sets.get(0));
    assertArrayEquals(new int[] {2, 1}, sets.get(1));
    highWater.raiseAndApply(3, 1, setter);
    assertArrayEquals(new int[] {4, 0}, sets.get(2));
    assertArrayEquals(new int[] {2, 1}, sets.get(3));
    highWater.raiseAndApply(6, 3, setter);
    assertArrayEquals(new int[] {6, 0}, sets.get(4));
    assertArrayEquals(new int[] {3, 1}, sets.get(5));
    assertEquals(0, RocksDbEnvHighWater.JAVA_FLUSH_POOL_ID);
    assertEquals(1, RocksDbEnvHighWater.JAVA_COMPACTION_POOL_ID);
  }

  /** Chainbase OPTIONS-* bytes do not depend on the budget (only the Env is touched). */
  @Test
  public void chainbaseOptionsFileIsIdenticalWithAndWithoutTheBudget() throws Exception {
    String inactive = chainbaseOptions(temporaryFolder.newFolder("inactive").toPath());
    RocksDbThreadBudget.activate(4, 2, 0, false);
    String active = chainbaseOptions(temporaryFolder.newFolder("active").toPath());
    assertEquals(inactive, active);
  }

  /** G8-4 evidence on 5.15.10 is the Linux thread count; on this host only when Linux. */
  @Test
  public void linuxThreadNamesReachTheBudget() throws Exception {
    Assume.assumeTrue(Files.isDirectory(java.nio.file.Paths.get("/proc/self/task")));
    RocksDbThreadBudget.activate(3, 2, 0, false);
    // A RocksDB pool spawns its threads lazily on first job; open a database to start them.
    Path directory = temporaryFolder.newFolder("threads").toPath();
    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB database = RocksDB.open(options, directory.toString())) {
      database.put(new byte[] {1}, new byte[] {1});
    }
    long low;
    try (Stream<Path> tasks = Files.list(java.nio.file.Paths.get("/proc/self/task"))) {
      low = tasks.map(task -> {
        try {
          return new String(Files.readAllBytes(task.resolve("comm"))).trim();
        } catch (IOException e) {
          return "";
        }
      }).filter(name -> name.startsWith("rocksdb:low")).count();
    }
    assertTrue("rocksdb:low threads=" + low, low >= 3);
  }

  private String chainbaseOptions(Path parent) throws IOException {
    RocksDbDataSourceImpl source = new RocksDbDataSourceImpl(parent.toString(), "account");
    source.closeDB();
    try (Stream<Path> files = Files.list(parent.resolve("account"))) {
      List<Path> options = files.filter(path -> path.getFileName().toString()
          .startsWith("OPTIONS-")).sorted().collect(Collectors.toList());
      byte[] latest = Files.readAllBytes(options.get(options.size() - 1));
      // Strip comment lines (they carry no option values).
      return Arrays.stream(new String(latest).replace(parent.toString(), "<dir>").split("\n"))
          .filter(line -> !line.startsWith("#")).collect(Collectors.joining("\n"));
    }
  }

  private static RocksDbThreadBudget.EnvOps recording(List<int[]> calls, boolean readable) {
    return new RocksDbThreadBudget.EnvOps() {
      @Override
      public void raise(int compactionThreads, int flushThreads) {
        calls.add(new int[] {compactionThreads, flushThreads});
      }

      @Override
      public boolean canReadPoolSizes() {
        return readable;
      }

      @Override
      public int compactionPoolSize() {
        return -1;
      }

      @Override
      public int flushPoolSize() {
        return -1;
      }
    };
  }
}
