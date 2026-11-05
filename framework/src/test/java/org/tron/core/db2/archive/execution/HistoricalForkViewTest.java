package org.tron.core.db2.archive.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.tron.core.db2.archive.execution.HistoricalTestState.address;
import static org.tron.core.db2.archive.execution.HistoricalVm.RETURN_TOP;
import static org.tron.core.db2.archive.execution.HistoricalVm.code;
import static org.tron.core.db2.archive.execution.HistoricalVm.push20Raw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.math.Maths;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.utils.ForkController;
import org.tron.common.utils.ForkStateSource;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.Parameter.ForkBlockVersionConsts;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;
import org.tron.core.db2.archive.query.session.HistoricalExecutionScope;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.vm.config.VMConfig;
import org.tron.core.vm.program.Program;

/** Chapter 05 WP-7 (D7, review X2-04): ForkView@S(T). T-7a golden, T-7b poisoned latest, T-7c. */
public class HistoricalForkViewTest {

  private static final byte[] CONTRACT = address(20);
  private static final long AFTER_FORKS = 1_700_000_000_000L;

  private Object originalManager;

  @Before
  public void saveForkController() {
    originalManager = ReflectionTestUtils.getField(ForkController.instance(), "manager");
  }

  @After
  public void restoreForkController() {
    ReflectionTestUtils.setField(ForkController.instance(), "manager", originalManager);
    VMConfig.clearLocalSnapshot();
  }

  // ---------------------------------------------------------------------------------------
  // T-7a: evaluate(v, src) is byte-for-byte the previous pass(v) for every version and input.

  /** The pre-extraction ForkController algorithm, verbatim except for the store parameter. */
  private static boolean legacyPass(int version, DynamicPropertiesStore store) {
    if (version > ForkBlockVersionEnum.VERSION_4_0.getValue()) {
      ForkBlockVersionEnum versionEnum = ForkBlockVersionEnum.getForkBlockVersionEnum(version);
      if (versionEnum == null) {
        return false;
      }
      long latestBlockTime = store.getLatestBlockHeaderTimestamp();
      long maintenanceTimeInterval = store.getMaintenanceTimeInterval();
      long hardForkTime = ((versionEnum.getHardForkTime() - 1) / maintenanceTimeInterval + 1)
          * maintenanceTimeInterval;
      if (latestBlockTime < hardForkTime) {
        return false;
      }
      byte[] stats = store.statsByVersion(version);
      if (stats == null || stats.length == 0) {
        return false;
      }
      int count = 0;
      for (byte stat : stats) {
        if (stat == (byte) 1) {
          ++count;
        }
      }
      return count >= Maths.ceil((double) versionEnum.getHardForkRate() * stats.length / 100,
          store.disableJavaLangMath());
    }
    if (version == ForkBlockVersionConsts.ENERGY_LIMIT) {
      return store.getLatestBlockHeaderNumber()
          >= CommonParameter.getInstance().getBlockNumForEnergyLimit();
    }
    byte[] stats = store.statsByVersion(version);
    if (stats == null || stats.length == 0) {
      return false;
    }
    for (byte stat : stats) {
      if (stat != (byte) 1) {
        return false;
      }
    }
    return true;
  }

  private static byte[] stats(int size, int upgraded) {
    byte[] stats = new byte[size];
    Arrays.fill(stats, 0, upgraded, (byte) 1);
    return stats;
  }

  @Test
  public void evaluateMatchesTheLegacyAlgorithmForEveryVersionAndInput() {
    List<byte[]> statVectors = Arrays.asList(null, new byte[0], stats(27, 27), stats(27, 0),
        stats(27, 18), stats(27, 19), stats(27, 21), stats(27, 22), stats(5, 4), stats(1, 1));
    long[] timestamps = {0L, 1_596_779_999_999L, 1_596_780_000_000L, AFTER_FORKS};
    long[] intervals = {21_600_000L, 300_000L};
    long[] headNumbers = {0L, 4_727_889L, 4_727_890L};
    boolean[] maths = {false, true};
    List<Integer> versions = new ArrayList<>();
    for (int v = 0; v <= 45; v++) {
      versions.add(v);
    }
    int checked = 0;
    ChainBaseManager manager = mock(ChainBaseManager.class);
    ReflectionTestUtils.setField(ForkController.instance(), "manager", manager);
    for (byte[] statVector : statVectors) {
      for (long timestamp : timestamps) {
        for (long interval : intervals) {
          for (long head : headNumbers) {
            for (boolean math : maths) {
              DynamicPropertiesStore store = mock(DynamicPropertiesStore.class);
              when(store.getLatestBlockHeaderTimestamp()).thenReturn(timestamp);
              when(store.getMaintenanceTimeInterval()).thenReturn(interval);
              when(store.getLatestBlockHeaderNumber()).thenReturn(head);
              when(store.disableJavaLangMath()).thenReturn(math);
              when(store.statsByVersion(anyInt())).thenReturn(statVector);
              when(manager.getDynamicPropertiesStore()).thenReturn(store);
              ForkStateSource source = dpsSource(store);
              for (int version : versions) {
                boolean legacy = legacyPass(version, store);
                assertEquals("v" + version, legacy, ForkController.instance().pass(version));
                assertEquals("v" + version, legacy, ForkController.evaluate(version, source));
                checked++;
              }
            }
          }
        }
      }
    }
    assertTrue(checked > 10_000);
  }

  private static ForkStateSource dpsSource(DynamicPropertiesStore store) {
    return new ForkStateSource() {
      @Override
      public long latestBlockHeaderTimestamp() {
        return store.getLatestBlockHeaderTimestamp();
      }

      @Override
      public long maintenanceTimeInterval() {
        return store.getMaintenanceTimeInterval();
      }

      @Override
      public byte[] statsByVersion(int version) {
        return store.statsByVersion(version);
      }

      @Override
      public boolean disableJavaLangMath() {
        return store.disableJavaLangMath();
      }

      @Override
      public long latestBlockHeaderNumber() {
        return store.getLatestBlockHeaderNumber();
      }
    };
  }

  @Test
  public void historicalViewEvaluatesTheSameRuleAtSt() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .property("latest_block_header_timestamp", AFTER_FORKS)
        .put("properties", HistoricalTestState.bytes("FORK_VERSION_35"), stats(27, 19))
        .put("properties", HistoricalTestState.bytes("FORK_VERSION_36"), stats(27, 21));
    try (HistoricalQuerySession session = state.open()) {
      assertTrue(session.forks().pass(ForkBlockVersionEnum.VERSION_4_8_1_1)); // 70%: 19/27
      assertFalse(session.forks().pass(ForkBlockVersionEnum.VERSION_4_8_2)); // 80%: 21/27
      assertFalse(session.forks().pass(ForkBlockVersionEnum.VERSION_4_8_2_2)); // absent
      long reads = session.getReadCount();
      assertTrue(session.forks().pass(ForkBlockVersionEnum.VERSION_4_8_1_1));
      assertEquals("decisions are cached per request", reads, session.getReadCount());
    }
  }

  @Test
  public void missingRequiredForkInputFailsClosed() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .removeProperty("MAINTENANCE_TIME_INTERVAL");
    try (HistoricalQuerySession session = state.open()) {
      HistoricalQueryCapabilityException missing = assertThrows(
          HistoricalQueryCapabilityException.class,
          () -> session.forks().pass(ForkBlockVersionEnum.VERSION_4_8_1_1));
      assertEquals(HistoricalQueryCapabilityException.UNSUPPORTED_STATE, missing.getDetail());
      assertTrue(session.isTerminal());
    }
  }

  // ---------------------------------------------------------------------------------------
  // T-7b: the ModExp fork gate follows S(T), never latest.

  /** STATICCALL ModExp with baseLen=0, expLen=1025, modLen=0; return the success flag. */
  private static byte[] modExpCaller() {
    return code("60006000" + "52", "6104016020" + "52", "60006040" + "52",
        "6000600060606000", push20Raw(0x05), "620f4240", "fa", RETURN_TOP);
  }

  private static void latestForkState(boolean passed) {
    ChainBaseManager manager = mock(ChainBaseManager.class);
    DynamicPropertiesStore store = mock(DynamicPropertiesStore.class);
    when(store.getLatestBlockHeaderTimestamp()).thenReturn(AFTER_FORKS);
    when(store.getMaintenanceTimeInterval()).thenReturn(21_600_000L);
    when(store.statsByVersion(anyInt())).thenReturn(passed ? stats(27, 27) : stats(27, 0));
    when(manager.getDynamicPropertiesStore()).thenReturn(store);
    ReflectionTestUtils.setField(ForkController.instance(), "manager", manager);
  }

  @Test
  public void modExpGateUsesStWhenLatestHasPassed() throws Exception {
    latestForkState(true);
    assertTrue(ForkController.instance().pass(ForkBlockVersionEnum.VERSION_4_8_1_1));
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .property("latest_block_header_timestamp", AFTER_FORKS)
        .contract(CONTRACT, modExpCaller());
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(1L, new org.tron.common.runtime.vm.DataWord(result.getHReturn()).longValue());
    }
  }

  @Test
  public void modExpGateUsesStWhenLatestHasNotPassed() throws Exception {
    latestForkState(false);
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .property("latest_block_header_timestamp", AFTER_FORKS)
        .put("properties", HistoricalTestState.bytes("FORK_VERSION_35"), stats(27, 27))
        .contract(CONTRACT, modExpCaller());
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertTrue(String.valueOf(result.getException()),
          result.getException() instanceof Program.OutOfTimeException);
      assertEquals("CPU timeout for modExp executing", result.getException().getMessage());
    }
  }

  @Test
  public void storageReadsDoNotRequireForksOutsideDevelop() throws Exception {
    byte[] sload = code("6000", "54", RETURN_TOP);
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .property("latest_block_header_timestamp", AFTER_FORKS)
        .contract(CONTRACT, sload);
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(0, state.reads("properties", HistoricalTestState.bytes("FORK_VERSION_38")));
    }
  }

  // ---------------------------------------------------------------------------------------
  // T-7c: the latest ForkController is a trap inside a historical scope.

  @Test
  public void forkControllerInstanceTrapsInsideHistoricalScope() throws Exception {
    latestForkState(true);
    HistoricalTestState state = new HistoricalTestState().vmDefaults();
    try (HistoricalQuerySession session = state.open()) {
      try (HistoricalExecutionScope ignored = HistoricalExecutionScope.enter(session)) {
        HistoricalQueryCapabilityException trap = assertThrows(
            HistoricalQueryCapabilityException.class, ForkController::instance);
        assertEquals(HistoricalQueryCapabilityException.LATEST_STATE_ACCESS, trap.getDetail());
        assertSame(trap, session.terminalFailure().orElse(null));
      }
      assertTrue(session.isTerminal());
    }
    assertTrue("trap is scoped", ForkController.instance().pass(
        ForkBlockVersionEnum.VERSION_4_8_1_1));
  }
}
