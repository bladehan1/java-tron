package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.tron.core.db2.archive.HistoricalTestState.address;
import static org.tron.core.db2.archive.HistoricalVm.RETURN_TOP;
import static org.tron.core.db2.archive.HistoricalVm.code;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.ChainBaseManager;
import org.tron.core.db2.archive.query.session.HistoricalExecutionScope;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.StoreFactory;
import org.tron.core.vm.JumpTable;
import org.tron.core.vm.Op;
import org.tron.core.vm.OperationRegistry;
import org.tron.core.vm.config.ConfigLoader;
import org.tron.core.vm.config.VMConfig;
import org.tron.core.vm.repository.RepositoryImpl;

/**
 * Chapter 05 WP-4 (review R3-03): VMConfig snapshot and operation table are request-scoped for
 * historical execution. T-4a concurrent latest table mutation, T-4b thread reuse, T-4c field
 * alignment of the two config loaders.
 */
public class HistoricalVmIsolationTest {

  private static final byte[] CONTRACT = address(50);

  @After
  public void clear() {
    VMConfig.clearLocalSnapshot();
    OperationRegistry.clearRequestTable();
  }

  private static JumpTable sharedConstantTable() {
    return (JumpTable) ReflectionTestUtils.getField(OperationRegistry.class,
        "CONSTANT_CALL_TABLE");
  }

  @SuppressWarnings("unchecked")
  private static VMConfig.Snapshot localSnapshot() {
    ThreadLocal<VMConfig.Snapshot> local = (ThreadLocal<VMConfig.Snapshot>)
        ReflectionTestUtils.getField(VMConfig.class, "localSnapshot");
    return local.get();
  }

  /** MLOAD/MSTORE-heavy body around one SLOAD, returning the loaded word. */
  private static byte[] memoryHeavy() {
    StringBuilder body = new StringBuilder();
    for (int i = 0; i < 8; i++) {
      body.append("6001").append(String.format("61%04x", i * 32)).append("52"); // MSTORE
    }
    body.append("6000").append("54").append("50"); // SLOAD(0), drop
    for (int i = 0; i < 8; i++) {
      body.append(String.format("61%04x", i * 32)).append("51").append("50"); // MLOAD, drop
    }
    return code(body.toString(), "6007", RETURN_TOP);
  }

  private static HistoricalTestState oldState() {
    // An old T: none of the cost-adjusting proposals is active.
    return new HistoricalTestState().vmDefaults()
        .property("ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX", 0L)
        .property("ALLOW_ENERGY_ADJUSTMENT", 0L)
        .property("ALLOW_TVM_OSAKA", 0L)
        .property("ALLOW_TVM_SELFDESTRUCT_RESTRICTION", 0L)
        .contract(CONTRACT, memoryHeavy());
  }

  private static void mutateSharedTableFromLatestThread() throws Exception {
    ExecutorService latest = Executors.newSingleThreadExecutor();
    try {
      latest.submit(() -> {
        VMConfig.Snapshot snapshot = new VMConfig.Snapshot();
        snapshot.allowHigherLimitForMaxCpuTimeOfOneTx = true;
        snapshot.allowEnergyAdjustment = true;
        snapshot.allowTvmOsaka = true;
        snapshot.allowTvmSelfdestructRestriction = true;
        VMConfig.setLocalSnapshot(snapshot);
        try {
          OperationRegistry.prepareAndGetTable(true);
        } finally {
          VMConfig.clearLocalSnapshot();
        }
        return null;
      }).get(10, TimeUnit.SECONDS);
    } finally {
      latest.shutdownNow();
    }
  }

  /** T-4a: a latest constant call re-preparing the shared table mid-request changes nothing. */
  @Test
  public void latestTableMutationDoesNotLeakIntoAHistoricalRequest() throws Exception {
    long baseline;
    HistoricalTestState quiet = oldState();
    try (HistoricalQuerySession session = quiet.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      baseline = result.getEnergyUsed();
    }

    HistoricalTestState state = oldState();
    AtomicReference<JumpTable> during = new AtomicReference<>();
    AtomicReference<Object> mloadBefore = new AtomicReference<>();
    AtomicReference<Object> mloadAfter = new AtomicReference<>();
    state.onRead((db, key) -> {
      if (!"storage-row".equals(db)) {
        return;
      }
      JumpTable table = OperationRegistry.getTable(true);
      during.set(table);
      mloadBefore.set(table.get(Op.MLOAD));
      try {
        mutateSharedTableFromLatestThread();
      } catch (Exception failure) {
        throw new AssertionError(failure);
      }
      mloadAfter.set(OperationRegistry.getTable(true).get(Op.MLOAD));
    });
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(new DataWord(7), new DataWord(result.getHReturn()));
      assertEquals("S(T) costs are unaffected by the concurrent latest call",
          baseline, result.getEnergyUsed());
    }
    assertNotNull(during.get());
    assertNotSame("historical request never uses the shared table", sharedConstantTable(),
        during.get());
    assertSame(mloadBefore.get(), mloadAfter.get());
    assertNotSame("the latest call did adjust the shared table",
        sharedConstantTable().get(Op.MLOAD), mloadBefore.get());
  }

  @Test
  public void requestTablesFollowTheStSwitchesAndAreCached() throws Exception {
    Map<String, Long> variants = new LinkedHashMap<>();
    variants.put("ALLOW_TVM_OSAKA", 1L);
    variants.put("ALLOW_TVM_SELFDESTRUCT_RESTRICTION", 1L);
    variants.put("ALLOW_ENERGY_ADJUSTMENT", 1L);
    JumpTable oldTable = tableFor(oldState());
    JumpTable again = tableFor(oldState());
    assertSame("cached per variant combination", oldTable, again);
    HistoricalTestState newer = oldState();
    variants.forEach(newer::property);
    JumpTable newTable = tableFor(newer);
    assertNotSame(oldTable, newTable);
    assertNotSame(oldTable.get(Op.SUICIDE), newTable.get(Op.SUICIDE));
    assertNotSame(oldTable.get(Op.VOTEWITNESS), newTable.get(Op.VOTEWITNESS));
  }

  private static JumpTable tableFor(HistoricalTestState state) throws Exception {
    AtomicReference<JumpTable> seen = new AtomicReference<>();
    state.onRead((db, key) -> {
      if ("storage-row".equals(db)) {
        seen.set(OperationRegistry.getTable(true));
      }
    });
    try (HistoricalQuerySession session = state.open()) {
      assertNull(HistoricalVm.call(session, CONTRACT).getException());
    }
    return seen.get();
  }

  /** T-4b: pooled-thread reuse leaves neither a request table nor a VM config snapshot behind. */
  @Test
  public void threadReuseLeavesNoRequestStateBehind() throws Exception {
    try (HistoricalQuerySession session = oldState().open()) {
      assertNull(HistoricalVm.call(session, CONTRACT).getException());
    }
    assertSame(sharedConstantTable(), OperationRegistry.getTable(true));
    assertNull(localSnapshot());

    HistoricalTestState failing = oldState().put("account", CONTRACT, new byte[]{(byte) 0x80});
    try (HistoricalQuerySession session = failing.open()) {
      assertThrows(RuntimeException.class, () -> HistoricalVm.call(session, CONTRACT));
    }
    assertSame(sharedConstantTable(), OperationRegistry.getTable(true));
    assertNull(localSnapshot());
    assertNull(HistoricalExecutionScope.current());
  }

  /** T-4c: load(StoreFactory) and load(Repository) fill every Snapshot field the same way. */
  @Test
  public void bothConfigLoadersReadEverySnapshotField() throws Exception {
    long originalForkBlock = CommonParameter.getInstance().blockNumForEnergyLimit;
    CommonParameter.getInstance().blockNumForEnergyLimit = 4_727_890L;
    try {
      assertLoadersAligned();
    } finally {
      CommonParameter.getInstance().blockNumForEnergyLimit = originalForkBlock;
    }
  }

  private static void assertLoadersAligned() throws Exception {
    Map<Long, VMConfig.Snapshot> historical = new LinkedHashMap<>();
    for (long value : new long[]{0L, 1L, 5_000_000L}) {
      VMConfig.Snapshot fromStore = loadFromStore(value);
      VMConfig.Snapshot fromRepository = loadFromRepository(value);
      for (Field field : VMConfig.Snapshot.class.getFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        assertEquals(field.getName() + " @" + value, field.get(fromStore),
            field.get(fromRepository));
      }
      historical.put(value, fromRepository);
    }
    for (Field field : VMConfig.Snapshot.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers())
          || "energyLimitHardFork".equals(field.getName())) {
        continue;
      }
      assertNotEquals("field " + field.getName() + " must be read from state",
          field.get(historical.get(0L)), field.get(historical.get(1L)));
    }
    assertNotEquals("energyLimitHardFork must follow latest_block_header_number",
        historical.get(1L).energyLimitHardFork, historical.get(5_000_000L).energyLimitHardFork);
  }

  private static VMConfig.Snapshot loadFromStore(long value) {
    DynamicPropertiesStore store = mock(DynamicPropertiesStore.class, invocation -> {
      Class<?> type = invocation.getMethod().getReturnType();
      if (type == long.class) {
        return value;
      }
      if (type == int.class) {
        return (int) value;
      }
      if (type == boolean.class) {
        return "supportUnfreezeDelay".equals(invocation.getMethod().getName())
            ? value > 0 : value == 1L;
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    });
    ChainBaseManager manager = mock(ChainBaseManager.class);
    when(manager.getDynamicPropertiesStore()).thenReturn(store);
    StoreFactory factory = mock(StoreFactory.class);
    when(factory.getChainBaseManager()).thenReturn(manager);
    ConfigLoader.load(factory, true);
    VMConfig.Snapshot snapshot = localSnapshot();
    VMConfig.clearLocalSnapshot();
    return snapshot;
  }

  private static VMConfig.Snapshot loadFromRepository(long value) throws Exception {
    HistoricalTestState state = new HistoricalTestState().anyProperty(value);
    try (HistoricalQuerySession session = state.open()) {
      ConfigLoader.load(RepositoryImpl.createHistoricalRoot(null, session));
      VMConfig.Snapshot snapshot = localSnapshot();
      VMConfig.clearLocalSnapshot();
      return snapshot;
    }
  }
}
