package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.tron.core.db2.archive.HistoricalTestState.address;

import java.util.Arrays;
import org.junit.After;
import org.junit.Test;
import org.tron.core.vm.HistoricalCapabilityException;
import org.tron.core.vm.Op;
import org.tron.core.vm.config.VMConfig;

/** A parent that ignores child CALL failure must not turn historical failure into success. */
public class HistoricalNestedFailureTest {

  private static final byte[] CALLER = address(10);
  private static final byte[] CALLEE = address(11);
  private static final byte[] PEER = address(12);

  @After
  public void clear() {
    VMConfig.clearLocalSnapshot();
  }

  @Test
  public void nestedCapabilityFailureAbortsRequestForEveryCallType() throws Exception {
    for (int opcode : Arrays.asList(Op.CALL, Op.STATICCALL, Op.DELEGATECALL, Op.CALLCODE)) {
      HistoricalTestState state = state(opcode, HistoricalVm.code("6000ff"));
      try (HistoricalQuerySession session = state.open()) {
        HistoricalCapabilityException failure = assertThrows(HistoricalCapabilityException.class,
            () -> HistoricalVm.call(session, CALLER));
        assertSame(failure, assertThrows(RuntimeException.class, session::checkActive));
      }
    }
  }

  @Test
  public void nestedCorruptReadAbortsRequest() throws Exception {
    HistoricalTestState state = state(Op.CALL,
        HistoricalVm.code(HistoricalVm.push20(PEER), "31", "50", "00"));
    state.put("account", PEER, new byte[]{(byte) 0x80});
    try (HistoricalQuerySession session = state.open()) {
      ArchivePersistenceException failure = assertThrows(ArchivePersistenceException.class,
          () -> HistoricalVm.call(session, CALLER));
      assertSame(failure, assertThrows(RuntimeException.class, session::checkActive));
    }
  }

  @Test
  public void nestedBudgetFailureAbortsRequest() throws Exception {
    StringBuilder reads = new StringBuilder();
    for (int slot = 0; slot < 100; slot++) {
      reads.append(String.format("60%02x5450", slot));
    }
    HistoricalTestState state = state(Op.CALL, HistoricalVm.code(reads.toString(), "00"));
    try (HistoricalQuerySession session = state.open(
        new HistoricalQuerySession.Limits(100, 1L << 20, 10_000))) {
      HistoricalQueryBudgetException failure = assertThrows(HistoricalQueryBudgetException.class,
          () -> HistoricalVm.call(session, CALLER));
      assertSame(failure, assertThrows(RuntimeException.class, session::checkActive));
    }
  }

  @Test
  public void ordinaryChildRevertCanStillBeHandledByParent() throws Exception {
    HistoricalTestState state = state(Op.CALL, HistoricalVm.code("60006000fd"));
    try (HistoricalQuerySession session = state.open()) {
      assertEquals(42, new org.tron.common.runtime.vm.DataWord(
          HistoricalVm.call(session, CALLER).getHReturn()).intValue());
      session.checkActive();
    }
  }

  private static HistoricalTestState state(int opcode, byte[] child) {
    return new HistoricalTestState().vmDefaults().account(HistoricalVm.OWNER, 1_000_000_000L)
        .contract(CALLER, HistoricalVm.callIgnoring(opcode, CALLEE)).contract(CALLEE, child);
  }
}
