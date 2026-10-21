package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.tron.core.db2.archive.HistoricalTestState.address;
import static org.tron.core.db2.archive.HistoricalVm.RETURN_TOP;
import static org.tron.core.db2.archive.HistoricalVm.callOp;
import static org.tron.core.db2.archive.HistoricalVm.callToken;
import static org.tron.core.db2.archive.HistoricalVm.code;
import static org.tron.core.db2.archive.HistoricalVm.push20;
import static org.tron.core.db2.archive.HistoricalVm.push20Raw;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Test;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.db2.archive.query.HistoricalFailureCategory;
import org.tron.core.db2.archive.query.HistoricalQueryBudgetException;
import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;
import org.tron.core.db2.archive.query.HistoricalQueryException;
import org.tron.core.vm.Op;
import org.tron.core.vm.config.VMConfig;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;

/**
 * Chapter 05 WP-2 (D6, review R3-01): historical-class failures fail the whole request from any
 * frame. T-2a nested negative matrix, T-2b terminal-after-success gate, T-2c TVM semantics kept.
 */
public class HistoricalFailClosedTest {

  private static final byte[] CALLER = address(10);
  private static final byte[] CALLEE = address(11);
  private static final byte[] PEER = address(12);
  private static final byte[] HALF_CONTRACT = address(13);
  private static final long TOKEN = 1_000_001L;

  @After
  public void clearThreadLocals() {
    VMConfig.clearLocalSnapshot();
  }

  /** One failing callee shape, with the category the whole request must fail with. */
  private static final class Callee {
    private final String name;
    private final byte[] code;
    private final Consumer<HistoricalTestState> state;
    private final HistoricalFailureCategory expected;
    private final String detail;

    private Callee(String name, byte[] code, Consumer<HistoricalTestState> state,
        HistoricalFailureCategory expected, String detail) {
      this.name = name;
      this.code = code;
      this.state = state;
      this.expected = expected;
      this.detail = detail;
    }
  }

  private static List<Callee> failingCallees() {
    List<Callee> callees = new ArrayList<>();
    callees.add(new Callee("selfdestruct", code("6000ff"), state -> { },
        HistoricalFailureCategory.CAPABILITY, HistoricalQueryCapabilityException.UNSUPPORTED));
    // STATICCALL to ValidateMultiSign (0x0a): outside the precompile allowlist.
    callees.add(new Callee("precompile", code(callOp(Op.STATICCALL, push20Raw(0x0a)), "00"),
        state -> { }, HistoricalFailureCategory.CAPABILITY,
        HistoricalQueryCapabilityException.UNSUPPORTED));
    // Corrupt Account row read by BALANCE inside the callee.
    callees.add(new Callee("corrupt-row", code(push20(PEER), "31", "50", "00"),
        state -> state.put("account", PEER, new byte[]{(byte) 0x80}),
        HistoricalFailureCategory.CORRUPT, null));
    // Non-corrupt I/O failure of a callee read.
    callees.add(new Callee("io-failure", code(push20(PEER), "31", "50", "00"),
        state -> state.failWithIo("account", PEER, new IOException("disk")),
        HistoricalFailureCategory.DATA_ACCESS, null));
    // TRC10 transfer with a sufficient S(T) balance (inline assetV2 map).
    callees.add(new Callee("trc10-transfer", code(callToken(PEER, TOKEN, 5), "00"),
        state -> { }, HistoricalFailureCategory.CAPABILITY,
        HistoricalQueryCapabilityException.UNSUPPORTED));
    // Code without contract metadata: upstream code dereferences the missing contract (NPE).
    callees.add(new Callee("unexpected-npe", code(callOp(Op.CALL, push20(HALF_CONTRACT)), "50",
        "00"), state -> state.put("account", HALF_CONTRACT, Account.newBuilder()
        .setAddress(ByteString.copyFrom(HALF_CONTRACT)).setType(AccountType.Contract)
        .setAssetOptimized(true).build().toByteArray())
        .put("code", HALF_CONTRACT, code("00")),
        HistoricalFailureCategory.CAPABILITY, HistoricalQueryCapabilityException.UNEXPECTED_STATE));
    return callees;
  }

  private static Map<String, byte[]> callers() {
    Map<String, byte[]> callers = new LinkedHashMap<>();
    callers.put("CALL-ignore", HistoricalVm.callIgnoring(Op.CALL, CALLEE));
    callers.put("DELEGATECALL-ignore", HistoricalVm.callIgnoring(Op.DELEGATECALL, CALLEE));
    callers.put("STATICCALL-ignore", HistoricalVm.callIgnoring(Op.STATICCALL, CALLEE));
    callers.put("CALL-try-catch", HistoricalVm.callReturningFlag(Op.CALL, CALLEE));
    callers.put("STATICCALL-try-catch", HistoricalVm.callReturningFlag(Op.STATICCALL, CALLEE));
    return callers;
  }

  private static HistoricalTestState baseState(byte[] callerCode, byte[] calleeCode) {
    HistoricalTestState state = new HistoricalTestState().vmDefaults();
    state.contract(CALLER, callerCode).contract(CALLEE, calleeCode).account(PEER, 0L);
    // Both CALLER (delegatecall context) and CALLEE hold enough TRC10 inline at S(T).
    for (byte[] holder : Arrays.asList(CALLER, CALLEE)) {
      state.account(Account.newBuilder().setAddress(ByteString.copyFrom(holder))
          .setType(AccountType.Contract).setAssetOptimized(true)
          .putAssetV2(String.valueOf(TOKEN), 100L).build());
    }
    return state;
  }

  /** T-2a: no cell of the matrix may produce a REVERT-shaped or success-shaped answer. */
  @Test
  public void nestedHistoricalFailuresFailTheWholeRequest() throws Exception {
    for (Callee callee : failingCallees()) {
      for (Map.Entry<String, byte[]> caller : callers().entrySet()) {
        boolean staticTokenTransfer = callee.name.equals("trc10-transfer")
            && caller.getKey().startsWith("STATICCALL");
        if (staticTokenTransfer) {
          // A value-bearing CALLTOKEN in a static frame is a TVM static-modification failure
          // in upstream too; it is covered by T-2c, not a capability case.
          continue;
        }
        HistoricalTestState state = baseState(caller.getValue(), callee.code);
        callee.state.accept(state);
        String cell = caller.getKey() + " x " + callee.name;
        try (HistoricalQuerySession session = state.open()) {
          RuntimeException failure = expectFailure(cell, session, CALLER);
          assertEquals(cell, callee.expected, HistoricalFailureCategory.of(failure));
          if (callee.detail != null) {
            assertEquals(cell, callee.detail, HistoricalFailureCategory.detail(failure));
          }
          assertTrue(cell, session.isTerminal());
          assertSame(cell, failure, session.terminalFailure().orElse(null));
        }
      }
    }
  }

  /** T-2a budget column: the read budget runs out inside the callee. */
  @Test
  public void nestedBudgetExhaustionFailsTheWholeRequest() throws Exception {
    List<byte[]> peers = new ArrayList<>();
    StringBuilder reads = new StringBuilder();
    for (int i = 0; i < 6; i++) {
      byte[] peer = address(40 + i);
      peers.add(peer);
      reads.append(push20(peer)).append("31").append("50");
    }
    byte[] calleeCode = code(reads.toString(), "00");
    for (Map.Entry<String, byte[]> caller : callers().entrySet()) {
      long total;
      HistoricalTestState generous = baseState(caller.getValue(), calleeCode);
      peers.forEach(peer -> generous.account(peer, 1L));
      try (HistoricalQuerySession session = generous.open()) {
        ProgramResult result = HistoricalVm.call(session, CALLER);
        assertNull(caller.getKey(), result.getException());
        total = session.getReadCount();
      }
      HistoricalTestState tight = baseState(caller.getValue(), calleeCode);
      peers.forEach(peer -> tight.account(peer, 1L));
      try (HistoricalQuerySession session = tight.open(
          new HistoricalQuerySession.Limits(total - 2, 64L << 20, 10_000L))) {
        RuntimeException failure = expectFailure(caller.getKey(), session, CALLER);
        assertEquals(caller.getKey(), HistoricalFailureCategory.BUDGET,
            HistoricalFailureCategory.of(failure));
        assertTrue(failure instanceof HistoricalQueryBudgetException);
      }
    }
  }

  /** Same shapes in current (non-historical) semantics: the caller completes normally. */
  @Test
  public void controlCellsWithoutFailureSucceed() throws Exception {
    HistoricalTestState state = baseState(HistoricalVm.callIgnoring(Op.CALL, CALLEE),
        code("00"));
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CALLER);
      assertNull(result.getException());
      assertEquals(new DataWord(42), new DataWord(result.getHReturn()));
      assertFalse(session.isTerminal());
    }
  }

  /**
   * T-2b: the root frame computes a normal return value, but a failure recorded during the
   * request (and swallowed by some layer) is TERMINAL; the request still fails with it.
   */
  @Test
  public void terminalSessionFailsEvenWhenTheRootFrameSucceeds() throws Exception {
    byte[] slotReader = code("6000", "54", RETURN_TOP); // SLOAD(0) and return it
    HistoricalTestState state = new HistoricalTestState().vmDefaults().contract(CALLER,
        slotReader);
    HistoricalQueryException swallowed = new HistoricalQueryException(
        HistoricalQueryException.Reason.CORRUPT, "recorded but swallowed below the VM");
    try (HistoricalQuerySession session = state.open()) {
      state.onRead((db, key) -> {
        if ("storage-row".equals(db)) {
          session.markTerminal(swallowed);
        }
      });
      RuntimeException failure = expectFailure("root", session, CALLER);
      assertSame(swallowed, failure);
      assertEquals(HistoricalFailureCategory.CORRUPT, HistoricalFailureCategory.of(failure));
    }
  }

  /** T-2b at the validate stage (P6): the recorded cause wins over a validation failure. */
  @Test
  public void terminalCauseWinsOverValidationFailure() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .contract(CALLER, code("00"));
    HistoricalQueryBudgetException recorded = new HistoricalQueryBudgetException("recorded");
    try (HistoricalQuerySession session = state.open()) {
      state.onRead((db, key) -> {
        if ("contract".equals(db)) {
          session.markTerminal(recorded);
        }
      });
      RuntimeException failure = expectFailure("validate", session, CALLER);
      assertSame(recorded, failure);
    }
  }

  /** T-2c: TVM semantic failures in a child frame keep upstream behavior. */
  @Test
  public void tvmSemanticFailuresInChildFramesKeepUpstreamSemantics() throws Exception {
    Map<String, byte[]> callees = new LinkedHashMap<>();
    callees.put("revert", code("60006000fd"));
    callees.put("invalid-opcode", code("fe"));
    callees.put("stack-underflow", code("01"));
    callees.put("static-token-transfer", code(callToken(PEER, TOKEN, 5), "00"));
    for (Map.Entry<String, byte[]> callee : callees.entrySet()) {
      boolean staticOnly = callee.getKey().equals("static-token-transfer");
      for (Map.Entry<String, byte[]> caller : callers().entrySet()) {
        if (staticOnly && !caller.getKey().startsWith("STATICCALL")) {
          continue;
        }
        HistoricalTestState state = baseState(caller.getValue(), callee.getValue());
        String cell = caller.getKey() + " x " + callee.getKey();
        try (HistoricalQuerySession session = state.open(
            new HistoricalQuerySession.Limits(100_000L, 64L << 20, 60_000L))) {
          ProgramResult result = HistoricalVm.call(session, CALLER);
          assertNull(cell, result.getException());
          assertFalse(cell, result.isRevert());
          DataWord returned = new DataWord(result.getHReturn());
          if (caller.getKey().endsWith("try-catch")) {
            assertEquals(cell, DataWord.ZERO(), returned);
          } else {
            assertEquals(cell, new DataWord(42), returned);
          }
          assertFalse(cell, session.isTerminal());
        }
      }
    }
  }

  /** The root frame's own REVERT is a TVM result, not a historical failure. */
  @Test
  public void rootRevertStaysATvmResult() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .contract(CALLER, code("60006000fd"));
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CALLER);
      assertTrue(result.isRevert());
      assertFalse(session.isTerminal());
    }
  }

  @Test
  public void sessionRecordsTheFirstTerminalCauseAndRethrowsIt() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults();
    try (HistoricalQuerySession session = state.open()) {
      RuntimeException first = new HistoricalQueryCapabilityException("first");
      assertSame(first, session.markTerminal(first));
      session.markTerminal(new HistoricalQueryBudgetException("second"));
      assertSame(first, assertThrows(RuntimeException.class, session::checkActive));
      assertSame(first, assertThrows(RuntimeException.class,
          () -> session.getCode(CALLER)));
    }
  }

  @Test
  public void escapingSessionFailuresBecomeTerminal() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .put("account", PEER, new byte[]{(byte) 0x80});
    try (HistoricalQuerySession session = state.open()) {
      HistoricalQueryException corrupt = assertThrows(HistoricalQueryException.class,
          () -> session.getAccount(PEER));
      assertEquals(HistoricalQueryException.Reason.CORRUPT, corrupt.getReason());
      assertTrue(session.isTerminal());
      assertSame(corrupt, assertThrows(RuntimeException.class, () -> session.getCode(CALLER)));
    }
  }

  @Test
  public void aSessionBindsAtMostOneExecution() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults();
    try (HistoricalQuerySession session = state.open()) {
      session.bindExecution();
      assertThrows(IllegalStateException.class, session::bindExecution);
    }
  }

  private static RuntimeException expectFailure(String cell, HistoricalQuerySession session,
      byte[] contract) throws Exception {
    ProgramResult result;
    try {
      result = HistoricalVm.call(session, contract);
    } catch (RuntimeException failure) {
      assertNotNull(cell, failure);
      return failure;
    }
    fail(cell + ": expected a historical failure, got exception=" + result.getException()
        + " revert=" + result.isRevert() + " return="
        + org.bouncycastle.util.encoders.Hex.toHexString(result.getHReturn()));
    return null;
  }
}
