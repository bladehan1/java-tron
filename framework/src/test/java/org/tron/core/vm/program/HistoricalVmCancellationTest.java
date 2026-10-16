package org.tron.core.vm.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.vm.JumpTable;
import org.tron.core.vm.Op;
import org.tron.core.vm.Operation;
import org.tron.core.vm.VM;
import org.tron.core.vm.program.invoke.ProgramInvoke;
import org.tron.core.vm.repository.Repository;

public class HistoricalVmCancellationTest {

  private static final HistoricalQueryException CANCELLED = new HistoricalQueryException(
      Reason.CANCELLED, "Historical request cancelled");

  private static Program loopingProgram(ContractState state) {
    Program program = mock(Program.class);
    when(program.getContractState()).thenReturn(state);
    when(program.isStopped()).thenReturn(false, true);
    when(program.getCurrentOpIntValue()).thenReturn(Op.PUSH1);
    when(program.getCurrentOp()).thenReturn((byte) Op.PUSH1);
    return program;
  }

  private static JumpTable enabledTable() {
    Operation operation = mock(Operation.class);
    when(operation.isEnabled()).thenReturn(true);
    when(operation.getOpcode()).thenReturn(Op.PUSH1);
    when(operation.getRequire()).thenReturn(0);
    when(operation.getRet()).thenReturn(1);
    when(operation.getEnergyCost(org.mockito.ArgumentMatchers.any())).thenReturn(3L);
    JumpTable table = mock(JumpTable.class);
    when(table.get(anyInt())).thenReturn(operation);
    return table;
  }

  private static RuntimeException capturedFailure(Program program) {
    ArgumentCaptor<RuntimeException> failure = ArgumentCaptor.forClass(RuntimeException.class);
    verify(program).setRuntimeFailure(failure.capture());
    return failure.getValue();
  }

  @Test
  public void preOpcodeCheckCancellationHaltsBeforeAnyOpcodeExecutes() {
    Repository repository = mock(Repository.class);
    when(repository.isHistorical()).thenReturn(true);
    doThrow(CANCELLED).when(repository).checkHistoricalQueryActive();
    ProgramInvoke invoke = mock(ProgramInvoke.class);
    when(invoke.getContractAddress()).thenReturn(new DataWord(1));
    when(invoke.getDeposit()).thenReturn(repository);
    ContractState state = new ContractState(invoke);
    Program program = loopingProgram(state);
    JumpTable table = enabledTable();

    VM.play(program, table);

    RuntimeException failure = capturedFailure(program);
    assertTrue(failure.toString(), failure instanceof HistoricalQueryException);
    assertEquals(Reason.CANCELLED, ((HistoricalQueryException) failure).getReason());
    assertEquals("Historical request cancelled", failure.getMessage());
    verify(table, never()).get(anyInt());
  }

  @Test
  public void postOpcodeCheckCancellationStopsProgramAndKeepsTypedFailure() {
    ContractState state = mock(ContractState.class);
    when(state.isHistorical()).thenReturn(true);
    doNothing().doThrow(CANCELLED).when(state).checkHistoricalQueryActive();
    Program program = loopingProgram(state);

    VM.play(program, enabledTable());

    verify(program).spendAllEnergy();
    verify(program).stop();
    RuntimeException failure = capturedFailure(program);
    assertTrue(failure.toString(), failure instanceof HistoricalQueryException);
    assertEquals(Reason.CANCELLED, ((HistoricalQueryException) failure).getReason());
    assertEquals("Historical request cancelled", failure.getMessage());
  }
}
