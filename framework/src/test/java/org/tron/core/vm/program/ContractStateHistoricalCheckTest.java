package org.tron.core.vm.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.vm.program.invoke.ProgramInvoke;
import org.tron.core.vm.repository.Repository;

public class ContractStateHistoricalCheckTest {

  private static ContractState stateWith(Repository repository) {
    ProgramInvoke invoke = mock(ProgramInvoke.class);
    when(invoke.getContractAddress()).thenReturn(new DataWord(1));
    when(invoke.getDeposit()).thenReturn(repository);
    return new ContractState(invoke);
  }

  @Test
  public void checkHistoricalQueryActiveDelegatesToRepository() {
    Repository repository = mock(Repository.class);
    ContractState state = stateWith(repository);
    HistoricalQueryException cancelled = new HistoricalQueryException(
        Reason.CANCELLED, "Historical request cancelled");
    doThrow(cancelled).when(repository).checkHistoricalQueryActive();

    HistoricalQueryException failure = assertThrows(HistoricalQueryException.class,
        state::checkHistoricalQueryActive);
    assertEquals(Reason.CANCELLED, failure.getReason());
    assertEquals("Historical request cancelled", failure.getMessage());
  }

  @Test
  public void isHistoricalDelegatesToRepository() {
    Repository repository = mock(Repository.class);
    when(repository.isHistorical()).thenReturn(true);
    ContractState state = stateWith(repository);
    assertTrue(state.isHistorical());
    when(repository.isHistorical()).thenReturn(false);
    assertFalse(state.isHistorical());
  }
}
