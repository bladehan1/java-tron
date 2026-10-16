package org.tron.core.services.jsonrpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.HistoricalQueryBudgetException;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.vm.HistoricalCapabilityException;

public class HistoricalRpcExceptionTest {

  @Test
  public void historicalQueryReasonsProjectToStableCodesAndCategories() {
    assertProjection(Reason.UNAVAILABLE, -32602);
    assertProjection(Reason.NON_CANONICAL, -32602);
    assertProjection(Reason.DEADLINE, -32000);
    assertProjection(Reason.CANCELLED, -32000);
    assertProjection(Reason.OVERLOADED, -32000);
    assertProjection(Reason.DATA_ACCESS, -32000);
  }

  private static void assertProjection(Reason reason, int expectedCode) {
    HistoricalQueryException typed = new HistoricalQueryException(reason, "typed " + reason);
    HistoricalRpcException projected = HistoricalRpcException.from(typed);
    assertEquals(expectedCode, projected.getCode());
    assertEquals(reason.name(), projected.getCategory());
    assertEquals("typed " + reason, projected.getMessage());
    assertSame(typed, projected.getCause());
  }

  @Test
  public void typedExceptionsAreFoundTwoLevelsDeepInTheCauseChain() {
    HistoricalQueryException typed = new HistoricalQueryException(
        Reason.OVERLOADED, "Historical query capacity exhausted");
    RuntimeException wrapped = new RuntimeException("outer",
        new IllegalStateException("middle", typed));
    HistoricalRpcException projected = HistoricalRpcException.from(wrapped);
    assertEquals(-32000, projected.getCode());
    assertEquals("OVERLOADED", projected.getCategory());
    assertEquals("Historical query capacity exhausted", projected.getMessage());
    assertSame(wrapped, projected.getCause());
  }

  @Test
  public void budgetCapabilityAndPlainPersistenceFailuresProjectByType() {
    HistoricalQueryBudgetException budget =
        new HistoricalQueryBudgetException("Historical query read budget exceeded");
    HistoricalRpcException projectedBudget = HistoricalRpcException.from(budget);
    assertEquals(-32000, projectedBudget.getCode());
    assertEquals("BUDGET", projectedBudget.getCategory());
    assertEquals("Historical query read budget exceeded", projectedBudget.getMessage());
    assertSame(budget, projectedBudget.getCause());

    HistoricalCapabilityException capability =
        new HistoricalCapabilityException("SELFDESTRUCT is not supported");
    HistoricalRpcException projectedCapability = HistoricalRpcException.from(capability);
    assertEquals(-32000, projectedCapability.getCode());
    assertEquals("CAPABILITY", projectedCapability.getCategory());
    assertEquals("SELFDESTRUCT is not supported", projectedCapability.getMessage());
    assertSame(capability, projectedCapability.getCause());

    ArchivePersistenceException persistence = new ArchivePersistenceException("corrupt row");
    HistoricalRpcException projectedPersistence = HistoricalRpcException.from(persistence);
    assertEquals(-32000, projectedPersistence.getCode());
    assertEquals("DATA_ACCESS", projectedPersistence.getCategory());
    assertEquals("Historical query failed", projectedPersistence.getMessage());
    assertSame(persistence, projectedPersistence.getCause());
  }

  @Test
  public void unrelatedFailuresProjectAsExecution() {
    RuntimeException failure = new RuntimeException("boom");
    HistoricalRpcException projected = HistoricalRpcException.from(failure);
    assertEquals(-32000, projected.getCode());
    assertEquals("EXECUTION", projected.getCategory());
    assertEquals("Historical query failed", projected.getMessage());
    assertSame(failure, projected.getCause());
  }

  @Test
  public void errorDataIsTheFrozenEmptyObjectAndProjectionIsIdempotent() {
    HistoricalRpcException projected = HistoricalRpcException.from(
        new HistoricalQueryException(Reason.DEADLINE, "Historical request deadline exceeded"));
    assertEquals("{}", projected.getErrorData());
    assertTrue(HistoricalRpcException.from(projected) == projected);
  }
}
