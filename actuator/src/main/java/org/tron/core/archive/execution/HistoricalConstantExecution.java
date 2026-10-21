package org.tron.core.archive.execution;

import java.util.Objects;
import org.tron.core.actuator.VMActuator;
import org.tron.core.db.TransactionContext;
import org.tron.core.db.TransactionContext.ExecutionMode;
import org.tron.core.db2.archive.HistoricalQuerySession;
import org.tron.core.db2.archive.query.HistoricalExecutionScope;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.vm.OperationRegistry;
import org.tron.core.vm.config.VMConfig;

/**
 * Runs one historical constant call (chapter 05 sections 3.7, 3.9 and 3.11):
 * <ul>
 *   <li>binds the session to exactly one execution;</li>
 *   <li>enters the thread-local {@link HistoricalExecutionScope} around validate and execute, so
 *   latest-state entry points trap and static fork gates read {@code ForkView@S(T)};</li>
 *   <li>P5 final gate: after execution, a TERMINAL session fails the request with its recorded
 *   cause, whatever the root frame returned;</li>
 *   <li>P6: a validate/execute failure surfacing after the session became TERMINAL is replaced by
 *   the recorded cause;</li>
 *   <li>clears the thread-local VM config snapshot and request operation table in one finally.</li>
 * </ul>
 */
public final class HistoricalConstantExecution {

  private HistoricalConstantExecution() {
  }

  public static void run(VMActuator actuator, TransactionContext context)
      throws ContractValidateException, ContractExeException {
    Objects.requireNonNull(actuator, "actuator");
    Objects.requireNonNull(context, "context");
    if (context.getExecutionMode() != ExecutionMode.HISTORICAL_CONSTANT) {
      throw new IllegalArgumentException("Historical execution requires HISTORICAL_CONSTANT");
    }
    HistoricalQuerySession session = context.getHistoricalQuerySession();
    session.bindExecution();
    try (HistoricalExecutionScope ignored = HistoricalExecutionScope.enter(session)) {
      session.checkActive();
      try {
        actuator.validate(context);
        actuator.execute(context);
      } catch (ContractValidateException | ContractExeException | RuntimeException failure) {
        RuntimeException terminal = session.terminalFailure().orElse(null);
        if (terminal != null && terminal != failure) {
          terminal.addSuppressed(failure);
          throw terminal;
        }
        throw failure;
      }
      session.checkActive();
    } finally {
      VMConfig.clearLocalSnapshot();
      OperationRegistry.clearRequestTable();
    }
  }
}
