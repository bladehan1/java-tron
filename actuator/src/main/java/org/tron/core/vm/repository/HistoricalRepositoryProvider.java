package org.tron.core.vm.repository;

import org.tron.core.db.TransactionContext;
import org.tron.core.db.TransactionContext.ExecutionMode;

/** Provider for a request-owned, exact historical Repository root. */
public enum HistoricalRepositoryProvider implements RepositoryProvider {
  INSTANCE;

  @Override
  public Repository createRoot(TransactionContext context) {
    if (context.getExecutionMode() != ExecutionMode.HISTORICAL_CONSTANT
        || context.getHistoricalQuerySession() == null) {
      throw new IllegalArgumentException(
          "Historical Repository requires a HISTORICAL_CONSTANT context");
    }
    return RepositoryImpl.createHistoricalRoot(context.getStoreFactory(),
        context.getHistoricalQuerySession());
  }
}
