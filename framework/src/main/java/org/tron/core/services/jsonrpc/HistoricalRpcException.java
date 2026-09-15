package org.tron.core.services.jsonrpc;

import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.HistoricalQueryBudgetException;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.vm.HistoricalCapabilityException;

/** Historical-only error projection. Numeric codes retain the current prototype contract. */
final class HistoricalRpcException extends RuntimeException {

  private final int code;
  private final String category;

  private HistoricalRpcException(int code, String category, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.category = category;
  }

  static HistoricalRpcException from(Throwable failure) {
    if (failure instanceof HistoricalRpcException) {
      return (HistoricalRpcException) failure;
    }
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof HistoricalQueryException) {
        HistoricalQueryException typed = (HistoricalQueryException) cause;
        String category = typed.getReason().name();
        int code = typed.getReason() == HistoricalQueryException.Reason.UNAVAILABLE
            || typed.getReason() == HistoricalQueryException.Reason.NON_CANONICAL ? -32602 : -32000;
        return new HistoricalRpcException(code, category, typed.getMessage(), failure);
      }
      if (cause instanceof HistoricalQueryBudgetException) {
        return new HistoricalRpcException(-32000, "BUDGET", cause.getMessage(), failure);
      }
      if (cause instanceof HistoricalCapabilityException) {
        return new HistoricalRpcException(-32000, "CAPABILITY", cause.getMessage(), failure);
      }
    }
    return new HistoricalRpcException(-32000,
        failure instanceof ArchivePersistenceException ? "DATA_ACCESS" : "EXECUTION",
        "Historical query failed", failure);
  }

  int getCode() {
    return code;
  }

  String getCategory() {
    return category;
  }

  String getErrorData() {
    // Wire schema remains the existing prototype until the requirement authority freezes it.
    return "{}";
  }
}
