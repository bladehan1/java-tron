package org.tron.core.db2.archive.query;

import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.contract.HistoricalQueryException;

/**
 * Stable external categories of historical query failures (chapter 05 section 3.15), with their
 * EIP-1474 style JSON-RPC codes and retryability. TVM results (REVERT, execution failure) and
 * contract-validation failures are not historical failures and keep the latest representation.
 */
public enum HistoricalFailureCategory {
  INVALID_PARAMS(-32602, false),
  HEADER_NOT_FOUND(-32602, false),
  NON_CANONICAL(-32602, false),
  OUT_OF_RANGE(-32001, false),
  NOT_YET_INDEXED(-32002, true),
  UNAVAILABLE(-32002, true),
  CANCELLED(-32002, true),
  OVERLOADED(-32005, true),
  DEADLINE(-32005, true),
  VM_TIMEOUT(-32005, true),
  BUDGET(-32005, false),
  CAPABILITY(-32004, false),
  CORRUPT(-32603, false),
  DATA_ACCESS(-32603, true),
  /** Not a historical-class failure: an execution error outside the categories above. */
  EXECUTION(-32000, false);

  private final int code;
  private final boolean retryable;

  HistoricalFailureCategory(int code, boolean retryable) {
    this.code = code;
    this.retryable = retryable;
  }

  public int getCode() {
    return code;
  }

  public boolean isRetryable() {
    return retryable;
  }

  /**
   * Classifies {@code failure} by the first historical-class exception in its cause chain.
   * Budget is checked before the generic persistence type it extends.
   */
  public static HistoricalFailureCategory of(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof HistoricalQueryBudgetException) {
        return BUDGET;
      }
      if (cause instanceof HistoricalQueryCapabilityException) {
        return CAPABILITY;
      }
      if (cause instanceof HistoricalQueryException) {
        return of(((HistoricalQueryException) cause).getReason());
      }
      if (cause instanceof ArchivePersistenceException) {
        return DATA_ACCESS;
      }
    }
    return EXECUTION;
  }

  public static HistoricalFailureCategory of(HistoricalQueryException.Reason reason) {
    switch (reason) {
      case UNAVAILABLE:
        return UNAVAILABLE;
      case NON_CANONICAL:
        return NON_CANONICAL;
      case DATA_ACCESS:
        return DATA_ACCESS;
      case DEADLINE:
        return DEADLINE;
      case CANCELLED:
        return CANCELLED;
      case OVERLOADED:
        return OVERLOADED;
      case CORRUPT:
        return CORRUPT;
      default:
        return EXECUTION;
    }
  }

  /** Short machine-readable detail, when the failure carries one. */
  public static String detail(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof HistoricalQueryCapabilityException) {
        return ((HistoricalQueryCapabilityException) cause).getDetail();
      }
    }
    return null;
  }
}
