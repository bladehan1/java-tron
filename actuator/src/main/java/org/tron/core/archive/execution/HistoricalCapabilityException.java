package org.tron.core.archive.execution;

import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;

/** Fail-closed rejection for a state capability unavailable to historical execution. */
public class HistoricalCapabilityException extends HistoricalQueryCapabilityException {

  public HistoricalCapabilityException(String message) {
    super(message);
  }

  public HistoricalCapabilityException(String detail, String message) {
    super(detail, message);
  }

  public HistoricalCapabilityException(String detail, String message, Throwable cause) {
    super(detail, message, cause);
  }
}
