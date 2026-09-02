package org.tron.core.vm;

/** Fail-closed rejection for a state capability unavailable to historical execution. */
public class HistoricalCapabilityException extends RuntimeException {

  public HistoricalCapabilityException(String message) {
    super(message);
  }
}
