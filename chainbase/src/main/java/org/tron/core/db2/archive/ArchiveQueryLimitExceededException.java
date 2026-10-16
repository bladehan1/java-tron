package org.tron.core.db2.archive;

/** Raised when a historical query cannot produce a complete result inside its resource budget. */
public class ArchiveQueryLimitExceededException extends RuntimeException {

  public ArchiveQueryLimitExceededException(String message) {
    super(message);
  }
}
