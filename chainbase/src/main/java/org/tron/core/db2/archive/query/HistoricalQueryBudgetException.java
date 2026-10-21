package org.tron.core.db2.archive.query;

import org.tron.core.db2.archive.ArchivePersistenceException;

/** A request-owned historical view exceeded its configured resource budget. */
public class HistoricalQueryBudgetException extends ArchivePersistenceException {

  public HistoricalQueryBudgetException(String message) {
    super(message);
  }
}
