package org.tron.core.db2.archive;

/** Fatal archive persistence or continuity failure. */
public class ArchivePersistenceException extends RuntimeException {

  public ArchivePersistenceException(String message, Throwable cause) {
    super(message, cause);
  }

  public ArchivePersistenceException(String message) {
    super(message);
  }
}
