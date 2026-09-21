package org.tron.core.db2.archive;

/** Stable internal failure reasons; transport-specific codes belong to the RPC layer. */
public class HistoricalQueryException extends ArchivePersistenceException {

  public enum Reason {
    UNAVAILABLE, NON_CANONICAL, DATA_ACCESS, DEADLINE, CANCELLED, OVERLOADED
  }

  private final Reason reason;

  public HistoricalQueryException(Reason reason, String message) {
    super(message);
    this.reason = java.util.Objects.requireNonNull(reason, "reason");
  }

  public HistoricalQueryException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = java.util.Objects.requireNonNull(reason, "reason");
  }

  public Reason getReason() {
    return reason;
  }
}
