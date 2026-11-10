package org.tron.core.db2.archive.serving.status;

import java.util.Objects;

/**
 * A classified serving failure (ch04 §5.1). Serving failures never leave the serving component:
 * they only move it to DEGRADED and are reported through
 * {@link org.tron.core.db2.archive.serving.ServingView} and metrics.
 */
public final class ServingFailure {

  /** ch04 §5.1 categories. */
  public enum Reason {
    TRANSIENT_IO(Kind.TRANSIENT),
    SOURCE_UNAVAILABLE(Kind.TRANSIENT),
    SOURCE_CORRUPT(Kind.PERMANENT),
    SOURCE_INCONSISTENT(Kind.PERMANENT),
    INDEX_CORRUPT(Kind.PERMANENT),
    /** ch09 OPS-D6: the disk budget is at RED or above; build and rebuild pause (§5.1). */
    DISK_PRESSURE(Kind.TRANSIENT),
    INDEX_INCOMPATIBLE(Kind.PERMANENT),
    INDEX_AHEAD(Kind.PERMANENT),
    IDENTITY(Kind.PERMANENT),
    PROGRAM_ERROR(Kind.PERMANENT),
    JVM_ERROR(Kind.PERMANENT);

    private final Kind kind;

    Reason(Kind kind) {
      this.kind = kind;
    }

    public Kind kind() {
      return kind;
    }

    /** Index-side permanent faults that {@code autoRebuild} may repair (ch04 §3.8 P3). */
    public boolean isRebuildable() {
      return this == INDEX_CORRUPT || this == INDEX_AHEAD || this == IDENTITY;
    }
  }

  public enum Kind {
    TRANSIENT, PERMANENT
  }

  private final Reason reason;
  private final String message;
  private final long timeMillis;

  public ServingFailure(Reason reason, String message, long timeMillis) {
    this.reason = Objects.requireNonNull(reason, "reason");
    this.message = message == null ? "" : message;
    this.timeMillis = timeMillis;
  }

  public Reason getReason() {
    return reason;
  }

  public Kind getKind() {
    return reason.kind();
  }

  public String getMessage() {
    return message;
  }

  public long getTimeMillis() {
    return timeMillis;
  }

  @Override
  public String toString() {
    return reason + "(" + reason.kind() + "): " + message;
  }

  /** A failure raised inside serving with an explicit ch04 §5.1 category. */
  public static final class ServingFaultException extends java.io.IOException {

    private final Reason reason;

    public ServingFaultException(Reason reason, String message) {
      super(message);
      this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ServingFaultException(Reason reason, String message, Throwable cause) {
      super(message, cause);
      this.reason = Objects.requireNonNull(reason, "reason");
    }

    public Reason getReason() {
      return reason;
    }
  }
}
