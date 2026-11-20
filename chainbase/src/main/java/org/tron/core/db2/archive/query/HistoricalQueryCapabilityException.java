package org.tron.core.db2.archive.query;

/**
 * A historical request asked for a state capability that historical execution does not provide
 * (chapter 05 section 3.6). It is a historical-class failure: the whole request fails closed with
 * category {@code CAPABILITY}; it is never a TVM result and never a data-access failure.
 */
public class HistoricalQueryCapabilityException extends RuntimeException {

  /** Generic unsupported capability. */
  public static final String UNSUPPORTED = "UNSUPPORTED";
  /** S(T) lacks a property that has no generated typed-getter default (section 3.10.1). */
  public static final String UNSUPPORTED_STATE = "UNSUPPORTED_STATE";
  /** An unexpected null-store style failure inside historical VM execution (section 3.7 P3). */
  public static final String UNEXPECTED_STATE = "UNEXPECTED_STATE";
  /** A storage-row key does not use the 32-byte format supported by develop. */
  public static final String UNSUPPORTED_STORAGE_KEY_FORMAT = "UNSUPPORTED_STORAGE_KEY_FORMAT";
  /** A process-wide latest-state entry point was reached inside a historical scope. */
  public static final String LATEST_STATE_ACCESS = "LATEST_STATE_ACCESS";

  private final String detail;

  public HistoricalQueryCapabilityException(String message) {
    this(UNSUPPORTED, message, null);
  }

  public HistoricalQueryCapabilityException(String detail, String message) {
    this(detail, message, null);
  }

  public HistoricalQueryCapabilityException(String detail, String message, Throwable cause) {
    super(message, cause);
    this.detail = java.util.Objects.requireNonNull(detail, "detail");
  }

  public String getDetail() {
    return detail;
  }
}
