package org.tron.core.db2.archive.serving;

/**
 * Serving states (ch04 §3.4.1). OPENING and CLOSED are lifecycle end points, not service states;
 * only LIVE admits queries.
 */
public enum ServingState {
  OPENING,
  BULK_CATCH_UP,
  LIVE,
  DEGRADED,
  REBUILDING,
  CLOSED
}
