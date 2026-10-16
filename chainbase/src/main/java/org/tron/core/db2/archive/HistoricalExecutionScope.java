package org.tron.core.db2.archive;

import java.util.Objects;

/**
 * Thread-local marker for "this thread is executing a historical constant call" (chapter 05
 * section 3.11 point 4 and section 3.9.3).
 *
 * <p>Process-wide latest-state entry points ({@code AssetUtil}, {@code ForkController.instance()})
 * consult it and trap: inside the scope they throw a marked {@code CAPABILITY} failure instead of
 * silently reading latest state. Code that legitimately needs S(T)-scoped state without a
 * Repository at hand (for example the static {@code MUtil} fork gates) reads it through
 * {@link #current()}.
 *
 * <p>The scope is entered and closed by the same thread, around validate and execute of exactly one
 * historical execution; nesting is rejected.
 */
public final class HistoricalExecutionScope implements AutoCloseable {

  private static final ThreadLocal<HistoricalExecutionScope> CURRENT = new ThreadLocal<>();

  private final HistoricalQuerySession session;
  private final Thread owner = Thread.currentThread();
  private boolean closed;

  private HistoricalExecutionScope(HistoricalQuerySession session) {
    this.session = Objects.requireNonNull(session, "session");
  }

  public static HistoricalExecutionScope enter(HistoricalQuerySession session) {
    if (CURRENT.get() != null) {
      throw new IllegalStateException("Historical execution scopes cannot be nested");
    }
    HistoricalExecutionScope scope = new HistoricalExecutionScope(session);
    CURRENT.set(scope);
    return scope;
  }

  /** The active scope of the calling thread, or {@code null}. */
  public static HistoricalExecutionScope current() {
    return CURRENT.get();
  }

  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  /**
   * Builds the trap failure for a latest-state entry point reached inside the scope, marks the
   * owning session TERMINAL and returns the failure for the caller to throw.
   */
  public static RuntimeException trap(String entryPoint) {
    HistoricalQueryCapabilityException failure = new HistoricalQueryCapabilityException(
        HistoricalQueryCapabilityException.LATEST_STATE_ACCESS,
        entryPoint + " reads latest state and is not available to historical execution");
    HistoricalExecutionScope scope = CURRENT.get();
    return scope == null ? failure : scope.session.markTerminal(failure);
  }

  public HistoricalQuerySession session() {
    return session;
  }

  /** Fork decisions evaluated at S(T) for this request. */
  public org.tron.common.utils.ForkView forks() {
    return session.forks();
  }

  @Override
  public void close() {
    if (Thread.currentThread() != owner) {
      throw new IllegalStateException("Historical execution scope closed by a foreign thread");
    }
    if (!closed) {
      closed = true;
      if (CURRENT.get() == this) {
        CURRENT.remove();
      }
    }
  }
}
