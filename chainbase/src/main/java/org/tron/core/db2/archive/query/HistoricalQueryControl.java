package org.tron.core.db2.archive.query;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.contract.HistoricalQueryException;
import org.tron.core.db2.archive.contract.HistoricalQueryException.Reason;

/**
 * Cooperative cancellation shared by admission, state reads and every historical VM frame.
 *
 * <p>It also carries the sticky {@code TERMINAL} state of chapter 05 section 3.7 (P1/P2): the first
 * historical-class failure recorded through {@link #markTerminal} is rethrown by every later
 * {@link #checkActive()}, so a nested frame that swallowed the original exception cannot let the
 * request continue to a REVERT-shaped or success-shaped answer.
 */
public final class HistoricalQueryControl {

  private final Thread owner = Thread.currentThread();
  private final long deadlineNanos;
  private final AtomicReference<RuntimeException> terminal = new AtomicReference<>();
  private volatile boolean cancelled;

  public HistoricalQueryControl(long timeoutMillis) {
    if (timeoutMillis <= 0 || timeoutMillis > TimeUnit.DAYS.toMillis(1)) {
      throw new IllegalArgumentException("Historical request timeout must be in (0, 1 day]");
    }
    deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
  }

  /** May be called by another thread. Native snapshots must still be closed by the owner. */
  public void cancel() {
    cancelled = true;
  }

  public void checkActive() {
    if (Thread.currentThread() != owner) {
      throw new IllegalStateException("Historical request changed owner threads");
    }
    RuntimeException recorded = terminal.get();
    if (recorded != null) {
      throw recorded;
    }
    if (cancelled || owner.isInterrupted()) {
      throw markTerminal(
          new HistoricalQueryException(Reason.CANCELLED, "Historical request cancelled"));
    }
    if (deadlineNanos - System.nanoTime() <= 0) {
      throw markTerminal(new HistoricalQueryException(Reason.DEADLINE,
          "Historical request deadline exceeded"));
    }
  }

  /**
   * Records {@code failure} as the terminal cause unless one is already recorded (the first cause
   * wins) and returns {@code failure} so callers can write {@code throw control.markTerminal(e)}.
   */
  public RuntimeException markTerminal(RuntimeException failure) {
    java.util.Objects.requireNonNull(failure, "failure");
    terminal.compareAndSet(null, failure);
    return failure;
  }

  public boolean isTerminal() {
    return terminal.get() != null;
  }

  /** The first recorded terminal cause, if any. Safe to call from any thread. */
  public Optional<RuntimeException> terminalFailure() {
    return Optional.ofNullable(terminal.get());
  }

  public long remainingNanos() {
    checkActive();
    return StrictMathWrapper.max(1L, deadlineNanos - System.nanoTime());
  }

  public void requireOwner() {
    if (Thread.currentThread() != owner) {
      throw new IllegalStateException("Historical resources must be released by their owner");
    }
  }
}
