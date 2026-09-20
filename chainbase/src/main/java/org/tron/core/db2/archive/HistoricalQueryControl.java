package org.tron.core.db2.archive;

import java.util.concurrent.TimeUnit;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;

/** Cooperative cancellation shared by admission, state reads and every historical VM frame. */
public final class HistoricalQueryControl {

  private final Thread owner = Thread.currentThread();
  private final long deadlineNanos;
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
    if (cancelled || owner.isInterrupted()) {
      throw new HistoricalQueryException(Reason.CANCELLED, "Historical request cancelled");
    }
    if (deadlineNanos - System.nanoTime() <= 0) {
      throw new HistoricalQueryException(Reason.DEADLINE, "Historical request deadline exceeded");
    }
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
