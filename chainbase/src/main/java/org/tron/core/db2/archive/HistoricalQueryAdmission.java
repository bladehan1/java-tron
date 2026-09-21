package org.tron.core.db2.archive;

import java.util.HashSet;
import java.util.Set;

/** Fail-fast admission; cancellation never closes another thread's native snapshot. */
public final class HistoricalQueryAdmission {

  private final Set<Request> active = new HashSet<>();
  private boolean stopped;

  public synchronized Request acquire(int capacity, long timeoutMillis) {
    if (stopped) {
      throw new HistoricalQueryException(HistoricalQueryException.Reason.UNAVAILABLE,
          "Historical query admission stopped");
    }
    if (capacity <= 0 || active.size() >= capacity) {
      throw new HistoricalQueryException(HistoricalQueryException.Reason.OVERLOADED,
          "Historical query capacity exhausted");
    }
    Request request = new Request(new HistoricalQueryControl(timeoutMillis));
    active.add(request);
    return request;
  }

  public synchronized void stop() {
    stopped = true;
    active.forEach(request -> request.control.cancel());
  }

  public final class Request implements AutoCloseable {

    private final HistoricalQueryControl control;

    private Request(HistoricalQueryControl control) {
      this.control = control;
    }

    public HistoricalQueryControl getControl() {
      return control;
    }

    @Override
    public void close() {
      synchronized (HistoricalQueryAdmission.this) {
        active.remove(this);
      }
    }
  }
}
