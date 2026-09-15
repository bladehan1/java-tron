package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;

public class HistoricalQueryControlTest {

  @Test
  public void admissionReleasesCapacityAndStopCancelsActiveRequests() {
    HistoricalQueryAdmission admission = new HistoricalQueryAdmission();
    HistoricalQueryAdmission.Request first = admission.acquire(1, 10_000);
    HistoricalQueryException overloaded = assertThrows(HistoricalQueryException.class,
        () -> admission.acquire(1, 10_000));
    assertEquals(Reason.OVERLOADED, overloaded.getReason());

    first.close();
    HistoricalQueryAdmission.Request second = admission.acquire(1, 10_000);
    admission.stop();
    HistoricalQueryException cancelled = assertThrows(HistoricalQueryException.class,
        second.getControl()::checkActive);
    assertEquals(Reason.CANCELLED, cancelled.getReason());
    HistoricalQueryException stopped = assertThrows(HistoricalQueryException.class,
        () -> admission.acquire(1, 10_000));
    assertEquals(Reason.UNAVAILABLE, stopped.getReason());
    second.close();
  }

  @Test
  public void controlEnforcesDeadlineInterruptAndOwnerThread() throws Exception {
    HistoricalQueryControl deadline = new HistoricalQueryControl(1);
    HistoricalQueryException expired = awaitFailure(deadline, Reason.DEADLINE);
    assertEquals("Historical request deadline exceeded", expired.getMessage());

    HistoricalQueryControl interrupted = new HistoricalQueryControl(10_000);
    Thread.currentThread().interrupt();
    try {
      HistoricalQueryException cancelled = assertThrows(HistoricalQueryException.class,
          interrupted::checkActive);
      assertEquals(Reason.CANCELLED, cancelled.getReason());
    } finally {
      Thread.interrupted();
    }

    HistoricalQueryControl owned = new HistoricalQueryControl(10_000);
    AtomicReference<Throwable> foreignFailure = new AtomicReference<>();
    Thread foreign = new Thread(() -> {
      try {
        owned.checkActive();
      } catch (Throwable failure) {
        foreignFailure.set(failure);
      }
    });
    foreign.start();
    foreign.join(5_000);
    assertTrue(foreignFailure.get() instanceof IllegalStateException);
    owned.checkActive();
  }

  private static HistoricalQueryException awaitFailure(HistoricalQueryControl control,
      Reason reason) {
    long failAt = System.nanoTime() + 1_000_000_000L;
    while (System.nanoTime() < failAt) {
      try {
        control.checkActive();
      } catch (HistoricalQueryException failure) {
        assertEquals(reason, failure.getReason());
        return failure;
      }
      Thread.yield();
    }
    throw new AssertionError("Historical query deadline did not expire");
  }
}
