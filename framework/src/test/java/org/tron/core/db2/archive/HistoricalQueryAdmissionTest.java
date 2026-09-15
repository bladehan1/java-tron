package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.tron.core.db2.archive.HistoricalQueryAdmission.Request;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;

public class HistoricalQueryAdmissionTest {

  @Test(timeout = 30000)
  public void concurrentCompetitionAdmitsExactlyCapacityAndReleasesEverySlot()
      throws Exception {
    HistoricalQueryAdmission admission = new HistoricalQueryAdmission();
    int capacity = 3;
    int competitors = 8;
    CountDownLatch start = new CountDownLatch(1);
    List<Request> acquired = new CopyOnWriteArrayList<>();
    List<HistoricalQueryException> rejected = new CopyOnWriteArrayList<>();
    Thread[] threads = new Thread[competitors];
    for (int index = 0; index < competitors; index++) {
      threads[index] = new Thread(() -> {
        try {
          start.await();
          acquired.add(admission.acquire(capacity, 10_000));
        } catch (HistoricalQueryException failure) {
          rejected.add(failure);
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
        }
      });
      threads[index].start();
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join(10_000);
    }
    assertEquals(capacity, acquired.size());
    assertEquals(competitors - capacity, rejected.size());
    rejected.forEach(failure -> assertEquals(Reason.OVERLOADED, failure.getReason()));

    acquired.forEach(Request::close);
    for (int index = 0; index < capacity; index++) {
      admission.acquire(capacity, 10_000).close();
    }
  }

  @Test
  public void doubleCloseIsSafeAndFreesTheSlotOnce() {
    HistoricalQueryAdmission admission = new HistoricalQueryAdmission();
    Request request = admission.acquire(1, 10_000);
    request.close();
    request.close();
    admission.acquire(1, 10_000).close();
  }

  @Test
  public void stopCancelsEveryActiveControlAndRejectsNewAcquires() {
    HistoricalQueryAdmission admission = new HistoricalQueryAdmission();
    Request first = admission.acquire(2, 10_000);
    Request second = admission.acquire(2, 10_000);

    admission.stop();

    for (Request request : new Request[]{first, second}) {
      HistoricalQueryException cancelled = assertThrows(HistoricalQueryException.class,
          request.getControl()::checkActive);
      assertEquals(Reason.CANCELLED, cancelled.getReason());
    }
    HistoricalQueryException stopped = assertThrows(HistoricalQueryException.class,
        () -> admission.acquire(2, 10_000));
    assertEquals(Reason.UNAVAILABLE, stopped.getReason());
    first.close();
    second.close();
  }

  @Test
  public void controlRejectsNonPositiveAndOverOneDayTimeouts() {
    for (long timeout : new long[]{0L, -1L, TimeUnit.DAYS.toMillis(1) + 1}) {
      assertThrows(IllegalArgumentException.class, () -> new HistoricalQueryControl(timeout));
    }
    HistoricalQueryControl boundary = new HistoricalQueryControl(TimeUnit.DAYS.toMillis(1));
    boundary.checkActive();
  }

  @Test
  public void remainingNanosIsPositiveWhileActive() {
    HistoricalQueryControl control = new HistoricalQueryControl(10_000);
    assertTrue(control.remainingNanos() > 0);
    assertTrue(control.remainingNanos() <= TimeUnit.MILLISECONDS.toNanos(10_000));
  }
}
