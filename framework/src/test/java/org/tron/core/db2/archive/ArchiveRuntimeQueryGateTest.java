package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedHistory;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.ArchiveRuntimeQueryGate.Lease;
import org.tron.core.db2.archive.ArchiveRuntimeQueryGate.State;

public class ArchiveRuntimeQueryGateTest {

  @Test
  public void quiesceRejectsNewPinsAndCloseRequiresEveryLease() throws Exception {
    PinnedSnapshot pinned = snapshot();
    AtomicInteger pinCalls = new AtomicInteger();
    ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(target -> {
      pinCalls.incrementAndGet();
      return pinned.snapshot;
    });

    Lease lease = gate.pin(0);

    assertSame(pinned.snapshot, lease.getSnapshot());
    assertEquals(1, gate.getActiveLeaseCount());
    assertFalse(gate.isDrained());
    gate.quiesce();
    assertEquals(State.QUIESCING, gate.getState());
    assertThrows(IllegalStateException.class, () -> gate.pin(0));
    assertEquals(1, pinCalls.get());
    assertThrows(IllegalStateException.class, gate::close);
    assertSame(pinned.snapshot, lease.getSnapshot());

    lease.close();
    lease.close();
    assertThrows(IllegalStateException.class, lease::getSnapshot);

    assertEquals(0, gate.getActiveLeaseCount());
    assertTrue(gate.isDrained());
    verify(pinned.history, times(1)).close();
    verify(pinned.latest, times(1)).close();
    verify(pinned.serving, times(1)).close();
    gate.close();
    gate.close();
    assertEquals(State.CLOSED, gate.getState());
  }

  @Test
  public void failedPinAndFailedSnapshotCloseDoNotLeakLeaseAccounting() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    PinnedSnapshot pinned = snapshot();
    doThrow(new IOException("injected close failure")).when(pinned.history).close();
    ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(target -> {
      if (calls.getAndIncrement() == 0) {
        throw new IOException("injected pin failure");
      }
      return pinned.snapshot;
    });

    assertThrows(IOException.class, () -> gate.pin(0));
    assertEquals(0, gate.getActiveLeaseCount());
    Lease lease = gate.pin(0);
    assertEquals(1, gate.getActiveLeaseCount());

    assertThrows(IOException.class, lease::close);

    assertTrue(gate.isDrained());
    verify(pinned.history).close();
    verify(pinned.latest).close();
    verify(pinned.serving).close();
    gate.close();
  }

  @Test
  public void inFlightPinCompletesBeforeConcurrentQuiesce() throws Exception {
    PinnedSnapshot pinned = snapshot();
    CountDownLatch pinEntered = new CountDownLatch(1);
    CountDownLatch allowPin = new CountDownLatch(1);
    ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(target -> {
      pinEntered.countDown();
      try {
        if (!allowPin.await(5, TimeUnit.SECONDS)) {
          throw new IOException("pin release timed out");
        }
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IOException("pin interrupted", failure);
      }
      return pinned.snapshot;
    });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Lease> pin = executor.submit(() -> gate.pin(0));
      assertTrue(pinEntered.await(5, TimeUnit.SECONDS));
      Future<?> quiesce = executor.submit(gate::quiesce);

      assertFalse(quiesce.isDone());
      allowPin.countDown();
      Lease lease = pin.get(5, TimeUnit.SECONDS);
      quiesce.get(5, TimeUnit.SECONDS);

      assertEquals(State.QUIESCING, gate.getState());
      assertEquals(1, gate.getActiveLeaseCount());
      assertThrows(IllegalStateException.class, () -> gate.pin(0));
      lease.close();
      assertTrue(gate.isDrained());
      gate.close();
    } finally {
      executor.shutdownNow();
    }
  }

  private static PinnedSnapshot snapshot() throws IOException {
    byte[] hash = new byte[32];
    ServingKeyIndex serving = mock(ServingKeyIndex.class);
    when(serving.getIndexedFrom()).thenReturn(0L);
    when(serving.getIndexedThrough()).thenReturn(0L);
    when(serving.getHeadHash()).thenReturn(hash);
    when(serving.getAuthoritativePrefixDigest()).thenReturn(new byte[0]);
    PinnedLatestState latest = mock(PinnedLatestState.class);
    when(latest.getBlockNumber()).thenReturn(0L);
    when(latest.getBlockHash()).thenReturn(hash);
    PinnedHistory history = mock(PinnedHistory.class);
    when(history.getIndexedFrom()).thenReturn(0L);
    when(history.getIndexedThrough()).thenReturn(0L);
    when(history.getHeadHash()).thenReturn(hash);
    when(history.getAuthoritativePrefixDigest()).thenReturn(new byte[0]);
    return new PinnedSnapshot(
        ArchiveReadSnapshot.pin(0, 0, hash, serving, latest, history), serving, latest, history);
  }

  private static final class PinnedSnapshot {
    private final ArchiveReadSnapshot snapshot;
    private final ServingKeyIndex serving;
    private final PinnedLatestState latest;
    private final PinnedHistory history;

    private PinnedSnapshot(ArchiveReadSnapshot snapshot, ServingKeyIndex serving,
        PinnedLatestState latest, PinnedHistory history) {
      this.snapshot = snapshot;
      this.serving = serving;
      this.latest = latest;
      this.history = history;
    }
  }
}
