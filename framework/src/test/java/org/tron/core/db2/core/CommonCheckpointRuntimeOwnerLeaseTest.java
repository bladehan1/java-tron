package org.tron.core.db2.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.core.db2.archive.HistoricalQueryControl;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointRuntimeOwner.ReadLease;

public class CommonCheckpointRuntimeOwnerLeaseTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static CommonCheckpointRuntimeOwner readyOwner(Path directory) throws IOException {
    CommonCheckpointRuntimeOwner owner = new CommonCheckpointRuntimeOwner(
        new CommonCheckpointRedoCoordinator(new CommonCheckpointFile(directory),
            materializer(Authority.CHAINBASE), materializer(Authority.PATH_STATE),
            materializer(Authority.STATE_ARCHIVE)));
    owner.recoverBeforeServing();
    return owner;
  }

  private static CommonCheckpointMaterializer materializer(Authority authority) {
    CommonCheckpointMaterializer materializer = mock(CommonCheckpointMaterializer.class);
    when(materializer.authority()).thenReturn(authority);
    return materializer;
  }

  private static ReentrantReadWriteLock gate(CommonCheckpointRuntimeOwner owner) {
    return (ReentrantReadWriteLock) ReflectionTestUtils.getField(owner, "gate");
  }

  /** Holds the publication write gate from another thread until released. */
  private static final class WriteGateBlocker implements AutoCloseable {

    private final CountDownLatch held = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final Thread thread;

    private WriteGateBlocker(ReentrantReadWriteLock gate) {
      thread = new Thread(() -> {
        gate.writeLock().lock();
        held.countDown();
        try {
          release.await();
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
        } finally {
          gate.writeLock().unlock();
        }
      });
      thread.start();
    }

    private void awaitHeld() throws InterruptedException {
      assertTrue(held.await(5, TimeUnit.SECONDS));
    }

    @Override
    public void close() throws InterruptedException {
      release.countDown();
      thread.join(5_000);
    }
  }

  @Test(timeout = 30000)
  public void deadlineExpiresWhilePublicationGateIsHeld() throws Exception {
    CommonCheckpointRuntimeOwner owner =
        readyOwner(temporaryFolder.newFolder("deadline").toPath());
    try (WriteGateBlocker blocker = new WriteGateBlocker(gate(owner))) {
      blocker.awaitHeld();
      HistoricalQueryControl control = new HistoricalQueryControl(200);
      HistoricalQueryException failure = assertThrows(HistoricalQueryException.class,
          () -> owner.acquireReadLease(control));
      assertEquals(Reason.DEADLINE, failure.getReason());
    } finally {
      owner.close();
    }
  }

  @Test(timeout = 30000)
  public void preCancelledControlFailsWithoutBlockingOnTheGate() throws Exception {
    CommonCheckpointRuntimeOwner owner =
        readyOwner(temporaryFolder.newFolder("cancelled").toPath());
    try (WriteGateBlocker blocker = new WriteGateBlocker(gate(owner))) {
      blocker.awaitHeld();
      HistoricalQueryControl control = new HistoricalQueryControl(10_000);
      control.cancel();
      HistoricalQueryException failure = assertThrows(HistoricalQueryException.class,
          () -> owner.acquireReadLease(control));
      assertEquals(Reason.CANCELLED, failure.getReason());
    } finally {
      owner.close();
    }
  }

  @Test(timeout = 30000)
  public void interruptWhileWaitingYieldsCancelledAndRestoresTheFlag() throws Exception {
    CommonCheckpointRuntimeOwner owner =
        readyOwner(temporaryFolder.newFolder("interrupt").toPath());
    try (WriteGateBlocker blocker = new WriteGateBlocker(gate(owner))) {
      blocker.awaitHeld();
      Thread waiting = Thread.currentThread();
      Thread interrupter = new Thread(() -> {
        long failAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiting.getState() != Thread.State.TIMED_WAITING
            && System.nanoTime() < failAt) {
          Thread.yield();
        }
        waiting.interrupt();
      });
      interrupter.start();
      try {
        HistoricalQueryControl control = new HistoricalQueryControl(10_000);
        HistoricalQueryException failure = assertThrows(HistoricalQueryException.class,
            () -> owner.acquireReadLease(control));
        assertEquals(Reason.CANCELLED, failure.getReason());
        assertTrue(failure.getCause() instanceof InterruptedException);
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        interrupter.join(5_000);
        Thread.interrupted();
      }
    } finally {
      owner.close();
    }
  }

  @Test(timeout = 30000)
  public void healthyControlAcquiresOnceTheGateIsFreeAndLeaseCloseIsOwnerOnly()
      throws Exception {
    CommonCheckpointRuntimeOwner owner =
        readyOwner(temporaryFolder.newFolder("healthy").toPath());
    ReentrantReadWriteLock gate = gate(owner);
    try (WriteGateBlocker blocker = new WriteGateBlocker(gate)) {
      blocker.awaitHeld();
    }
    HistoricalQueryControl control = new HistoricalQueryControl(10_000);
    ReadLease lease = owner.acquireReadLease(control);
    assertTrue(gate.getReadLockCount() > 0);

    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread foreign = new Thread(() -> {
      try {
        lease.close();
      } catch (Throwable throwable) {
        failure.set(throwable);
      }
    });
    foreign.start();
    foreign.join(5_000);
    assertTrue(failure.get() instanceof IllegalStateException);

    lease.close();
    assertEquals(0, gate.getReadLockCount());
    lease.close();
    owner.close();
  }
}
