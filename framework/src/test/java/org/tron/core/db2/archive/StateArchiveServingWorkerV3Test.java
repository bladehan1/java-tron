package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.StateArchiveServingIndexBuildCoordinatorV3.Mode;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveServingWorkerV3Test {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 15000)
  public void blockedBuilderAllowsCoalescedOffersAndBackgroundRangeAfterHandoff()
      throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch liveEntered = new CountDownLatch(1);
    CountDownLatch liveRelease = new CountDownLatch(1);
    java.util.concurrent.atomic.AtomicBoolean firstBuild =
        new java.util.concurrent.atomic.AtomicBoolean(true);
    java.util.concurrent.atomic.AtomicBoolean blockLiveBuild =
        new java.util.concurrent.atomic.AtomicBoolean();
    java.util.concurrent.atomic.AtomicReference<Thread> buildThread =
        new java.util.concurrent.atomic.AtomicReference<>();
    Fixture fixture = new Fixture(root, 1);
    try (StateArchiveFiveLaneWriterV5 writer = fixture.writer) {
      fixture.append(1);
      fixture.publishThrough(1);
      StateArchiveServingWorkerV3 worker = worker(root, fixture.source, () -> {
        buildThread.set(Thread.currentThread());
        boolean first = firstBuild.getAndSet(false);
        if (!first && !blockLiveBuild.get()) {
          return;
        }
        CountDownLatch currentEntered = first ? entered : liveEntered;
        CountDownLatch currentRelease = first ? release : liveRelease;
        currentEntered.countDown();
        try {
          if (!currentRelease.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("test release timed out");
          }
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(failure);
        }
      });
      try {
        worker.offer(target(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        fixture.append(2);
        fixture.publishThrough(2);
        worker.offer(target(2, 2));
        assertEquals(2, worker.status().getCommittedThrough());
        assertEquals(-1, worker.status().getIndexedThrough());
        release.countDown();
        worker.completeInitialSync(target(2, 2));
        assertEquals(2, worker.status().getIndexedThrough());
        assertEquals(Mode.LIVE_BACKGROUND, worker.status().getMode());
        blockLiveBuild.set(true);
        long sequence = worker.status().getBuildSequence();
        for (int block = 3; block <= 12; block++) {
          fixture.append(block);
        }
        fixture.publishThrough(12);
        worker.offer(target(3, 12));
        assertTrue(liveEntered.await(5, TimeUnit.SECONDS));
        assertTrue(buildThread.get() != Thread.currentThread());
        assertEquals(2, worker.status().getIndexedThrough());
        liveRelease.countDown();
        awaitIndexed(worker, 12);
        assertEquals(sequence + 1, worker.status().getBuildSequence());
        worker.offer(target(3, 12));
        assertEquals(sequence + 1, worker.status().getBuildSequence());
        assertThrows(IOException.class, () -> worker.offer(target(14, 14)));
      } finally {
        release.countDown();
        liveRelease.countDown();
        worker.close();
      }
    }
  }

  @Test(timeout = 15000)
  public void failedOwnerIsObservableAndRestartReplaysLostNotification() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Fixture fixture = new Fixture(root, 1);
    try (StateArchiveFiveLaneWriterV5 writer = fixture.writer) {
      fixture.append(1);
      fixture.publishThrough(1);
      try (StateArchiveServingWorkerV3 failed = worker(root, fixture.source, () -> {
        throw new IllegalStateException("injected builder failure");
      })) {
        failed.offer(target(1, 1));
        assertThrows(IOException.class, () -> failed.completeInitialSync(target(1, 1)));
        assertEquals(Mode.CATCH_UP_REQUIRED, failed.status().getMode());
        assertNotNull(failed.failure());
        assertEquals(-1, failed.status().getIndexedThrough());
      }
      // No volatile mailbox survives. The recovered Common target is sufficient.
      try (StateArchiveServingWorkerV3 recovered = worker(root, fixture.source, () -> { })) {
        recovered.offer(target(1, 1));
        recovered.completeInitialSync(target(1, 1));
        assertEquals(1, recovered.status().getIndexedThrough());
      }
      try (StateArchiveServingWorkerV3 again = worker(root, fixture.source, () -> { })) {
        again.offer(target(1, 1));
        again.completeInitialSync(target(1, 1));
        assertEquals(0, again.status().getBuildSequence());
      }
    }
  }

  @Test(timeout = 15000)
  public void indexOpenFailureDoesNotThrowOnConstruction() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Fixture fixture = new Fixture(root, 1);
    try (StateArchiveFiveLaneWriterV5 writer = fixture.writer;
        StateArchiveServingWorkerV3 worker = new StateArchiveServingWorkerV3(() -> {
          throw new IOException("injected index open failure");
        }, fixture.source, () -> { })) {
      assertThrows(IOException.class, () -> {
        worker.offer(target(1, 1));
        worker.completeInitialSync(target(1, 1));
      });
      assertEquals(Mode.CATCH_UP_REQUIRED, worker.status().getMode());
    }
  }

  @Test
  public void readBudgetRejectsBeforeAllocatingAnOversizedFrame() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Fixture fixture = new Fixture(root, 1);
    try (StateArchiveFiveLaneWriterV5 writer = fixture.writer) {
      fixture.append(1);
      fixture.publishThrough(1);
      assertThrows(StateArchiveServingSource.ReadBudgetException.class,
          () -> fixture.source.readCommittedDiffs(0, 1, 1));
      assertEquals(1, fixture.source.readCommittedDiffs(0, 1, Long.MAX_VALUE).size());
    }
  }

  @Test(timeout = 15000)
  public void workerStartsFromAvailableHistoryInsteadOfGenesis() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Fixture fixture = new Fixture(root, 101);
    try (StateArchiveFiveLaneWriterV5 writer = fixture.writer) {
      fixture.append(101);
      fixture.append(102);
      fixture.publishThrough(102);
      try (StateArchiveServingWorkerV3 worker = worker(root, fixture.source, () -> { })) {
        worker.offer(target(101, 102));
        worker.completeInitialSync(target(101, 102));
        assertEquals(100, worker.status().getIndexedFrom());
        assertEquals(102, worker.status().getIndexedThrough());
      }
      try (StateArchiveServingWorkerV3 worker = worker(root, fixture.source, () -> { })) {
        worker.offer(target(101, 102));
        worker.completeInitialSync(target(101, 102));
        assertEquals(100, worker.status().getIndexedFrom());
        assertEquals(0, worker.status().getBuildSequence());
      }
    }
  }

  private StateArchiveServingWorkerV3 worker(Path root,
      StateArchiveServingSourceV5 source, Runnable hook) {
    return new StateArchiveServingWorkerV3(
        () -> new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.ROCKSDB, 1000),
        source, hook, 10);
  }

  private void awaitIndexed(StateArchiveServingWorkerV3 worker, long block)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (worker.status().getIndexedThrough() != block && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(block, worker.status().getIndexedThrough());
  }

  private static BlockReverseDiff diff(int block) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L), Collections.emptyList());
  }

  private static CommonCheckpointTarget target(int first, int last) {
    return CommonCheckpointTarget.restore(hash(70), hash(last + 80), diff(first).getMeta(),
        diff(last).getMeta(), hash(first + 30), hash(last + 31));
  }

  private static byte[] hash(int value) {
    byte[] hash = new byte[32];
    hash[31] = (byte) value;
    return hash;
  }

  private static final class Fixture {

    private final StateArchiveFiveLaneWriterV5 writer;
    private final StateArchiveServingSourceV5 source;
    private final Map<Long, BlockSnapshotMeta> metas = new HashMap<>();
    private int publishedThrough;

    private Fixture(Path root, int firstBlock) throws IOException {
      writer = new StateArchiveFiveLaneWriterV5(root, firstBlock, hash(90), 1500);
      source = new StateArchiveServingSourceV5(root, firstBlock, hash(90), metas::get);
      publishedThrough = firstBlock - 1;
    }

    private void append(int block) throws IOException {
      BlockReverseDiff diff = diff(block);
      metas.put((long) block, diff.getMeta());
      writer.append(diff);
    }

    private void publishThrough(int last) throws IOException {
      CommonCheckpointTarget target = target(publishedThrough + 1, last);
      source.updatePublished(writer.forceTailReady(target), target);
      publishedThrough = last;
    }
  }
}
