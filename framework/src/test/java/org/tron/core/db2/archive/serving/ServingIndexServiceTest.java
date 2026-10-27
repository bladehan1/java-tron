package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.contract.HistoricalQueryException;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Source;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Writer;
import org.tron.core.db2.archive.serving.ServingFailure.Reason;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.StateArchiveIndexEngineManifest;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * ServingIndexService state machine and isolation (ch04 §3.4–§3.10; T1, T3, T4, T5 service
 * part, T7/T8 component part, T9, T11, T12, T14, T18, T19, T20, T21). Every fixture writes a
 * real History V5 root and a RocksDB serving index; Common is represented by the History
 * writer and the published-boundary source, which serving never writes to (D5).
 */
public class ServingIndexServiceTest {

  private static final Duration WAIT = Duration.ofSeconds(20);

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<AutoCloseable> closeables = new ArrayList<>();

  @After
  public void closeAll() throws Exception {
    for (int index = closeables.size() - 1; index >= 0; index--) {
      closeables.get(index).close();
    }
  }

  /** T3 (R2-05): several Common targets published during one build merge into one range. */
  @Test(timeout = 60_000)
  public void coalescedWatermarksBuildOneContiguousRange() throws Exception {
    History history = history(1);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean first = new AtomicBoolean(true);
    ServingIndexService service = service(history, config(), new ServingIndexService.Hooks() {
      @Override
      public void beforeBatch(long from, long last, boolean solo) {
        if (first.getAndSet(false)) {
          entered.countDown();
          await(release);
        }
      }
    });
    assertTrue(service.awaitLive(WAIT));
    history.publish(service, 1, 2);
    assertTrue(entered.await(10, TimeUnit.SECONDS));
    long started = System.nanoTime();
    history.publish(service, 3, 4);
    history.publish(service, 5, 6);
    assertTrue("onCommonPublished must not wait for a build",
        System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
    release.countDown();
    assertTrue(awaitThrough(service, 6));
    assertEquals(ServingState.LIVE, service.status().getState());
    assertNull(service.status().getLastFailure());
    assertEquals(2, service.batches(0));
    history.assertOracle(service);
  }

  /** T4 (R2-05, R1-05): a range over the source-byte budget becomes several batches. */
  @Test(timeout = 60_000)
  public void rangeOverSourceBudgetSplitsWithoutFailure() throws Exception {
    History history = history(1);
    ServingIndexService service = service(history,
        config().setMaxSourceBytes(1024L * 1024), ServingIndexService.Hooks.NONE);
    assertTrue(service.awaitLive(WAIT));
    history.payloadBytes = 300 * 1024;
    history.publish(service, 1, 2);
    assertTrue(awaitThrough(service, 2));
    assertEquals(2, service.batches(0) + service.batches(1));
    assertNull(service.status().getLastFailure());
    history.assertOracle(service);
  }

  /**
   * T5 (service part): a block over every budget is a SOLO batch chunked by key; a query in
   * between never sees I + 1, and the result matches the oracle.
   */
  @Test(timeout = 60_000)
  public void oversizedBlockIsIndexedSoloInKeyAtomicChunks() throws Exception {
    History history = history(1);
    AtomicInteger chunks = new AtomicInteger();
    AtomicReference<Long> seenDuringSolo = new AtomicReference<>();
    AtomicReference<ServingIndexService> self = new AtomicReference<>();
    ServingIndexService service = service(history, config().setMaxSourceBytes(1024L * 1024)
        .setMaxWriteMutations(64).setMaxWriteBytes(64 * 1024), new ServingIndexService.Hooks() {
          @Override
          public void beforeSoloChunk(int index, int count) {
            chunks.set(count);
            if (index == 1) {
              seenDuringSolo.set(self.get().status().getIndexedThrough());
            }
          }
        });
    self.set(service);
    assertTrue(service.awaitLive(WAIT));
    history.keysPerBlock = 300;
    history.payloadBytes = 16 * 1024;
    history.publish(service, 1, 1);
    assertTrue(awaitThrough(service, 1));
    assertEquals(1, service.batches(2));
    assertTrue(chunks.get() > 2);
    assertEquals(Long.valueOf(0), seenDuringSolo.get());
    history.assertOracle(service);
  }

  /** T9: lag beyond live.maxLagBlocks returns to BULK_CATCH_UP, then LIVE again. */
  @Test(timeout = 60_000)
  public void lagBeyondThresholdFallsBackToBulkAndRecovers() throws Exception {
    History history = history(1);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean first = new AtomicBoolean(true);
    List<ServingState> seen = new CopyOnWriteArrayList<>();
    AtomicReference<ServingIndexService> self = new AtomicReference<>();
    ServingIndexService service = service(history, config().setLiveMaxLagBlocks(2),
        new ServingIndexService.Hooks() {
          @Override
          public void beforeBatch(long from, long last, boolean solo) {
            seen.add(self.get().status().getState());
            if (first.getAndSet(false)) {
              entered.countDown();
              await(release);
            }
          }
        });
    self.set(service);
    assertTrue(service.awaitLive(WAIT));
    history.publish(service, 1, 1);
    assertTrue(entered.await(10, TimeUnit.SECONDS));
    for (int block = 2; block <= 10; block++) {
      history.publish(service, block, block);
    }
    release.countDown();
    assertTrue(awaitThrough(service, 10));
    assertTrue(seen.toString(), seen.contains(ServingState.BULK_CATCH_UP));
    assertEquals(ServingState.LIVE, service.status().getState());
  }

  /**
   * T1 / T19 (R2-01, R1-05): injected serving write failures, a lost acknowledgement and a
   * missing History read only degrade serving; publication never waits or fails; transient
   * failures recover after reopening the handle and draining leases.
   */
  @Test(timeout = 60_000)
  public void transientFailuresDegradeAndRecoverWithoutTouchingHistory() throws Exception {
    History history = history(1);
    AtomicInteger failBefore = new AtomicInteger();
    AtomicInteger loseAck = new AtomicInteger();
    ServingIndexService service = service(history, config(), new ServingIndexService.Hooks() {
      @Override
      public void beforeBatch(long from, long last, boolean solo) throws IOException {
        if (failBefore.getAndDecrement() > 0) {
          throw new IOException("injected serving write I/O error");
        }
      }

      @Override
      public void afterCommit(long from, long last) throws IOException {
        if (loseAck.getAndDecrement() > 0) {
          throw new IOException("injected lost acknowledgement");
        }
      }
    });
    assertTrue(service.awaitLive(WAIT));
    failBefore.set(1);
    history.publish(service, 1, 1);
    awaitFailure(service, Reason.TRANSIENT_IO);
    for (int block = 2; block <= 4; block++) {
      // Common keeps publishing while serving is DEGRADED.
      history.publish(service, block, block);
    }
    assertTrue(awaitThrough(service, 4));
    loseAck.set(1);
    history.publish(service, 5, 5);
    assertTrue(awaitThrough(service, 5));
    assertEquals(ServingState.LIVE, service.status().getState());
    // History read failure: a missing canonical block is SOURCE_UNAVAILABLE (transient).
    history.hideCanonical = 6;
    history.publish(service, 6, 6);
    awaitFailure(service, Reason.SOURCE_UNAVAILABLE);
    assertNotReady(service);
    history.hideCanonical = -1;
    assertTrue(awaitThrough(service, 6));
    history.assertOracle(service);
  }

  /** T1 / T19: corrupt index data is permanent — DEGRADED without a fast retry loop. */
  @Test(timeout = 60_000)
  public void corruptIndexIsPermanentAndNeverRetriedFast() throws Exception {
    History history = history(1);
    AtomicInteger opens = new AtomicInteger();
    ServingIndexService service = service(history, config(), new ServingIndexService.Hooks() {
      @Override
      public void beforeBatch(long from, long last, boolean solo) {
        opens.incrementAndGet();
      }
    });
    assertTrue(service.awaitLive(WAIT));
    history.publish(service, 1, 1);
    assertTrue(awaitThrough(service, 1));
    try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
        history.servingRoot.resolve("single-v1").resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(ArchiveIndexStorage.put(
          PersistentServingKeyIndexGeneration.keyMetaKey("code", History.key(0)),
          new byte[]{1, 2, 3, 4, 5})), true);
    }
    history.publish(service, 2, 2);
    awaitFailure(service, Reason.INDEX_CORRUPT);
    int attempts = opens.get();
    history.publish(service, 3, 3);
    Thread.sleep(500);
    assertEquals(ServingState.DEGRADED, service.status().getState());
    assertEquals(attempts, opens.get());
    // T14: W and the backlog keep moving while DEGRADED.
    assertEquals(3, service.status().getCommittedThrough());
    assertEquals(2, service.status().getBacklogBlocks());
  }

  /** T11 (R2-01): ENGINE mismatch, legacy `current` and a corrupt descriptor only degrade. */
  @Test(timeout = 60_000)
  public void incompatibleOrCorruptIndexDegradesOnly() throws Exception {
    History mismatch = history(1);
    Path single = mismatch.servingRoot.resolve("single-v1");
    Files.createDirectories(single);
    StateArchiveIndexEngineManifest.openOrCreate(single, Engine.LEVELDB);
    ServingIndexService engine = service(mismatch, config(), ServingIndexService.Hooks.NONE);
    awaitFailure(engine, Reason.INDEX_INCOMPATIBLE);
    mismatch.publish(engine, 1, 2);
    assertNotReady(engine);

    History legacy = history(1);
    Files.createDirectories(legacy.servingRoot.resolve("current"));
    ServingIndexService current = service(legacy, config(), ServingIndexService.Hooks.NONE);
    awaitFailure(current, Reason.INDEX_INCOMPATIBLE);

    History corrupt = history(1);
    ServingIndexService healthy = service(corrupt, config(), ServingIndexService.Hooks.NONE);
    corrupt.publish(healthy, 1, 2);
    assertTrue(awaitThrough(healthy, 2));
    healthy.close();
    try (ArchiveIndexStorage.Writer raw = ArchiveIndexStorage.openWriter(
        corrupt.servingRoot.resolve("single-v1").resolve("keys"), Engine.ROCKSDB)) {
      raw.write(Collections.singletonList(ArchiveIndexStorage.put(
          PersistentServingKeyIndexGeneration.PROGRESS_KEY, new byte[100])), true);
    }
    ServingIndexService reopened = service(corrupt, config(), ServingIndexService.Hooks.NONE);
    awaitFailure(reopened, Reason.INDEX_CORRUPT);
  }

  /**
   * T12 / T20: a newer serving index over an older History is INDEX_AHEAD; an operator rebuild
   * (or autoRebuild) discards it, rebuilds from B0 in REBUILDING and becomes LIVE.
   */
  @Test(timeout = 60_000)
  public void indexAheadOfHistoryIsRebuilt() throws Exception {
    History newer = history(1);
    ServingIndexService first = service(newer, config(), ServingIndexService.Hooks.NONE);
    newer.publish(first, 1, 6);
    assertTrue(awaitThrough(first, 6));
    first.close();

    History older = history(1, newer.servingRoot);
    older.publishWithoutNotify(1, 3);
    ServingIndexService ahead = service(older, config(), ServingIndexService.Hooks.NONE);
    awaitFailure(ahead, Reason.INDEX_AHEAD);
    ahead.requestRebuild();
    assertTrue(ahead.awaitLive(WAIT));
    assertEquals(3, ahead.status().getIndexedThrough());
    older.assertOracle(ahead);
    ahead.close();

    History auto = history(1, newer.servingRoot);
    auto.publishWithoutNotify(1, 2);
    ServingIndexService rebuilt = service(auto, config().setAutoRebuild(true)
        .setRetryInitialBackoffMillis(10), ServingIndexService.Hooks.NONE);
    assertTrue(rebuilt.awaitLive(WAIT));
    assertEquals(2, rebuilt.status().getIndexedThrough());
    awaitNoDiscarded(newer.servingRoot);
  }

  @Test(timeout = 60_000)
  public void operatorRebuildRequestSurvivesDiskPressureWithBackoff() throws Exception {
    assertRebuildPausesUnderDiskPressure(false);
  }

  @Test(timeout = 60_000)
  public void automaticRebuildWaitsUnderDiskPressureWithBackoff() throws Exception {
    assertRebuildPausesUnderDiskPressure(true);
  }

  private void assertRebuildPausesUnderDiskPressure(boolean autoRebuild) throws Exception {
    History newer = history(1);
    ServingIndexService first = service(newer, config(), ServingIndexService.Hooks.NONE);
    newer.publish(first, 1, 6);
    assertTrue(awaitThrough(first, 6));
    first.close();

    History older = history(1, newer.servingRoot);
    older.publishWithoutNotify(1, 3);
    AtomicLong now = new AtomicLong(1_000);
    AtomicBoolean pressure = new AtomicBoolean(true);
    AtomicBoolean opening = new AtomicBoolean(true);
    AtomicInteger pressureChecks = new AtomicInteger();
    CountDownLatch expiredRetryObserved = new CountDownLatch(1);
    ServingIndexService service = new ServingIndexService(older.servingRoot, older.root,
        null, Engine.ROCKSDB, older.source, config().setAutoRebuild(autoRebuild)
            .setRetryInitialBackoffMillis(1_000).setRetryMaxBackoffMillis(1_000)
            .setPollMillis(1_000).validate(), ServingIndexService.Hooks.NONE, now::get);
    closeables.add(service);
    service.setDiskPressureSignal(() -> {
      if (opening.getAndSet(false)) {
        // Allow admission to observe INDEX_AHEAD before pressure pauses its rebuild.
        return false;
      }
      if (now.get() >= 2_000 && pressure.get()) {
        pressureChecks.incrementAndGet();
        expiredRetryObserved.countDown();
      }
      return pressure.get();
    });
    if (!autoRebuild) {
      service.requestRebuild();
    }
    service.start();
    awaitFailure(service, Reason.INDEX_AHEAD);
    Path discarded = older.servingRoot.resolve(ServingIndexService.DISCARDED_PREFIX + "test");
    Files.createDirectory(discarded);
    now.set(2_000);
    service.onCommonPublished(null);
    assertTrue(expiredRetryObserved.await(10, TimeUnit.SECONDS));
    Thread.sleep(150);
    assertEquals(ServingState.DEGRADED, service.status().getState());
    assertTrue("expired retry must back off instead of polling at 1 ms",
        pressureChecks.get() <= 5);
    assertFalse("discard cleanup continues while rebuild is paused", Files.exists(discarded));

    pressure.set(false);
    now.set(3_000);
    service.onCommonPublished(null);
    assertTrue(service.awaitLive(WAIT));
    assertEquals(3, service.status().getIndexedThrough());
    older.assertOracle(service);
  }

  /** T8 (component, G4): restart with I == W is LIVE with no notification; I < W catches up. */
  @Test(timeout = 60_000)
  public void readinessIsDerivedFromDurableStateOnly() throws Exception {
    History history = history(1);
    ServingIndexService first = service(history, config(), ServingIndexService.Hooks.NONE);
    history.publish(first, 1, 3);
    assertTrue(awaitThrough(first, 3));
    first.close();
    ServingIndexService equal = service(history, config(), ServingIndexService.Hooks.NONE);
    assertTrue(equal.awaitLive(WAIT));
    assertEquals(3, equal.status().getIndexedThrough());
    equal.close();
    history.publishWithoutNotify(4, 8);
    ServingIndexService behind = service(history, config(), ServingIndexService.Hooks.NONE);
    assertTrue("no Common notification is needed", awaitThrough(behind, 8));
    assertEquals(ServingState.LIVE, behind.status().getState());
  }

  /** T2 (component): deleting the serving directory offline rebuilds it from B0 to LIVE. */
  @Test(timeout = 60_000)
  public void deletedServingDirectoryRebuildsFromHistory() throws Exception {
    History history = history(1);
    ServingIndexService first = service(history, config(), ServingIndexService.Hooks.NONE);
    history.publish(first, 1, 5);
    assertTrue(awaitThrough(first, 5));
    first.close();
    ServingIndexService.deleteRecursively(history.servingRoot);
    List<ServingState> seen = new CopyOnWriteArrayList<>();
    AtomicReference<ServingIndexService> self = new AtomicReference<>();
    ServingIndexService rebuilt = service(history, config(), new ServingIndexService.Hooks() {
      @Override
      public void beforeBatch(long from, long last, boolean solo) {
        seen.add(self.get().status().getState());
      }
    });
    self.set(rebuilt);
    assertTrue(awaitThrough(rebuilt, 5));
    assertTrue(seen.toString(), seen.contains(ServingState.REBUILDING));
    assertFalse(rebuilt.status().isRebuildInProgress());
    history.assertOracle(rebuilt);
  }

  /** T7 (component, R2-07): notifications never wait; queries fail fast while not LIVE. */
  @Test(timeout = 60_000)
  public void handoffNeverBlocksCommonAndQueriesFailFast() throws Exception {
    History history = history(1);
    history.publishWithoutNotify(1, 5);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch entered = new CountDownLatch(1);
    ServingIndexService service = service(history, config(), new ServingIndexService.Hooks() {
      @Override
      public void beforeBatch(long from, long last, boolean solo) {
        entered.countDown();
        await(release);
      }
    });
    try {
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      assertEquals(ServingState.REBUILDING, service.status().getState());
      long started = System.nanoTime();
      for (int block = 6; block <= 20; block++) {
        history.publish(service, block, block);
      }
      assertNotReady(service);
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
    } finally {
      release.countDown();
    }
    assertTrue(awaitThrough(service, 20));
    // Static rule: no block-import or Common class references awaitLive.
    Path repo = repositoryRoot();
    try (Stream<Path> files = Stream.concat(
        Files.walk(repo.resolve("chainbase/src/main/java/org/tron/core/db2/core")),
        Stream.of(repo.resolve("framework/src/main/java/org/tron/core/db/Manager.java")))) {
      files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
        try {
          assertFalse(path.toString(), new String(Files.readAllBytes(path),
              java.nio.charset.StandardCharsets.UTF_8).contains("awaitLive"));
        } catch (IOException failure) {
          throw new AssertionError(failure);
        }
      });
    }
  }

  /**
   * T18: close is cooperative between solo chunks; with a build stuck inside a write the close
   * times out and leaves the native handle to process exit, never closing it under the build.
   */
  @Test(timeout = 60_000)
  public void closeStopsBetweenChunksAndTimesOutWithoutClosingTheHandle() throws Exception {
    History history = history(1);
    CountDownLatch inChunk = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ServingIndexService service = service(history, config().setMaxSourceBytes(1024L * 1024)
        .setMaxWriteMutations(64).setMaxWriteBytes(64 * 1024).setCloseTimeoutMillis(300),
        new ServingIndexService.Hooks() {
          @Override
          public void beforeSoloChunk(int index, int count) {
            if (index == 1) {
              inChunk.countDown();
              await(release);
            }
          }
        });
    assertTrue(service.awaitLive(WAIT));
    history.keysPerBlock = 300;
    history.payloadBytes = 16 * 1024;
    history.publish(service, 1, 1);
    assertTrue(inChunk.await(10, TimeUnit.SECONDS));
    ServingIndexStore handle = service.status().store();
    long started = System.nanoTime();
    service.close();
    assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(250));
    assertEquals(ServingState.CLOSED, service.status().getState());
    release.countDown();
    // The build thread now stops at the next chunk boundary; the handle was never closed under
    // it and is never used after close.
    Thread.sleep(300);
    assertEquals(0, handle.useAfterCloseCount());
    handle.close();
    ServingIndexService restarted = service(history, config(), ServingIndexService.Hooks.NONE);
    assertTrue(awaitThrough(restarted, 1));
    history.assertOracle(restarted);
  }

  /**
   * T21 (R3-06, R2-01): leases racing with reopen. A lease parked between count and view read,
   * or between view read and snapshot, keeps the handle open until it is released; after the
   * drain, no lease touches the closed handle and new acquisitions get NOT_READY.
   */
  @Test(timeout = 60_000)
  public void leaseAcquisitionRacesNeverUseAClosedHandle() throws Exception {
    for (int point = 0; point < 2; point++) {
      History history = history(1);
      CountDownLatch parked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicBoolean armed = new AtomicBoolean();
      AtomicInteger failBuild = new AtomicInteger();
      int injection = point;
      ServingIndexService service = service(history, config().setLeaseDrainTimeoutMillis(200)
          .setRetryInitialBackoffMillis(20), new ServingIndexService.Hooks() {
            @Override
            public void beforeBatch(long from, long last, boolean solo) throws IOException {
              if (failBuild.getAndDecrement() > 0) {
                throw new IOException("injected transient failure");
              }
            }

            @Override
            public void afterLeaseCounted() {
              if (injection == 0 && armed.compareAndSet(true, false)) {
                parked.countDown();
                await(release);
              }
            }

            @Override
            public void afterLeaseViewRead() {
              if (injection == 1 && armed.compareAndSet(true, false)) {
                parked.countDown();
                await(release);
              }
            }
          });
      history.publish(service, 1, 2);
      assertTrue(awaitThrough(service, 2));
      ServingIndexStore handle = service.status().store();
      ExecutorService query = Executors.newSingleThreadExecutor();
      try {
        armed.set(true);
        Future<Object> pending = query.submit(() -> {
          try (ServingLease lease = service.acquireLease()) {
            return lease.firstChangeAfter("code", History.key(0), 0);
          } catch (HistoricalQueryException notReady) {
            return notReady;
          }
        });
        assertTrue(parked.await(10, TimeUnit.SECONDS));
        failBuild.set(1);
        history.publish(service, 3, 3);
        awaitFailure(service, Reason.TRANSIENT_IO);
        // The drain cannot finish while the parked request holds its count.
        Thread.sleep(500);
        assertNotReady(service);
        assertEquals(0, handle.useAfterCloseCount());
        release.countDown();
        Object result = pending.get(10, TimeUnit.SECONDS);
        if (injection == 0) {
          // Parked before reading the view: it sees DEGRADED and never touches the handle.
          assertTrue(String.valueOf(result), result instanceof HistoricalQueryException);
        } else {
          // Parked after reading LIVE: it still snapshots the open handle and answers.
          assertEquals(OptionalLong.of(1), result);
        }
        assertTrue(awaitThrough(service, 3));
        assertEquals(0, handle.useAfterCloseCount());
        assertEquals(0, service.activeLeases());
      } finally {
        release.countDown();
        query.shutdownNow();
      }
    }
  }

  /** T14 (R2-11): state, failure, progress and backlog metrics are exported in every state. */
  @Test(timeout = 60_000)
  public void metricsFollowStateFailuresAndBacklog() throws Exception {
    boolean previous = CommonParameter.getInstance().isMetricsPrometheusEnable();
    CommonParameter.getInstance().setMetricsPrometheusEnable(true);
    try {
      History history = history(1);
      AtomicInteger fail = new AtomicInteger();
      ServingIndexService service = service(history, config()
          .setRetryInitialBackoffMillis(60_000), new ServingIndexService.Hooks() {
            @Override
            public void beforeBatch(long from, long last, boolean solo) throws IOException {
              if (fail.get() > 0) {
                throw new IOException("injected");
              }
            }
          });
      history.publish(service, 1, 2);
      assertTrue(awaitThrough(service, 2));
      assertEquals(1.0, gauge("tron_archive_serving_state", "state", "LIVE"), 0);
      assertEquals(0.0, gauge("tron_archive_serving_backlog_bytes", null, null), 0);
      fail.set(1);
      history.publish(service, 3, 3);
      awaitFailure(service, Reason.TRANSIENT_IO);
      assertEquals(1.0, gauge("tron_archive_serving_state", "state", "DEGRADED"), 0);
      assertTrue(CollectorRegistry.defaultRegistry.getSampleValue(
          "tron_archive_serving_failures_total", new String[]{"reason", "class"},
          new String[]{"TRANSIENT_IO", "transient"}) >= 1);
      history.publish(service, 4, 5);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (gauge("tron_archive_serving_progress", "kind", "committed_through") != 5
          && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(5.0, gauge("tron_archive_serving_progress", "kind", "committed_through"), 0);
      assertEquals(3.0, gauge("tron_archive_serving_progress", "kind", "backlog_blocks"), 0);
      assertTrue(gauge("tron_archive_serving_backlog_bytes", null, null) > 0);
      assertTrue(gauge("tron_archive_serving_last_failure_timestamp_seconds", null, null) > 0);
    } finally {
      CommonParameter.getInstance().setMetricsPrometheusEnable(previous);
    }
  }

  // ------------------------------------------------------------------------------- fixture

  private static double gauge(String name, String label, String value) {
    Double sample = label == null ? CollectorRegistry.defaultRegistry.getSampleValue(name)
        : CollectorRegistry.defaultRegistry.getSampleValue(name, new String[]{label},
            new String[]{value});
    assertNotNull(name, sample);
    return sample;
  }

  static ServingIndexService.Config config() {
    return ServingIndexService.Config.defaults().setRetryInitialBackoffMillis(20)
        .setRetryMaxBackoffMillis(200).setPollMillis(50).setLeaseDrainTimeoutMillis(2_000)
        .setCloseTimeoutMillis(10_000);
  }

  private ServingIndexService service(History history, ServingIndexService.Config config,
      ServingIndexService.Hooks hooks) {
    ServingIndexService service = new ServingIndexService(history.servingRoot, history.root,
        null, Engine.ROCKSDB, history.source, config.validate(), hooks,
        System::currentTimeMillis);
    closeables.add(service);
    service.start();
    return service;
  }

  private History history(long firstBlock) throws IOException {
    return history(firstBlock, null);
  }

  private History history(long firstBlock, Path servingRoot) throws IOException {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, servingRoot == null ? root.resolve("serving")
        : servingRoot, firstBlock);
    closeables.add(history.writer);
    return history;
  }

  /** Polls until LIVE with I >= block (awaitIndexedThrough fails fast when not LIVE). */
  private static boolean awaitThrough(ServingIndexService service, long block)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      ServingView view = service.status();
      if (view.getState() == ServingState.LIVE && view.getIndexedThrough() >= block) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
  }

  private static void awaitFailure(ServingIndexService service, Reason reason)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (System.nanoTime() < deadline) {
      ServingView view = service.status();
      if (view.getLastFailure() != null && view.getLastFailure().getReason() == reason) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("serving never failed with " + reason + ": " + service.status());
  }

  private static void assertNotReady(ServingIndexService service) {
    if (service.status().getState() == ServingState.LIVE) {
      return;
    }
    HistoricalQueryException failure = assertThrows(HistoricalQueryException.class,
        service::acquireLease);
    assertEquals(HistoricalQueryException.Reason.UNAVAILABLE, failure.getReason());
  }

  private static void awaitNoDiscarded(Path servingRoot) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      try (Stream<Path> entries = Files.list(servingRoot)) {
        if (entries.noneMatch(path -> path.getFileName().toString()
            .startsWith(ServingIndexService.DISCARDED_PREFIX))) {
          return;
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("discarded serving directories were not deleted");
  }

  private static Path repositoryRoot() {
    Path current = java.nio.file.Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    while (current != null && !Files.isDirectory(current.resolve("chainbase"))) {
      current = current.getParent();
    }
    assertNotNull(current);
    return current;
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(20, TimeUnit.SECONDS)) {
        throw new IllegalStateException("test latch timed out");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }

  /** A real History V5 root plus the published-boundary view serving consumes. */
  static final class History {
    final Path root;
    final Path servingRoot;
    final long firstBlock;
    final Writer writer;
    final Source source;
    final Map<Long, BlockSnapshotMeta> metas = new ConcurrentHashMap<>();
    final Map<Long, BlockReverseDiff> diffs = new ConcurrentHashMap<>();
    volatile long hideCanonical = -1;
    int keysPerBlock = 3;
    int payloadBytes = 8;
    long publishedThrough;

    History(Path root, Path servingRoot, long firstBlock) throws IOException {
      this.root = root;
      this.servingRoot = servingRoot;
      this.firstBlock = firstBlock;
      writer = new Writer(root, firstBlock, hash(firstBlock - 1),
          1L << 30);
      source = new Source(root, firstBlock, hash(firstBlock - 1),
          block -> block == hideCanonical ? null : metas.get(block));
      publishedThrough = firstBlock - 1;
    }

    void publish(ServingIndexService service, long first, long last) throws IOException {
      CommonCheckpointTarget target = publishWithoutNotify(first, last);
      service.onCommonPublished(target);
    }

    CommonCheckpointTarget publishWithoutNotify(long first, long last) throws IOException {
      if (first != publishedThrough + 1) {
        throw new IllegalArgumentException("test publishes contiguously");
      }
      for (long block = first; block <= last; block++) {
        BlockReverseDiff diff = diff(block);
        metas.put(block, diff.getMeta());
        diffs.put(block, diff);
        writer.append(diff);
      }
      CommonCheckpointTarget target = CommonCheckpointTarget.restore(hash(1_000),
          hash(2_000 + last), metas.get(first), metas.get(last), hash(3_000 + first),
          hash(4_000 + last));
      source.updatePublished(writer.forceTailReady(target), target);
      publishedThrough = last;
      return target;
    }

    private BlockReverseDiff diff(long block) {
      List<BlockReverseDiff.Entry> entries = new ArrayList<>();
      for (int key = 0; key < keysPerBlock; key++) {
        if (key > 0 && (block + key) % 2 == 0) {
          continue;
        }
        byte[] value = new byte[payloadBytes];
        new java.util.Random(block * 31 + key).nextBytes(value);
        entries.add(new BlockReverseDiff.Entry(key(key), OldValue.present(value)));
      }
      return new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block),
          hash(block - 1), block * 3000L), Collections.singletonList(
              new BlockReverseDiff.DbGroup("code", entries)));
    }

    /** Every key's firstChangeAfter over a lease equals the published diffs. */
    void assertOracle(ServingIndexService service) throws IOException {
      try (ServingLease lease = service.acquireLease()) {
        long through = lease.indexedThrough();
        for (int index = 0; index < keysPerBlock; index++) {
          final int key = index;
          for (long target = firstBlock - 1; target < through; target++) {
            Long expected = null;
            for (long block = target + 1; block <= through; block++) {
              BlockReverseDiff diff = diffs.get(block);
              boolean changed = diff.getGroups().stream().flatMap(group ->
                  group.getEntries().stream()).anyMatch(entry ->
                  java.util.Arrays.equals(entry.getKey(), key(key)));
              if (changed) {
                expected = block;
                break;
              }
            }
            assertEquals("key " + key + " after " + target,
                expected == null ? OptionalLong.empty() : OptionalLong.of(expected),
                lease.firstChangeAfter("code", key(key), target));
          }
        }
      }
    }

    static byte[] key(int key) {
      return ByteBuffer.allocate(4).putInt(key).array();
    }

    static byte[] hash(long value) {
      return ByteBuffer.allocate(32).putLong(value).array();
    }
  }
}
