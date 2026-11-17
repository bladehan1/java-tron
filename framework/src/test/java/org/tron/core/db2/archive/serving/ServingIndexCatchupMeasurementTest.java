package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Writer;
import org.tron.core.db2.archive.serving.ServingIndexServiceTest.History;
import org.tron.core.db2.archive.serving.status.ServingState;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Local real-V5/RocksDB workload, not a production throughput acceptance gate. */
public class ServingIndexCatchupMeasurementTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 180_000)
  public void measuresNetDrainIncludingRetriesAndConcurrentHistory() throws Exception {
    for (int round = 0; round < 3; round++) {
      measure(round);
    }
  }

  private void measure(int round) throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    history.keysPerBlock = 32;
    history.payloadBytes = 64;
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean gate = new AtomicBoolean();
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ServingIndexService.Config config = new ServingIndexService.Config()
        .setMaxBlocks(100).setMaxWriteMutations(64).setMaxWriteBytes(64 * 1024)
        .setPollMillis(10).setCloseTimeoutMillis(30_000);
    try (Writer ignored = history.writer;
        ServingIndexService service = new ServingIndexService(history.servingRoot, root,
            null, Engine.ROCKSDB, history.source, config, new ServingIndexService.Hooks() {
              @Override
              public void beforeBatch(long first, long last, boolean solo) {
                if (gate.compareAndSet(true, false)) {
                  entered.countDown();
                  try {
                    if (!release.await(30, TimeUnit.SECONDS)) {
                      throw new IllegalStateException("measurement gate timed out");
                    }
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                  }
                }
              }
            }, System::currentTimeMillis)) {
      service.start();
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      history.publish(service, 1, 300);
      awaitThrough(service, 300);
      gate.set(true);
      history.publish(service, 301, 700);
      assertTrue(entered.await(30, TimeUnit.SECONDS));
      long startI = service.status().getIndexedThrough();
      long startW = history.publishedThrough;
      Thread producer = new Thread(() -> {
        try {
          for (int added = 0; added < 60 && !stop.get(); added++) {
            long next = history.publishedThrough + 1;
            history.publish(service, next, next);
            Thread.sleep(20);
          }
        } catch (Throwable thrown) {
          failure.set(thrown);
        }
      }, "serving-measurement-producer");
      long start = System.nanoTime();
      producer.start();
      release.countDown();
      try {
        long deadline = start + TimeUnit.SECONDS.toNanos(45);
        while (service.status().getIndexedThrough() < startW && System.nanoTime() < deadline) {
          Thread.sleep(5);
        }
        assertTrue(service.status().getIndexedThrough() >= startW);
      } finally {
        stop.set(true);
        producer.join(10_000);
      }
      assertTrue(!producer.isAlive());
      assertNull(failure.get());
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      awaitThrough(service, history.publishedThrough);
      double seconds = (System.nanoTime() - start) / 1e9;
      long deltaI = service.status().getIndexedThrough() - startI;
      long deltaW = history.publishedThrough - startW;
      long backlogEnd = history.publishedThrough - service.status().getIndexedThrough();
      System.out.printf(java.util.Locale.ROOT,
          "SERVING_NET_DRAIN round=%d seconds=%.6f deltaI=%d deltaW=%d backlogStart=%d "
              + "backlogEnd=%d indexBps=%.3f arrivalBps=%.3f netBps=%.3f%n",
          round, seconds, deltaI, deltaW, startW - startI, backlogEnd,
          deltaI / seconds, deltaW / seconds, (deltaI - deltaW) / seconds);
      assertEquals(0, backlogEnd);
      history.assertOracle(service);
    } finally {
      release.countDown();
      stop.set(true);
    }
  }

  private static void awaitThrough(ServingIndexService service, long target)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      if (service.status().getState() == ServingState.LIVE
          && service.status().getIndexedThrough() >= target) {
        return;
      }
      Thread.sleep(5);
    }
    throw new AssertionError("serving did not reach target: " + service.status());
  }
}
