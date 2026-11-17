package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.history.v5.V5TestSupport.Writer;
import org.tron.core.db2.archive.serving.ServingIndexServiceTest.History;
import org.tron.core.db2.archive.serving.source.ServingHistorySource;
import org.tron.core.db2.archive.serving.status.ServingState;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

public class ServingIndexSourceWindowTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void retainsDecodedSuffixAcrossCommitsAndConcurrentPublication() throws Exception {
    run(false);
  }

  @Test
  public void discardsDecodedSuffixAfterUncertainCommitAndReopensAtDurableI() throws Exception {
    run(true);
  }

  @Test
  public void pressureAndCloseStopBeforeConsumingBufferedSuffix() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    CountingSource source = new CountingSource(history.source);
    AtomicBoolean pressure = new AtomicBoolean();
    ServingIndexService.Config config = new ServingIndexService.Config()
        .setMaxPlanEntries(5).setPollMillis(10).setRetryInitialBackoffMillis(10)
        .setRetryMaxBackoffMillis(50).setCloseTimeoutMillis(10_000);
    try (Writer ignored = history.writer) {
      history.publishWithoutNotify(1, 9);
      try (ServingIndexService first = new ServingIndexService(history.servingRoot, root, null,
          Engine.ROCKSDB, source, config, new ServingIndexService.Hooks() {
            @Override
            public void afterCommit(long from, long last) {
              pressure.set(true);
            }
          }, System::currentTimeMillis)) {
        first.setDiskPressureSignal(pressure::get);
        first.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (first.status().getState() != ServingState.DEGRADED
            && System.nanoTime() < deadline) {
          Thread.sleep(5);
        }
        assertEquals(ServingState.DEGRADED, first.status().getState());
        assertEquals(2, first.status().getIndexedThrough());
        assertEquals(1, source.reads.size());
      }
      try (ServingIndexService reopened = new ServingIndexService(history.servingRoot, root, null,
          Engine.ROCKSDB, source, config, ServingIndexService.Hooks.NONE,
          System::currentTimeMillis)) {
        reopened.start();
        assertTrue(reopened.awaitLive(Duration.ofSeconds(30)));
        assertEquals(9, reopened.status().getIndexedThrough());
        assertEquals(2, source.reads.size());
        assertEquals("3:9", source.reads.get(1));
        history.assertOracle(reopened);
      }
    }
  }

  private void run(boolean failAfterCommit) throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    History history = new History(root, root.resolve("serving"), 1);
    CountingSource source = new CountingSource(history.source);
    List<Long> committed = new ArrayList<>();
    AtomicBoolean first = new AtomicBoolean(true);
    AtomicReference<ServingIndexService> self = new AtomicReference<>();
    ServingIndexService.Config config = new ServingIndexService.Config()
        .setMaxPlanEntries(5).setPollMillis(10).setRetryInitialBackoffMillis(10)
        .setRetryMaxBackoffMillis(50).setCloseTimeoutMillis(10_000);
    try (Writer ignored = history.writer;
        ServingIndexService service = new ServingIndexService(history.servingRoot, root, null,
            Engine.ROCKSDB, source, config, new ServingIndexService.Hooks() {
              @Override
              public void afterCommit(long from, long last) throws IOException {
                committed.add(last);
                if (first.getAndSet(false)) {
                  if (failAfterCommit) {
                    throw new IOException("injected failure after durable publication");
                  }
                  history.publish(self.get(), 10, 12);
                }
              }
            }, System::currentTimeMillis)) {
      self.set(service);
      history.publishWithoutNotify(1, 9);
      service.start();
      assertTrue(service.awaitLive(Duration.ofSeconds(30)));
      assertEquals(failAfterCommit ? 9 : 12, service.status().getIndexedThrough());
      assertEquals(Long.valueOf(2), committed.get(0));
      assertEquals(2, source.reads.size());
      assertEquals("1:9", source.reads.get(0));
      assertEquals(failAfterCommit ? "3:9" : "10:12", source.reads.get(1));
      history.assertOracle(service);
    }
  }

  private static final class CountingSource implements ServingHistorySource {
    private final ServingHistorySource delegate;
    private final List<String> reads = new ArrayList<>();

    private CountingSource(ServingHistorySource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Committed committed() {
      return delegate.committed();
    }

    @Override
    public long[] bundleBytes(Committed view, long first, long last) throws IOException {
      return delegate.bundleBytes(view, first, last);
    }

    @Override
    public List<CommittedBlock> read(Committed view, long first, long last) throws IOException {
      reads.add(first + ":" + last);
      return delegate.read(view, first, last);
    }

    @Override
    public byte[] blockHash(Committed view, long block) throws IOException {
      return delegate.blockHash(view, block);
    }
  }
}
