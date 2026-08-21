package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveHistoryWriter.Stage;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class AsyncArchiveHistorySinkTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void waitsForDurabilityAndRevertsCommittedHead() throws Exception {
    Path archive = temporaryFolder.newFolder("async").toPath();
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases());
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 2)) {
      sink.accept(diff(1));
      sink.accept(diff(2));
      sink.awaitCommitted(2);
      assertEquals(2, writer.committedHead().getMeta().getEpoch());
      sink.revert(diff(2).getMeta());
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
      sink.releaseThrough(1);
    }
  }

  @Test
  public void persistsOneFlushRangeAsOneDurabilityBatch() throws Exception {
    Path archive = temporaryFolder.newFolder("async-batch").toPath();
    java.util.List<Stage> stages = new ArrayList<>();
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases(),
        (stage, meta) -> stages.add(stage));
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 2)) {
      sink.acceptAll(Arrays.asList(diff(1), diff(2), diff(3)));
      sink.awaitCommitted(3);
      assertEquals(3, writer.committedHead().getMeta().getEpoch());
      assertEquals(3, stages.stream().filter(stage -> stage == Stage.APPEND_BODY).count());
      assertEquals(3, stages.stream().filter(stage -> stage == Stage.APPEND_INDEX).count());
      assertEquals(1, stages.stream().filter(stage -> stage == Stage.SYNC_BODY).count());
      assertEquals(1, stages.stream().filter(stage -> stage == Stage.SYNC_INDEX).count());
      assertEquals(3, stages.stream().filter(stage -> stage == Stage.COMMIT_MARKER).count());
    }
  }

  @Test
  public void createsEvidenceFromTheSameDurableWriterAuthority() throws Exception {
    Path archive = temporaryFolder.newFolder("async-evidence").toPath();
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
        ArchiveStoreScope.getStateDatabases());
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 1)) {
      BlockReverseDiff committed = diff(1);
      sink.accept(committed);
      sink.awaitCommitted(1);

      List<HistoryCommitMarker> evidence = sink.createMarkerRangeEvidence(1)
          .read(Collections.singletonList(committed.getMeta()));

      assertEquals(1, evidence.size());
      assertEquals(committed.getMeta(), evidence.get(0).getMeta());
    }
  }

  @Test
  public void removesQueuedForkHeadWithoutPublishingIt() throws Exception {
    Path archive = temporaryFolder.newFolder("queued-reorg").toPath();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases(),
        (stage, meta) -> {
          if (stage == Stage.APPEND_BODY && meta.getEpoch() == 1) {
            entered.countDown();
            await(release);
          }
        });
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 1)) {
      sink.accept(diff(1));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      sink.accept(diff(2));
      sink.revert(diff(2).getMeta());
      release.countDown();
      sink.awaitCommitted(1);
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void fullQueueAppliesBackpressureWithoutDroppingBlocks() throws Exception {
    Path archive = temporaryFolder.newFolder("backpressure").toPath();
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch entered = new CountDownLatch(1);
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases(),
        (stage, meta) -> {
          if (stage == Stage.APPEND_BODY && meta.getEpoch() == 1) {
            entered.countDown();
            await(release);
          }
        });
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 1)) {
      sink.accept(diff(1));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      sink.accept(diff(2));
      CountDownLatch thirdAccepted = new CountDownLatch(1);
      Thread producer = new Thread(() -> {
        sink.accept(diff(3));
        thirdAccepted.countDown();
      });
      producer.start();
      assertFalse(thirdAccepted.await(200, TimeUnit.MILLISECONDS));
      release.countDown();
      assertTrue(thirdAccepted.await(5, TimeUnit.SECONDS));
      sink.awaitCommitted(3);
      producer.join();
      assertEquals(3, writer.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void writerFailureBecomesFatalForQueue() throws Exception {
    Path archive = temporaryFolder.newFolder("failure").toPath();
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases(),
        (stage, meta) -> {
          if (stage == Stage.APPEND_INDEX) {
            throw new java.io.IOException("injected");
          }
        });
    try (AsyncArchiveHistorySink sink = new AsyncArchiveHistorySink(writer, 1)) {
      sink.accept(diff(1));
      assertThrows(ArchivePersistenceException.class, () -> sink.awaitCommitted(1));
      assertThrows(ArchivePersistenceException.class, () -> sink.accept(diff(2)));
    }
  }

  private static Set<String> databases() {
    return new java.util.LinkedHashSet<>(Arrays.asList("account", "properties"));
  }

  private static BlockReverseDiff diff(int number) {
    return new BlockReverseDiff(new BlockSnapshotMeta(number, number, hash(number),
        hash(number - 1), number), Collections.singletonList(new DbGroup("account",
        Collections.singletonList(new Entry(bytes("key-" + number), OldValue.absent())))));
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static void await(CountDownLatch latch) throws java.io.IOException {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new java.io.IOException("interrupted", e);
    }
  }
}
