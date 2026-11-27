package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.blocks;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.hash;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.plan;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.restoreBuildThreads;
import static org.tron.core.db2.archive.serving.ServingIndexStoreTest.setBuildThreads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * Parallel chunked preparation keeps the final index rows identical to the serial path:
 * workers only read and encode contiguous key chunks, the build thread performs every write,
 * and completed chunks may reach the sink in completion order.
 */
public class ServingIndexParallelBuildTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void parallelStoreMatchesSerialStoreRowByRow() throws Exception {
    Path serialDirectory = temporaryFolder.newFolder().toPath();
    Path parallelDirectory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore serial = ServingIndexStore.open(serialDirectory, Engine.ROCKSDB,
        1, hash(0), false, 1);
        ServingIndexStore parallel = openParallel(parallelDirectory, 4)) {
      commitWindows(serial);
      commitWindows(parallel);
      assertEquals(dump(serial), dump(parallel));
    }
    try (ServingIndexStore serial = ServingIndexStore.open(serialDirectory, Engine.ROCKSDB,
        1, hash(0), false, 1);
        ServingIndexStore parallel = ServingIndexStore.open(parallelDirectory, Engine.ROCKSDB,
            1, hash(0), false, 1)) {
      assertEquals(serial.indexedThrough(), parallel.indexedThrough());
      assertEquals(serial.keyChanges(), parallel.keyChanges());
      assertEquals(dump(serial), dump(parallel));
    }
  }

  /** INLINE, promotion, paged appends, an empty window, a page roll and a multi-key chunk. */
  private static void commitWindows(ServingIndexStore store) throws IOException {
    store.commit(plan(store, blocks(1, 4, 12)), false);
    store.commit(plan(store, blocks(5, 2, 12)), false);
    store.commit(plan(store, blocks(7, 3, 12)), false);
    store.commit(plan(store, blocks(10, 1, 0)), false);
    store.commit(plan(store, blocks(11, 5000, 1)), false);
    // 72 unique keys over 4 workers make chunkKeys 2, so parallel delivery works in chunks.
    store.commit(plan(store, blocks(5011, 2, 72)), false);
  }

  @Test
  public void parallelReadsOverlapAndMatchSerialReads() throws Exception {
    Path seedDirectory = temporaryFolder.newFolder().toPath();
    Map<String, byte[]> seeded;
    ServingIndexIncrementalPlan second;
    try (ServingIndexStore store = ServingIndexStore.open(seedDirectory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      store.commit(plan(store, blocks(1, 20, 8)), false);
      seeded = dumpRows(store);
      second = plan(store, blocks(21, 2, 8));
    }
    Map<ExactKey, long[]> changeMap = changes(second);
    assertEquals(8, changeMap.size());

    StubWriter serial = new StubWriter(seeded);
    List<Mutation> serialBatch = new ArrayList<>();
    try (ServingIndexParallelAppender serialAppender = new ServingIndexParallelAppender(1)) {
      serialAppender.appendAll(serial, changeMap, Long.MAX_VALUE, serialBatch::addAll);
    }
    assertTrue(serial.writes.isEmpty());

    // Both parked meta reads hold their worker until the other arrives: only genuinely
    // concurrent reads can finish before the five second await times out.
    CountDownLatch together = new CountDownLatch(2);
    StubWriter parallel = new StubWriter(seeded);
    parallel.awaitTogether(metaKeyHex(2), together);
    parallel.awaitTogether(metaKeyHex(3), together);
    List<Mutation> parallelBatch = new ArrayList<>();
    long started = System.nanoTime();
    try (ServingIndexParallelAppender appender = new ServingIndexParallelAppender(4)) {
      appender.appendAll(parallel, changeMap, Long.MAX_VALUE, parallelBatch::addAll);
    }
    assertTrue("the two parked reads never ran concurrently",
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 4000);
    assertEquals(0L, together.getCount());
    assertTrue(parallel.writes.isEmpty());
    assertEquals(serial.gets, parallel.gets);
    assertEquals(serialBatch.size(), parallelBatch.size());
    // Delivery order is the workers' completion order, so compare the delivered mutations
    // as a multiset of encoded sizes rather than index by index.
    List<Long> serialSizes = encodedSizes(serialBatch);
    List<Long> parallelSizes = encodedSizes(parallelBatch);
    Collections.sort(serialSizes);
    Collections.sort(parallelSizes);
    assertEquals(serialSizes, parallelSizes);
  }

  @Test
  public void laterChunksDeliverWhileASlowChunkIsStillPreparing() throws Exception {
    // 40 keys over 2 workers make chunkKeys 2, so the parked read stalls the whole first
    // chunk; every later chunk must still reach the sink while chunk 0 is in flight, which
    // order-preserving delivery never could.
    Map<ExactKey, long[]> changeMap = new LinkedHashMap<>();
    for (int key = 0; key < 40; key++) {
      changeMap.put(new ExactKey("code", ByteBuffer.allocate(4).putInt(key).array()),
          new long[] {100 + key});
    }

    StubWriter stub = new StubWriter();
    stub.parkFor(metaKeyHex(0), 3000);
    CountDownLatch delivered = new CountDownLatch(1);
    List<Mutation> batch = new CopyOnWriteArrayList<>();
    Thread builder = new Thread(() -> {
      try (ServingIndexParallelAppender appender = new ServingIndexParallelAppender(2)) {
        appender.appendAll(stub, changeMap, Long.MAX_VALUE, chunk -> {
          batch.addAll(chunk);
          delivered.countDown();
        });
      } catch (IOException failure) {
        throw new IllegalStateException(failure);
      }
    });
    builder.start();
    try {
      assertTrue("no chunk was delivered while the slow chunk was still preparing",
          delivered.await(1500, TimeUnit.MILLISECONDS));
    } finally {
      builder.join(30000);
      assertFalse(builder.isAlive());
    }
    assertTrue(stub.writes.isEmpty());
    assertEquals(40, batch.size());
  }

  /** Encoded sizes of the delivered mutations, for order-insensitive comparisons. */
  private static List<Long> encodedSizes(List<Mutation> mutations) {
    List<Long> sizes = new ArrayList<>(mutations.size());
    for (Mutation mutation : mutations) {
      sizes.add(mutation.estimatedBytes());
    }
    return sizes;
  }

  @Test
  public void workerFailureWaitsForInFlightReadsAndWritesNothing() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    ServingIndexIncrementalPlan window;
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB,
        1, hash(0), false, 1)) {
      window = plan(store, blocks(1, 3, 8));
    }
    Map<ExactKey, long[]> changeMap = changes(window);

    StubWriter stub = new StubWriter();
    stub.parkFor(metaKeyHex(3), 3000);
    stub.failAfterInFlight(metaKeyHex(5), "boom");
    List<Mutation> batch = new ArrayList<>();
    long started = System.nanoTime();
    try (ServingIndexParallelAppender appender = new ServingIndexParallelAppender(4)) {
      try {
        appender.appendAll(stub, changeMap, Long.MAX_VALUE, batch::addAll);
        fail("expected the worker failure to propagate");
      } catch (IllegalStateException expected) {
        assertEquals("boom", expected.getMessage());
      }
      // The failure was recorded only once a read was in flight, and appendAll waited for
      // that read to leave the database before propagating the failure.
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 2500);
      assertTrue(stub.writes.isEmpty());
      // Chunks delivered before the failure may already be in the sink; that is the streamed
      // flush contract (the durable-write gate plus recovery own their cleanup). The stub
      // never wrote anything, and the delivered prefix only contains complete key groups.
      for (Mutation mutation : batch) {
        assertTrue(mutation.estimatedBytes() > 0);
      }
      // The pool survives a failed window.
      StubWriter clean = new StubWriter();
      List<Mutation> retry = new ArrayList<>();
      appender.appendAll(clean, changeMap, Long.MAX_VALUE, retry::addAll);
      assertEquals(8, retry.size());
      assertTrue(clean.writes.isEmpty());
    }
  }

  @Test
  public void emptyWindowUnderParallelismStillAdvancesProgress() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = openParallel(directory, 4)) {
      store.commit(plan(store, blocks(1, 3, 8)), false);
      long through = store.indexedThrough();
      store.commit(plan(store, blocks(through + 1, 1, 0)), false);
      assertEquals(through + 1, store.indexedThrough());
    }
  }

  @Test
  public void closeStopsWorkerThreads() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    try (ServingIndexStore store = openParallel(directory, 4)) {
      store.commit(plan(store, blocks(1, 30, 16)), false);
    }
    assertWorkersStopped();
  }

  @Test
  public void sinkErrorDrainsWorkersBeforePropagating() throws Exception {
    // 200 keys over 4 workers make chunkKeys 6, so 33 more chunks are waiting when the sink
    // fails with an Error: every worker must still stop and exit before it propagates.
    Map<ExactKey, long[]> changeMap = new LinkedHashMap<>();
    for (int key = 0; key < 200; key++) {
      changeMap.put(new ExactKey("code", ByteBuffer.allocate(4).putInt(key).array()),
          new long[] {100 + key});
    }
    Error sinkFailure = new Error("injected sink failure");
    try (ServingIndexParallelAppender appender = new ServingIndexParallelAppender(4)) {
      try {
        appender.appendAll(new StubWriter(), changeMap, Long.MAX_VALUE, chunk -> {
          throw sinkFailure;
        });
        fail("expected the sink error to propagate");
      } catch (Error expected) {
        assertSame(sinkFailure, expected);
      }
      assertWorkersDrained();
    }
  }

  /** All submitted work must have finished before a sink failure propagates. */
  private static void assertWorkersDrained() {
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (!thread.getName().startsWith("serving-index-build-")) {
        continue;
      }
      for (StackTraceElement frame : thread.getStackTrace()) {
        assertFalse("worker is still preparing or delivering: " + frame,
            frame.getClassName().equals(ServingIndexParallelAppender.class.getName()));
      }
    }
  }

  /** No preparation worker may outlive the appender that submitted it. */
  private static void assertWorkersStopped() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (Thread.getAllStackTraces().keySet().stream()
        .anyMatch(thread -> thread.getName().startsWith("serving-index-build-"))) {
      assertTrue("worker threads did not stop", System.nanoTime() < deadline);
      Thread.sleep(20);
    }
  }

  private static ServingIndexStore openParallel(Path directory, int threads)
      throws IOException {
    // resolveParallelism() caps the request at the host's processors: skip rather than
    // silently compare two serial builds.
    assumeTrue("needs " + threads + " processors",
        Runtime.getRuntime().availableProcessors() >= threads);
    String previous = setBuildThreads(threads);
    try {
      return ServingIndexStore.open(directory, Engine.ROCKSDB, 1, hash(0), false, 1);
    } finally {
      restoreBuildThreads(previous);
    }
  }

  /** The same grouping ServingIndexStore applies before dispatching workers. */
  private static Map<ExactKey, long[]> changes(ServingIndexIncrementalPlan plan) {
    Map<ExactKey, List<Long>> grouped = new LinkedHashMap<>();
    for (Map.Entry<String, List<ServingIndexIncrementalPlan.KeyChange>> database
        : plan.getChangesByDatabase().entrySet()) {
      for (ServingIndexIncrementalPlan.KeyChange change : database.getValue()) {
        grouped.computeIfAbsent(ExactKey.owned(database.getKey(), change.getRawKey()),
            ignored -> new ArrayList<>()).add(change.getEpoch());
      }
    }
    Map<ExactKey, long[]> changes = new LinkedHashMap<>();
    grouped.forEach((key, epochs) -> {
      long[] values = new long[epochs.size()];
      for (int index = 0; index < values.length; index++) {
        values[index] = epochs.get(index);
      }
      changes.put(key, values);
    });
    return changes;
  }

  private static String metaKeyHex(int key) {
    return Hex.toHexString(PersistentServingKeyIndexGeneration.keyMetaKey("code",
        ByteBuffer.allocate(4).putInt(key).array()));
  }

  private static List<String> dump(ServingIndexStore store) throws IOException {
    List<String> rows = new ArrayList<>();
    try (ArchiveIndexStorage.Reader snapshot = store.snapshot();
        ArchiveIndexStorage.Cursor cursor = snapshot.cursor()) {
      cursor.seek(new byte[0]);
      ArchiveIndexStorage.KeyValue row;
      while ((row = cursor.next()) != null) {
        rows.add(Hex.toHexString(row.getKey()) + "=" + Hex.toHexString(row.getValue()));
      }
    }
    return rows;
  }

  private static Map<String, byte[]> dumpRows(ServingIndexStore store) throws IOException {
    Map<String, byte[]> rows = new LinkedHashMap<>();
    try (ArchiveIndexStorage.Reader snapshot = store.snapshot();
        ArchiveIndexStorage.Cursor cursor = snapshot.cursor()) {
      cursor.seek(new byte[0]);
      ArchiveIndexStorage.KeyValue row;
      while ((row = cursor.next()) != null) {
        rows.put(Hex.toHexString(row.getKey()), row.getValue());
      }
    }
    return rows;
  }

  /** In-memory writer: counts gets, records writes, parks or fails chosen keys on demand. */
  private static final class StubWriter implements ArchiveIndexStorage.Writer {
    private final ConcurrentHashMap<String, byte[]> rows = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Integer> gets = new ConcurrentHashMap<>();
    final CopyOnWriteArrayList<Mutation> writes = new CopyOnWriteArrayList<>();
    final ConcurrentHashMap<String, String> failures = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Long> parks = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, CountDownLatch> awaits = new ConcurrentHashMap<>();
    final CountDownLatch inFlight = new CountDownLatch(1);

    StubWriter() {
    }

    StubWriter(Map<String, byte[]> seeded) {
      seeded.forEach((hex, value) -> rows.put(hex, Arrays.copyOf(value, value.length)));
    }

    void parkFor(String hex, long millis) {
      parks.put(hex, millis);
    }

    void awaitTogether(String hex, CountDownLatch together) {
      awaits.put(hex, together);
    }

    void failAfterInFlight(String hex, String message) {
      failures.put(hex, message);
    }

    @Override
    public byte[] get(String columnFamily, byte[] key) {
      String hex = Hex.toHexString(key);
      gets.merge(hex, 1, Integer::sum);
      Long park = parks.get(hex);
      if (park != null) {
        inFlight.countDown();
        sleep(park);
      }
      CountDownLatch together = awaits.get(hex);
      if (together != null) {
        together.countDown();
        await(together);
      }
      String failure = failures.get(hex);
      if (failure != null) {
        await(inFlight);
        throw new IllegalStateException(failure);
      }
      byte[] value = rows.get(hex);
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public void write(List<Mutation> mutations) {
      writes.addAll(mutations);
    }

    @Override
    public void write(List<Mutation> mutations, boolean sync) {
      writes.addAll(mutations);
    }

    @Override
    public void close() {
    }

    private static void sleep(long millis) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    private static void await(CountDownLatch latch) {
      try {
        latch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
