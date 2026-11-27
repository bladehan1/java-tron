package org.tron.core.db2.archive.serving;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation;

/**
 * Parallel chunked preparation for one serving index flush, Geth-style: workers claim
 * contiguous chunks of grouped keys from a shared cursor, so distinct keys are appended
 * concurrently while one key's epochs stay strictly ordered on a single worker. Workers only
 * read the writer and produce complete per-key mutation groups; the build thread still
 * performs every write, so failure semantics and the on-disk format are identical to the
 * serial path.
 *
 * <p>Finished chunks stream through {@link ChunkSink} in completion order while workers keep
 * preparing later chunks; a bounded queue caps in-flight results. Only a key's own epochs
 * must stay ordered, so a slow chunk never delays already-finished chunks, and delivering
 * one chunk per hand-off keeps queue traffic independent of the key count. Recovery mode
 * re-checks each key against a crashed flush: keys still at the old boundary append normally,
 * keys the interrupted flush already wrote verify against the replayed epochs and are
 * skipped.
 *
 * <p>Parallelism is {@link #THREADS_PROPERTY} (default 1, the serial path) capped at the
 * available processors. The first failure stops dispatching; in-flight workers finish their
 * current key and exit before the failure propagates. In a streamed flush chunks already
 * delivered to the sink may be durable data batches guarded by the flush intent; recovery
 * replays and verifies them instead of assuming nothing was written.
 */
final class ServingIndexParallelAppender implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServingIndexParallelAppender.class);
  /** Worker threads for chunked index preparation; 1 keeps the serial path. */
  static final String THREADS_PROPERTY = "tron.archive.serving.buildThreads";
  /** Target chunk count per worker; smaller chunks balance hot and cold key ranges. */
  private static final int CHUNKS_PER_WORKER = 8;
  /** Upper bound of keys per chunk, capping in-flight result memory. */
  private static final int MAX_CHUNK_KEYS = 1024;
  private static final long CLOSE_WAIT_SECONDS = 10;

  /** Receives completed chunks in completion order; the sink decides how writes batch. */
  @FunctionalInterface
  interface ChunkSink {

    void accept(List<Mutation> chunk) throws IOException;
  }

  /** Processes one key, returning its complete mutation group (empty when nothing to write). */
  @FunctionalInterface
  interface KeyProcessor {

    List<Mutation> process(ArchiveIndexStorage.Writer reader, ExactKey key, long[] epochs)
        throws IOException;
  }

  private final int parallelism;
  private ExecutorService pool;

  ServingIndexParallelAppender(int parallelism) {
    this.parallelism = Math.max(1, parallelism);
  }

  /** Serial unless the property selects bounded parallel workers; never above available CPUs. */
  static int resolveParallelism() {
    int requested = Integer.getInteger(THREADS_PROPERTY, 1);
    int usable = Runtime.getRuntime().availableProcessors();
    return Math.max(1, Math.min(requested, usable));
  }

  /**
   * Appends every grouped key's changes exactly once; chunks of contiguous input keys reach
   * the sink in the workers' completion order, which is safe because only a key's own epochs
   * must stay ordered. All workers of one call share {@code durableI}, exactly like the
   * serial loop reading one durable progress value.
   */
  void appendAll(ArchiveIndexStorage.Writer reader, Map<ExactKey, long[]> changes,
      long durableI, ChunkSink sink) throws IOException {
    runKeys(reader, changes, (source, key, epochs) -> {
      List<Mutation> mutations = new ArrayList<>();
      PersistentServingKeyIndexGeneration.appendExactChanges(source, mutations, key, epochs,
          durableI);
      return mutations;
    }, sink);
  }

  /**
   * Recovery counterpart of {@link #appendAll}: a key still at or below {@code baseI} appends
   * normally; a key the interrupted flush already wrote (last epoch in
   * {@code (baseI, target]}) must match the replayed epochs exactly and is skipped; anything
   * beyond {@code target} is corruption.
   */
  void recoverAll(ArchiveIndexStorage.Writer reader, Map<ExactKey, long[]> changes,
      long baseI, long target, ChunkSink sink) throws IOException {
    runKeys(reader, changes, (source, key, epochs) ->
        PersistentServingKeyIndexGeneration.recoverExactChanges(source, key, epochs, baseI,
            target), sink);
  }

  private void runKeys(ArchiveIndexStorage.Writer reader, Map<ExactKey, long[]> changes,
      KeyProcessor processor, ChunkSink sink) throws IOException {
    if (changes.isEmpty()) {
      return;
    }
    List<Map.Entry<ExactKey, long[]>> entries = new ArrayList<>(changes.entrySet());
    int chunkKeys = Math.max(1, Math.min(entries.size() / (parallelism * CHUNKS_PER_WORKER),
        MAX_CHUNK_KEYS));
    int chunkCount = (entries.size() + chunkKeys - 1) / chunkKeys;
    if (parallelism == 1 || entries.size() < 2) {
      for (int chunk = 0; chunk < chunkCount; chunk++) {
        sink.accept(processChunk(reader, processor, entries, chunkKeys, chunk, null));
      }
      return;
    }
    runParallel(reader, processor, entries, chunkKeys, chunkCount, sink);
  }

  /**
   * One chunk per claim, one chunk per delivery: workers take the next unclaimed chunk of
   * contiguous input keys from a shared cursor, and the build thread consumes finished
   * chunks in completion order. Only a key's own epochs must stay ordered, so no
   * re-sequencing buffer exists and a slow chunk never delays already-finished chunks.
   */
  private void runParallel(ArchiveIndexStorage.Writer reader, KeyProcessor processor,
      List<Map.Entry<ExactKey, long[]>> entries, int chunkKeys, int chunkCount, ChunkSink sink)
      throws IOException {
    AtomicInteger nextChunk = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ExecutorService workers = pool();
    int workerCount = Math.min(parallelism, chunkCount);
    BlockingQueue<Object> completed = new ArrayBlockingQueue<>(workerCount * 2);
    List<Future<?>> futures = new ArrayList<>(workerCount);
    try {
      for (int worker = 0; worker < workerCount; worker++) {
        futures.add(workers.submit(() ->
            runWorker(reader, processor, entries, chunkKeys, chunkCount, nextChunk, failure,
                completed)));
      }
      // Consume finished chunks in completion order. Poll with a failure check:
      // producers drop work once a failure is recorded, so the consumer must never block
      // indefinitely on a queue nobody feeds anymore.
      int emitted = 0;
      while (emitted < chunkCount && failure.get() == null) {
        Object taken = pollCompleted(completed);
        if (Thread.currentThread().isInterrupted()) {
          failure.compareAndSet(null, new InterruptedException());
          break;
        }
        if (!(taken instanceof List)) {
          continue; // poison or timeout: a worker exited or is still preparing
        }
        @SuppressWarnings("unchecked")
        List<Mutation> chunk = (List<Mutation>) taken;
        sink.accept(chunk);
        emitted++;
      }
    } catch (IOException | RuntimeException | Error consumerFailure) {
      // The sink aborted the flush. Record it before draining, whatever its type, so workers
      // stop after their current key instead of waiting forever on a queue nobody drains.
      if (!failure.compareAndSet(null, consumerFailure)
          && failure.get() != consumerFailure) {
        failure.get().addSuppressed(consumerFailure);
      }
    }
    // Drain unconditionally, including after a sink Error or a partial submission failure.
    if (awaitTerminated(futures, failure)) {
      Thread.currentThread().interrupt();
    }
    Throwable cause = failure.get();
    if (cause != null) {
      if (cause instanceof InterruptedException) {
        throw new IOException("Serving parallel index build interrupted", cause);
      }
      rethrow(cause);
    }
  }

  /** Claims and processes chunks until none remain or a failure is recorded; never throws. */
  private void runWorker(ArchiveIndexStorage.Writer reader, KeyProcessor processor,
      List<Map.Entry<ExactKey, long[]>> entries, int chunkKeys, int chunkCount,
      AtomicInteger nextChunk, AtomicReference<Throwable> failure,
      BlockingQueue<Object> completed) {
    try {
      for (int chunk = nextChunk.getAndIncrement(); chunk < chunkCount
          && failure.get() == null; chunk = nextChunk.getAndIncrement()) {
        put(completed, processChunk(reader, processor, entries, chunkKeys, chunk, failure),
            failure);
      }
    } catch (IOException | RuntimeException | Error workerFailure) {
      failure.compareAndSet(null, workerFailure);
    } finally {
      put(completed, POISON, failure);
    }
  }

  /**
   * Processes one chunk's contiguous keys in order. A parallel worker stops at the first key
   * once a failure is recorded and delivers the partial chunk; the flush fails anyway. The
   * serial path passes a {@code null} failure reference and always processes every key.
   */
  private List<Mutation> processChunk(ArchiveIndexStorage.Writer reader, KeyProcessor processor,
      List<Map.Entry<ExactKey, long[]>> entries, int chunkKeys, int chunk,
      AtomicReference<Throwable> failure) throws IOException {
    List<Mutation> mutations = new ArrayList<>();
    int last = Math.min(entries.size(), (chunk + 1) * chunkKeys);
    for (int index = chunk * chunkKeys; index < last
        && (failure == null || failure.get() == null); index++) {
      Map.Entry<ExactKey, long[]> entry = entries.get(index);
      mutations.addAll(processor.process(reader, entry.getKey(), entry.getValue()));
    }
    return mutations;
  }

  private static final Object POISON = new Object();

  private static void put(BlockingQueue<Object> completed, Object item,
      AtomicReference<Throwable> failure) {
    try {
      // Offer with a failure check: once a failure is recorded the consumer stops draining,
      // so a blocking put here would wedge the worker and hang every future.get().
      while (!completed.offer(item, 100, TimeUnit.MILLISECONDS)) {
        if (failure.get() != null) {
          return; // drop the item; the flush fails anyway
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failure.compareAndSet(null, interrupted);
    }
  }

  /** Takes one completion, timing out so the caller can re-check the failure flag. */
  private static Object pollCompleted(BlockingQueue<Object> completed) {
    try {
      return completed.poll(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return POISON;
    }
  }

  /** Waits for every worker to exit (they stop after their current key once failure is set). */
  private static boolean awaitTerminated(List<Future<?>> futures,
      AtomicReference<Throwable> failure) {
    boolean interrupted = false;
    for (Future<?> future : futures) {
      while (true) {
        try {
          future.get();
          break;
        } catch (InterruptedException waiting) {
          interrupted = true;
          failure.compareAndSet(null, waiting);
        } catch (ExecutionException unexpected) {
          // A worker normally routes its own failures; this covers any other fatal exit.
          failure.compareAndSet(null, unexpected.getCause());
          break;
        } catch (RuntimeException unexpected) {
          failure.compareAndSet(null, unexpected);
          break;
        }
      }
    }
    return interrupted;
  }

  private static void rethrow(Throwable failure) throws IOException {
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    throw new IOException("Serving parallel index build failed", failure);
  }

  private synchronized ExecutorService pool() {
    if (pool == null) {
      AtomicInteger seq = new AtomicInteger();
      pool = Executors.newFixedThreadPool(parallelism, runnable -> {
        Thread thread = new Thread(runnable, "serving-index-build-" + seq.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      });
      LOGGER.info("Serving index parallel build enabled: threads={}", parallelism);
    }
    return pool;
  }

  /** Stops accepting chunks and waits for workers; workers are never in flight here. */
  @Override
  public synchronized void close() {
    if (pool != null) {
      pool.shutdown();
      try {
        if (!pool.awaitTermination(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)) {
          LOGGER.warn("Serving index build workers did not stop within {}s; forcing",
              CLOSE_WAIT_SECONDS);
          pool.shutdownNow();
        }
      } catch (InterruptedException interrupted) {
        pool.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        pool = null;
      }
    }
  }
}
