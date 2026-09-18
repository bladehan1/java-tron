package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/**
 * Benchmark-only ordered worker that removes PathState preparation from the PushBlock thread.
 *
 * <p>The queue is bounded and enqueue blocks when the worker cannot sustain the input rate. This
 * owner deliberately returns no Snapshot delta, so it must not cross a Snapshot flush boundary.
 */
@Slf4j(topic = "DB")
public final class PathStateAsyncPrepareHead implements PathStateHead {

  private static final int DEFAULT_QUEUE_CAPACITY = 64;

  private final PathStateHead delegate;
  private final BlockingQueue<Work> queue;
  private final Thread worker;
  private Work pending;
  private volatile PathStateRootMetadata completed;
  private volatile Throwable failure;
  private volatile boolean closed;

  public PathStateAsyncPrepareHead(PathStateHead delegate) throws IOException {
    this(delegate, DEFAULT_QUEUE_CAPACITY);
  }

  PathStateAsyncPrepareHead(PathStateHead delegate, int queueCapacity) throws IOException {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("async PathState queue capacity must be positive");
    }
    queue = new ArrayBlockingQueue<>(queueCapacity);
    completed = delegate.getHead();
    worker = new Thread(this::run, "path-state-async-prepare");
    worker.setDaemon(true);
    worker.start();
  }

  @Override
  public synchronized PathStateSnapshotDelta prepareSnapshotDelta(BlockSnapshotMeta meta,
      PathStateBlockTransition transition) throws IOException {
    requireHealthy();
    if (pending != null) {
      throw new IOException("async PathState transition is already pending enqueue");
    }
    pending = new Work(Objects.requireNonNull(meta, "meta"),
        Objects.requireNonNull(transition, "transition"));
    return null;
  }

  @Override
  public synchronized PathStateRootMetadata advance(PathStateBlockTransition transition)
      throws IOException {
    requireHealthy();
    if (pending == null || pending.transition != Objects.requireNonNull(transition, "transition")) {
      throw new IOException("async PathState publication differs from captured transition");
    }
    Work admitted = pending;
    pending = null;
    long startedNanos = System.nanoTime();
    try {
      queue.put(admitted);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("async PathState enqueue interrupted", interrupted);
    }
    logger.info("Path-state async enqueued: head={}, queueDepth={}, enqueueMicros={}",
        transition.getBlockNumber(), queue.size(),
        TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startedNanos));
    return copy(completed);
  }

  @Override
  public synchronized PathStateRootMetadata rewindTo(long blockNumber, byte[] blockHash)
      throws IOException {
    requireHealthy();
    if (pending != null || !queue.isEmpty()) {
      throw new IOException("async PathState rewind requires an empty benchmark queue");
    }
    completed = delegate.rewindTo(blockNumber, blockHash);
    return copy(completed);
  }

  @Override
  public PathStateRootMetadata flushBaseThrough(long blockNumber, byte[] blockHash)
      throws IOException {
    awaitThrough(blockNumber, blockHash);
    return delegate.flushBaseThrough(blockNumber, blockHash);
  }

  @Override
  public synchronized byte[] preview(PathStateBlockTransition transition) {
    return null;
  }

  @Override
  public synchronized PathStateRootMetadata getHead() throws IOException {
    requireHealthy();
    return copy(completed);
  }

  private void awaitThrough(long blockNumber, byte[] blockHash) throws IOException {
    byte[] expectedHash = Arrays.copyOf(Objects.requireNonNull(blockHash, "blockHash"),
        blockHash.length);
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
    while (true) {
      requireHealthy();
      PathStateRootMetadata current = completed;
      if (current.getBlockNumber() == blockNumber
          && Arrays.equals(current.getBlockHash(), expectedHash)) {
        return;
      }
      if (current.getBlockNumber() > blockNumber || System.nanoTime() >= deadline) {
        throw new IOException("async PathState worker did not reach requested flush target");
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IOException("async PathState wait interrupted", interrupted);
      }
    }
  }

  private void run() {
    while (!closed || !queue.isEmpty()) {
      try {
        Work work = queue.poll(100L, TimeUnit.MILLISECONDS);
        if (work == null) {
          continue;
        }
        long startedNanos = System.nanoTime();
        delegate.prepareSnapshotDelta(work.meta, work.transition);
        PathStateRootMetadata advanced = delegate.advance(work.transition);
        completed = advanced;
        logger.info("Path-state async prepared: head={}, queueDepth={}, serviceMs={}",
            advanced.getBlockNumber(), queue.size(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos));
      } catch (InterruptedException interrupted) {
        if (!closed) {
          failure = interrupted;
          Thread.currentThread().interrupt();
        }
        return;
      } catch (IOException | RuntimeException currentFailure) {
        failure = currentFailure;
        logger.error("Path-state async worker failed", currentFailure);
        return;
      }
    }
  }

  private void requireHealthy() throws IOException {
    if (closed) {
      throw new IOException("async PathState owner is closed");
    }
    if (failure != null) {
      throw new IOException("async PathState worker failed", failure);
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    try {
      worker.join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("async PathState close interrupted", interrupted);
    } finally {
      if (worker.isAlive()) {
        worker.interrupt();
      }
      delegate.close();
    }
  }

  private static PathStateRootMetadata copy(PathStateRootMetadata metadata) {
    return PathStateRootMetadata.decode(metadata.encode());
  }

  private static final class Work {

    private final BlockSnapshotMeta meta;
    private final PathStateBlockTransition transition;

    private Work(BlockSnapshotMeta meta, PathStateBlockTransition transition) {
      this.meta = meta;
      this.transition = transition;
    }
  }
}
