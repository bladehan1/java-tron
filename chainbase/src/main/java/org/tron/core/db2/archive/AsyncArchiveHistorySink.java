package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Bounded single-writer queue with explicit durable-epoch waiting and head-only reorg handling. */
public final class AsyncArchiveHistorySink implements DurableBlockReverseDiffSink, Closeable {

  private final ArchiveHistoryWriter writer;
  private final BlockingQueue<WorkItem> queue;
  private final Map<Long, WorkItem> submitted = new LinkedHashMap<>();
  private final Thread worker;
  private volatile Throwable fatalFailure;
  private volatile boolean closed;
  private BlockSnapshotMeta acceptedHead;

  public AsyncArchiveHistorySink(ArchiveHistoryWriter writer, int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.writer = writer;
    this.queue = new ArrayBlockingQueue<>(capacity);
    HistoryCommitMarker committedHead = writer.committedHead();
    this.acceptedHead = committedHead == null ? null : committedHead.getMeta();
    this.worker = new Thread(this::run, "archive-history-writer");
    this.worker.setDaemon(true);
    this.worker.start();
  }

  @Override
  public void accept(BlockReverseDiff diff) {
    WorkItem item = new WorkItem(diff);
    synchronized (submitted) {
      ensureOperational();
      if (acceptedHead != null) {
        validateContinuity(acceptedHead, diff.getMeta());
      }
      if (submitted.put(diff.getMeta().getEpoch(), item) != null) {
        throw new ArchivePersistenceException("Duplicate submitted archive epoch");
      }
      acceptedHead = diff.getMeta();
    }
    try {
      queue.put(item);
    } catch (InterruptedException e) {
      synchronized (submitted) {
        submitted.remove(diff.getMeta().getEpoch());
        WorkItem previous = lastSubmitted();
        acceptedHead = previous == null ? committedMeta() : previous.diff.getMeta();
      }
      Thread.currentThread().interrupt();
      throw new ArchivePersistenceException("Interrupted by archive queue backpressure", e);
    }
  }

  @Override
  public void revert(BlockSnapshotMeta meta) {
    WorkItem item;
    synchronized (submitted) {
      ensureOperational();
      item = lastSubmitted();
      if (item == null || !item.diff.getMeta().equals(meta)) {
        throw new ArchivePersistenceException("Archive reorg must remove the submitted head");
      }
      if (queue.remove(item)) {
        submitted.remove(meta.getEpoch());
        WorkItem previous = lastSubmitted();
        acceptedHead = previous == null ? committedMeta() : previous.diff.getMeta();
        item.completion.cancel(false);
        return;
      }
    }
    await(item);
    writer.revert(meta);
    synchronized (submitted) {
      submitted.remove(meta.getEpoch());
      WorkItem previous = lastSubmitted();
      acceptedHead = previous == null ? committedMeta() : previous.diff.getMeta();
    }
  }

  @Override
  public void awaitCommitted(long epoch) {
    List<WorkItem> required = new ArrayList<>();
    synchronized (submitted) {
      ensureOperational();
      submitted.forEach((candidate, item) -> {
        if (candidate <= epoch) {
          required.add(item);
        }
      });
    }
    required.forEach(this::await);
    ensureOperational();
  }

  /** Releases completed queue bookkeeping after the corresponding disk epoch is durable. */
  @Override
  public void releaseThrough(long epoch) {
    synchronized (submitted) {
      submitted.entrySet().removeIf(entry -> entry.getKey() <= epoch
          && entry.getValue().completion.isDone()
          && !entry.getValue().completion.isCompletedExceptionally());
    }
  }

  public int queueSize() {
    return queue.size();
  }

  private void run() {
    while (true) {
      WorkItem item;
      try {
        item = queue.take();
      } catch (InterruptedException e) {
        if (closed || fatalFailure != null) {
          return;
        }
        continue;
      }
      if (item.poison) {
        return;
      }
      try {
        writer.accept(item.diff);
        item.completion.complete(null);
      } catch (Throwable failure) {
        fatalFailure = failure;
        item.completion.completeExceptionally(failure);
        failQueued(failure);
        return;
      }
    }
  }

  private void failQueued(Throwable failure) {
    WorkItem item;
    while ((item = queue.poll()) != null) {
      if (!item.poison) {
        item.completion.completeExceptionally(failure);
      }
    }
  }

  private void await(WorkItem item) {
    try {
      item.completion.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ArchivePersistenceException("Interrupted while waiting for durable history", e);
    } catch (ExecutionException e) {
      throw new ArchivePersistenceException("Archive writer failed", e.getCause());
    } catch (java.util.concurrent.CancellationException e) {
      throw new ArchivePersistenceException("Archive history item was reverted", e);
    }
  }

  private void validateContinuity(BlockSnapshotMeta previous, BlockSnapshotMeta current) {
    if (current.getEpoch() != previous.getEpoch() + 1
        || current.getBlockNumber() != previous.getBlockNumber() + 1
        || !Arrays.equals(current.getParentHash(), previous.getBlockHash())) {
      throw new ArchivePersistenceException("Submitted archive metadata is not contiguous");
    }
  }

  private WorkItem lastSubmitted() {
    WorkItem last = null;
    for (WorkItem item : submitted.values()) {
      last = item;
    }
    return last;
  }

  private BlockSnapshotMeta committedMeta() {
    HistoryCommitMarker marker = writer.committedHead();
    return marker == null ? null : marker.getMeta();
  }

  private void ensureOperational() {
    if (fatalFailure != null) {
      throw new ArchivePersistenceException("Archive writer is in a failed state", fatalFailure);
    }
    if (closed) {
      throw new ArchivePersistenceException("Archive writer queue is closed");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (fatalFailure == null) {
      try {
        while (worker.isAlive()
            && !queue.offer(WorkItem.poison(), 100, java.util.concurrent.TimeUnit.MILLISECONDS)) {
          // Let the writer drain queued blocks before adding the terminal item.
        }
        worker.join();
      } catch (InterruptedException e) {
        worker.interrupt();
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while closing archive writer queue", e);
      }
    } else {
      worker.interrupt();
    }
    writer.close();
  }

  private static final class WorkItem {
    private final BlockReverseDiff diff;
    private final boolean poison;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    private WorkItem(BlockReverseDiff diff) {
      this.diff = diff;
      this.poison = false;
    }

    private WorkItem() {
      this.diff = null;
      this.poison = true;
    }

    private static WorkItem poison() {
      return new WorkItem();
    }
  }
}
