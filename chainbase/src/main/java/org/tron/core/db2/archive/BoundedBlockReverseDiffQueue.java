package org.tron.core.db2.archive;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Bounded, lossless hand-off queue. A full queue applies block-commit backpressure. */
public final class BoundedBlockReverseDiffQueue implements BlockReverseDiffSink {

  private final BlockingQueue<BlockReverseDiff> queue;

  public BoundedBlockReverseDiffQueue(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    queue = new ArrayBlockingQueue<>(capacity);
  }

  @Override
  public void accept(BlockReverseDiff diff) {
    Objects.requireNonNull(diff, "diff");
    try {
      queue.put(diff);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while applying archive queue backpressure", e);
    }
  }

  @Override
  public void revert(BlockSnapshotMeta meta) {
    queue.removeIf(diff -> diff.getMeta().equals(meta));
  }

  public BlockReverseDiff take() throws InterruptedException {
    return queue.take();
  }

  public int size() {
    return queue.size();
  }
}
