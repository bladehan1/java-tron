package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.tron.core.db2.archive.StateArchiveServingIndexBuildCoordinatorV3.BuildProgress;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Single owner with a constant-size committed watermark mailbox, never a queue of bodies. */
final class StateArchiveServingWorkerV3 implements AutoCloseable {
  static final long MAX_SOURCE_BYTES = 32L * 1024 * 1024;
  private final CoordinatorFactory factory;
  private final StateArchiveFiveLaneSegmentWriterV3 source;
  private final Object dispatch = new Object();
  private final Thread thread;
  private final Runnable beforeBuild;
  private final long tailDelayNanos;
  private CommonCheckpointTarget requested;
  private CommonCheckpointTarget completed;
  private CommonCheckpointTarget handoff;
  private boolean live;
  private boolean closing;
  private long pendingSince;
  private StateArchiveServingIndexBuildCoordinatorV3 coordinator;
  private StateArchiveServingIndexBuildCoordinatorV3.LiveServingIndexer liveHandle;
  private volatile IOException failure;
  private volatile BuildProgress progress;

  StateArchiveServingWorkerV3(CoordinatorFactory factory,
      StateArchiveFiveLaneSegmentWriterV3 source, Runnable beforeBuild) {
    this(factory, source, beforeBuild, 15_000);
  }

  StateArchiveServingWorkerV3(CoordinatorFactory factory,
      StateArchiveFiveLaneSegmentWriterV3 source, Runnable beforeBuild, long tailDelayMillis) {
    this.factory = factory;
    this.source = source;
    this.beforeBuild = beforeBuild;
    if (tailDelayMillis < 0 || tailDelayMillis > 60_000) {
      throw new IllegalArgumentException("Invalid serving tail delay");
    }
    tailDelayNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(tailDelayMillis);
    progress = new BuildProgress(StateArchiveServingIndexBuildCoordinatorV3.Mode.BULK_CATCH_UP,
        -1, -1, 0, 0);
    thread = new Thread(this::run, "state-archive-serving-v3");
    thread.setDaemon(true);
    thread.start();
  }

  void offer(CommonCheckpointTarget target) throws IOException {
    synchronized (dispatch) {
      synchronized (this) {
        requireHealthy();
        if (requested != null && !requested.equals(target)
            && (target.getFirstBlock().getBlockNumber()
                != requested.getLastBlock().getBlockNumber() + 1
                || !java.util.Arrays.equals(target.getFirstBlock().getParentHash(),
                    requested.getLastBlock().getBlockHash()))) {
          IOException gap = new IOException("Serving notification is not a committed successor");
          fail(gap);
          throw gap;
        }
        requested = target;
        ServingIndexTiming.progress(status());
        if (pendingSince == 0) {
          pendingSince = System.nanoTime();
        }
        notifyAll();
      }
      if (live && !target.equals(completed)) {
        try {
          beforeBuild.run();
          List<BlockReverseDiff> batch = readSource(
              progress.getIndexedThrough(), target.getLastBlock().getBlockNumber(),
              MAX_SOURCE_BYTES);
          progress = liveHandle.indexNow(batch, target);
          ServingIndexTiming.progress(status());
          synchronized (this) {
            completed = target;
            pendingSince = 0;
          }
        } catch (IOException | RuntimeException error) {
          progress = coordinator.status();
          ServingIndexTiming.progress(status());
          IOException cause = new IOException("Foreground serving index failed", error);
          fail(cause);
          throw cause;
        }
      }
    }
  }

  void completeInitialSync(CommonCheckpointTarget target) throws IOException {
    synchronized (dispatch) {
      synchronized (this) {
        requireHealthy();
        if (requested == null || !requested.equals(target) || live || handoff != null) {
          throw new IOException("Serving handoff differs from committed mailbox boundary");
        }
        handoff = target;
        notifyAll();
        await(target, true);
      }
      try {
        thread.join();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        IOException cause = new IOException("Interrupted joining serving bulk owner", interrupted);
        fail(cause);
        throw cause;
      }
    }
  }

  synchronized BuildProgress status() {
    BuildProgress snapshot = progress;
    long committed = requested == null ? snapshot.getCommittedThrough()
        : requested.getLastBlock().getBlockNumber();
    return new BuildProgress(closing ? StateArchiveServingIndexBuildCoordinatorV3.Mode.CLOSED
        : failure != null ? StateArchiveServingIndexBuildCoordinatorV3.Mode.CATCH_UP_REQUIRED
        : handoff != null && !live
            ? StateArchiveServingIndexBuildCoordinatorV3.Mode.HANDOFF_DRAINING : snapshot.getMode(),
        snapshot.getIndexedThrough(), committed, snapshot.getPendingBlocks(),
        snapshot.getBuildSequence(), snapshot.getIndexedFrom());
  }

  IOException failure() {
    return failure;
  }

  private void await(CommonCheckpointTarget target, boolean requireLive) throws IOException {
    while (!target.equals(completed) || requireLive && !live) {
      requireHealthy();
      try {
        wait();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        IOException cancelled = new IOException("Interrupted waiting for serving publication",
            interrupted);
        fail(cancelled);
        throw cancelled;
      }
    }
    requireHealthy();
  }

  private void requireHealthy() throws IOException {
    if (failure != null) {
      throw new IOException("Serving index requires recovery", failure);
    }
    if (closing) {
      throw new IOException("Serving worker is closed");
    }
  }

  private void run() {
    try {
      coordinator = factory.open();
      progress = coordinator.status();
      if (coordinator.getIndexedFrom() >= 0
          && coordinator.getIndexedFrom() != source.getHistoryStartBlock() - 1) {
        throw new IOException("Serving indexedFrom differs from available history baseline");
      }
      while (true) {
        CommonCheckpointTarget target;
        boolean enterLive;
        synchronized (this) {
          while (!closing && failure == null && (requested == null || requested.equals(completed))
              && (handoff == null || live)) {
            wait();
          }
          if (closing || failure != null) {
            return;
          }
          // Bound tail age without forcing a tiny publication every second during sync.
          if (!live && handoff == null
              && requested.getLastBlock().getBlockNumber() - progress.getIndexedThrough() < 1_000) {
            long remaining = tailDelayNanos - (System.nanoTime() - pendingSince);
            if (remaining > 0) {
              wait(Math.max(1, remaining / 1_000_000L));
              continue;
            }
          }
          target = requested;
          enterLive = handoff != null && !live;
        }
        beforeBuild.run();
        if (liveHandle == null) {
          long cursor = progress.getIndexedThrough();
          if (cursor < 0) {
            cursor = source.getHistoryStartBlock() - 1;
          }
          long end = target.getLastBlock().getBlockNumber();
          if (cursor > end || cursor < 0) {
            throw new IOException("Serving durable boundary is outside committed history");
          }
          if (cursor == end) {
            coordinator.recoverCommittedRange(Collections.emptyList(), target, true);
          }
          int readBlocks = 1_000;
          while (cursor < end) {
            synchronized (this) {
              if (closing || failure != null) {
                return;
              }
            }
            long batchEnd = Math.min(end, cursor + readBlocks);
            List<BlockReverseDiff> batch;
            while (true) {
              try {
                batch = readSource(cursor, batchEnd, MAX_SOURCE_BYTES);
                break;
              } catch (StateArchiveFiveLaneSegmentWriterV3.ServingReadBudgetException tooLarge) {
                if (batchEnd == cursor + 1) {
                  throw tooLarge;
                }
                batchEnd = cursor + (batchEnd - cursor) / 2;
                readBlocks = Math.toIntExact(batchEnd - cursor);
              }
            }
            coordinator.recoverCommittedRange(batch, target, batchEnd == end);
            coordinator.flushRecoveryBatch();
            progress = coordinator.status();
            ServingIndexTiming.progress(status());
            cursor = batchEnd;
          }
          if (enterLive) {
            liveHandle = coordinator.completeInitialSync(target);
          }
        } else if (!target.equals(completed)) {
          List<BlockReverseDiff> batch = readSource(
              progress.getIndexedThrough(), target.getLastBlock().getBlockNumber(),
              MAX_SOURCE_BYTES);
          liveHandle.indexNow(batch, target);
        }
        progress = coordinator.status();
        synchronized (this) {
          completed = target;
          if (target.equals(requested)) {
            pendingSince = 0;
          }
          live = failure == null && liveHandle != null;
          ServingIndexTiming.progress(status());
          notifyAll();
          if (live) {
            // No further background writes. Dispatch now owns the same coordinator and DB.
            return;
          }
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      fail(new IOException("Serving owner interrupted", interrupted));
    } catch (IOException | RuntimeException buildFailure) {
      if (coordinator != null) {
        progress = coordinator.status();
      }
      fail(new IOException("Serving build failed", buildFailure));
    } finally {
      try {
        if (coordinator != null && liveHandle == null) {
          coordinator.close();
        }
      } catch (IOException closeFailure) {
        fail(closeFailure);
      }
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private List<BlockReverseDiff> readSource(long from, long through, long maxBytes)
      throws IOException {
    long started = System.nanoTime();
    try {
      return source.readCommittedDiffs(from, through, maxBytes);
    } finally {
      ServingIndexTiming.sourceCall(started);
    }
  }

  private synchronized void fail(IOException cause) {
    failure = cause;
    live = false;
    progress = new BuildProgress(StateArchiveServingIndexBuildCoordinatorV3.Mode.CATCH_UP_REQUIRED,
        progress.getIndexedThrough(), requested == null ? -1
            : requested.getLastBlock().getBlockNumber(), 0, progress.getBuildSequence(),
        progress.getIndexedFrom());
    ServingIndexTiming.progress(status());
    org.slf4j.LoggerFactory.getLogger("DB").error("Archive serving owner degraded", cause);
    notifyAll();
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      closing = true;
      notifyAll();
    }
    boolean interrupted = false;
    while (thread.isAlive()) {
      try {
        thread.join();
      } catch (InterruptedException retry) {
        interrupted = true;
      }
    }
    synchronized (dispatch) {
      if (coordinator != null) {
        coordinator.close();
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted closing serving owner; owner has terminated");
    }
  }

  interface CoordinatorFactory {
    StateArchiveServingIndexBuildCoordinatorV3 open() throws IOException;
  }
}
