package org.tron.core.db2.archive.serving;

import java.io.IOException;
import java.util.List;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.serving.source.ServingHistorySource;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.Committed;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.CommittedBlock;

/**
 * Deterministic batch selection (ch04 §3.5.3, S3/S4). Budgets only choose batch boundaries; they
 * never fail a build: a single block that exceeds them is a SOLO batch.
 */
final class ServingBatchPlanner {

  private final ServingIndexService.Config config;

  ServingBatchPlanner(ServingIndexService.Config config) {
    this.config = config;
  }

  /**
   * Selects the next batch after {@code indexedThrough} from blocks.idx sizes only (no body
   * read). SOLO is decided by the accumulated byte count, never by the last size read.
   */
  Selection select(ServingHistorySource source, Committed view, long indexedThrough,
      long committedThrough) throws IOException {
    if (indexedThrough >= committedThrough) {
      return null;
    }
    long first = indexedThrough + 1;
    long end = StrictMathWrapper.min(committedThrough,
        indexedThrough + config.getMaxBlocks());
    long[] sizes = source.bundleBytes(view, first, end);
    int n = 0;
    long bytes = 0;
    for (long size : sizes) {
      if (n > 0 && bytes + size > config.getMaxSourceBytes()) {
        break;
      }
      n++;
      bytes += size;
    }
    if (n == 1 && bytes > config.getMaxSourceBytes()) {
      return new Selection(first, first, true);
    }
    return new Selection(first, first + n - 1, false);
  }

  /** Longest prefix within the plan budgets; 0 means the first block alone exceeds them. */
  int planPrefix(List<CommittedBlock> blocks) {
    long entries = 0;
    long input = 0;
    int k = 0;
    for (CommittedBlock block : blocks) {
      entries += block.getEntries();
      input += block.getPlanInputBytes();
      if (entries > config.getMaxPlanEntries() || input > config.getMaxPlanInputBytes()) {
        break;
      }
      k++;
    }
    return k;
  }

  static final class Selection {
    private final long first;
    private final long last;
    private final boolean solo;

    Selection(long first, long last, boolean solo) {
      this.first = first;
      this.last = last;
      this.solo = solo;
    }

    long getFirst() {
      return first;
    }

    long getLast() {
      return last;
    }

    boolean isSolo() {
      return solo;
    }

    @Override
    public String toString() {
      return (solo ? "SOLO(" + first : "RANGE(" + first + "," + last) + ")";
    }
  }
}
