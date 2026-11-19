package org.tron.core.db2.archive.serving;

import java.io.IOException;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.serving.source.ServingHistorySource;
import org.tron.core.db2.archive.serving.source.ServingHistorySource.Committed;

/** Selects a bounded History read window; an oversized first block is always included. */
final class ServingBatchPlanner {

  private final ServingIndexService.Config config;

  ServingBatchPlanner(ServingIndexService.Config config) {
    this.config = config;
  }

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
    return new Selection(first, first + n - 1);
  }

  static final class Selection {
    private final long first;
    private final long last;

    Selection(long first, long last) {
      this.first = first;
      this.last = last;
    }

    long getFirst() {
      return first;
    }

    long getLast() {
      return last;
    }

    @Override
    public String toString() {
      return "RANGE(" + first + "," + last + ")";
    }
  }
}
