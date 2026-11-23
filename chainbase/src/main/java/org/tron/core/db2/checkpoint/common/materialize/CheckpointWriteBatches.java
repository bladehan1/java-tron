package org.tron.core.db2.checkpoint.common.materialize;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;
import org.tron.common.math.StrictMathWrapper;

/** Ordered logical-byte partitioning for unsynced checkpoint target writes. */
public final class CheckpointWriteBatches {

  private CheckpointWriteBatches() {
  }

  public static <T> List<List<T>> split(Iterable<T> mutations, ToLongFunction<T> encodedBytes,
      long maximumBytes) {
    List<List<T>> batches = new ArrayList<>();
    List<T> current = new ArrayList<>();
    long currentBytes = 0L;
    for (T mutation : mutations) {
      long mutationBytes = StrictMathWrapper.max(0L, encodedBytes.applyAsLong(mutation));
      if (maximumBytes > 0 && !current.isEmpty()
          && currentBytes + mutationBytes > maximumBytes) {
        batches.add(current);
        current = new ArrayList<>();
        currentBytes = 0L;
      }
      current.add(mutation);
      currentBytes += mutationBytes;
    }
    if (!current.isEmpty()) {
      batches.add(current);
    }
    return batches;
  }
}
