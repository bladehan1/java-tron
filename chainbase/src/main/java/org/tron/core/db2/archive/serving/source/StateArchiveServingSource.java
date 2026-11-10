package org.tron.core.db2.archive.serving.source;

import java.io.IOException;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;

/** Committed reverse-diff source consumed by the independent serving-index lifecycle. */
public interface StateArchiveServingSource {

  long getHistoryStartBlock();

  List<BlockReverseDiff> readCommittedDiffs(long fromExclusive, long through,
      long maxEncodedBytes) throws IOException;

  class ReadBudgetException extends IOException {

    public ReadBudgetException() {
      super("Serving source encoded-byte budget exceeded");
    }
  }
}
