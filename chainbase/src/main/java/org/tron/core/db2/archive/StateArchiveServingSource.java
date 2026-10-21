package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.List;

/** Committed reverse-diff source consumed by the independent serving-index lifecycle. */
interface StateArchiveServingSource {

  long getHistoryStartBlock();

  List<BlockReverseDiff> readCommittedDiffs(long fromExclusive, long through,
      long maxEncodedBytes) throws IOException;

  class ReadBudgetException extends IOException {

    ReadBudgetException() {
      super("Serving source encoded-byte budget exceeded");
    }
  }
}
