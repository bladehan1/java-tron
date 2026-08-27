package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;

/** Physical history-body placement behind the authoritative history writer. */
interface HistoryBodyStore extends Closeable {

  HistoryLocation append(BlockReverseDiff diff) throws IOException;

  void sync() throws IOException;

  BlockReverseDiff read(HistoryLocation location) throws IOException;

  HistorySegmentStore.ScanResult getScanResult();

  void truncateAfter(HistoryLocation last) throws IOException;

  void truncateAfter(HistoryLocation last, long knownRecordCount) throws IOException;

  long getStartupScannedRecords();
}
