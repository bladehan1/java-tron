package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;

/** Request-owned point-read contract shared by legacy and common-checkpoint generations. */
public interface ArchivePointSnapshot extends Closeable {

  OldValue get(String dbName, byte[] physicalRawKey) throws IOException;

  default List<Entry> range(String dbName, KeyRange range, Limits limits) throws IOException {
    throw new UnsupportedOperationException(
        "Range reads are unavailable for this archive generation");
  }

  long getTargetBlock();

  long getPinnedBlock();

  byte[] getPinnedHash();

  void requirePinnedIdentity();
}
