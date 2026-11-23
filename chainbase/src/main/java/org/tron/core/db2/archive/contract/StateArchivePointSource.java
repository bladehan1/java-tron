package org.tron.core.db2.archive.contract;

import java.io.IOException;
import org.tron.core.db2.archive.OldValue;

/** Reads one authoritative old value without replaying a complete five-lane bundle. */
public interface StateArchivePointSource {

  OldValue readCommittedOldValue(String dbName, byte[] rawKey, long blockNumber)
      throws IOException;
}
