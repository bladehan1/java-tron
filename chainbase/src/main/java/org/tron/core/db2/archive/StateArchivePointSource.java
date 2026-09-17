package org.tron.core.db2.archive;

import java.io.IOException;

/** Reads one authoritative old value without replaying a complete five-lane bundle. */
interface StateArchivePointSource {

  OldValue readCommittedOldValue(String dbName, byte[] rawKey, long blockNumber)
      throws IOException;
}
