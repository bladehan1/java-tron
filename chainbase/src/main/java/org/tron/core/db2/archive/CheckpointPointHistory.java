package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Optional;

/** Pinned reverse-history coverage consumed with a same-head Chainbase snapshot. */
interface CheckpointPointHistory extends AutoCloseable {

  Optional<OldValue> findOldValueAfter(String dbName, byte[] rawKey, long targetBlock)
      throws IOException;

  long getIndexedFrom();

  long getIndexedThrough();

  byte[] getHeadHash();

  @Override
  void close() throws IOException;
}
