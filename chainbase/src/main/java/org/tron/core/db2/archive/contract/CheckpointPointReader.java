package org.tron.core.db2.archive.contract;

import java.io.IOException;
import java.util.Optional;
import org.tron.core.db2.archive.OldValue;

/** Short-lived reverse-history coverage used for one lock-free key access. */
public interface CheckpointPointReader extends AutoCloseable {

  Optional<OldValue> findOldValueAfter(String dbName, byte[] rawKey, long targetBlock)
      throws IOException;

  long getIndexedFrom();

  long getIndexedThrough();

  byte[] getHeadHash();

  @Override
  void close() throws IOException;
}
