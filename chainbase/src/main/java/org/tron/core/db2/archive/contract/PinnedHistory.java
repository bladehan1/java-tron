package org.tron.core.db2.archive.contract;

import java.io.Closeable;
import java.io.IOException;
import org.tron.core.db2.archive.OldValue;

/** Request-owned authoritative reads and identity for one pinned history prefix. */
public interface PinnedHistory extends Closeable {
  long getIndexedFrom();

  long getIndexedThrough();

  byte[] getHeadHash();

  byte[] getAuthoritativePrefixDigest();

  OldValue read(String dbName, byte[] physicalRawKey, long firstChangeBlock)
      throws IOException;
}
