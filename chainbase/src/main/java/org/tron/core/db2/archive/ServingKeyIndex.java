package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

/** One immutable serving generation for exact physical-key history lookup. */
public interface ServingKeyIndex extends Closeable {

  String getGenerationId();

  long getIndexedFrom();

  long getIndexedThrough();

  byte[] getHeadHash();

  byte[] getAuthoritativePrefixDigest();

  OptionalLong firstChangeAfter(String dbName, byte[] rawKey, long targetBlock,
      long upperBound) throws IOException;

  List<ServingKeyIndexGeneration.ChangedKey> changesInRange(String dbName,
      byte[] lowerInclusive, byte[] upperExclusive, long targetBlock, long upperBound,
      int maxChangedKeys) throws IOException;

  @Override
  default void close() throws IOException {
    // Pure in-memory generations own no external resource.
  }
}
