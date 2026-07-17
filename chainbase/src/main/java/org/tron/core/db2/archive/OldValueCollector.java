package org.tron.core.db2.archive;

/** Materializes a block reverse diff independently of its persistence format. */
public interface OldValueCollector {

  BlockReverseDiff collect(BlockChangeView view);
}
