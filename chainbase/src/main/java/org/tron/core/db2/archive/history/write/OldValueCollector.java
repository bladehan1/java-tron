package org.tron.core.db2.archive.history.write;

import org.tron.core.db2.archive.BlockChangeView;
import org.tron.core.db2.archive.BlockReverseDiff;

/** Materializes a block reverse diff independently of its persistence format. */
public interface OldValueCollector {

  BlockReverseDiff collect(BlockChangeView view);
}
