package org.tron.core.db2.archive;

import org.tron.core.db2.archive.AccountAssetBlockProjectionBridge.PreparedBlockProjection;

/** Prepares one identity-bound reverse and forward projection from one immutable block view. */
@FunctionalInterface
public interface ArchiveBlockProjectionPreparer {

  PreparedBlockProjection prepare(BlockChangeView view);
}
