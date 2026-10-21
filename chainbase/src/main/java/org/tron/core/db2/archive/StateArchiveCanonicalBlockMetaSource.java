package org.tron.core.db2.archive;

import java.io.IOException;

/** Canonical Block Store metadata required to replay compact V5 archive frames. */
@FunctionalInterface
public interface StateArchiveCanonicalBlockMetaSource {

  BlockSnapshotMeta load(long blockNumber) throws IOException;
}
