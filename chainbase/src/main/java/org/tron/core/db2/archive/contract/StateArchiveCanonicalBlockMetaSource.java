package org.tron.core.db2.archive.contract;

import java.io.IOException;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Canonical Block Store metadata required to replay compact V5 archive frames. */
@FunctionalInterface
public interface StateArchiveCanonicalBlockMetaSource {

  BlockSnapshotMeta load(long blockNumber) throws IOException;
}
