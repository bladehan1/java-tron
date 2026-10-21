package org.tron.core.db2.archive.checkpoint;

import java.io.IOException;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Publishes derived serving authority for one H/WAL-proven committed prefix. */
@FunctionalInterface
public interface ArchiveCommittedPrefixPublisher {

  void publish(BlockSnapshotMeta target) throws IOException;
}
