package org.tron.core.db2.archive;

import java.io.IOException;

/** Publishes derived serving authority for one H/WAL-proven committed prefix. */
@FunctionalInterface
public interface ArchiveCommittedPrefixPublisher {

  void publish(BlockSnapshotMeta target) throws IOException;
}
