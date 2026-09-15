package org.tron.core.db2.archive;

import java.io.IOException;

/** Runs one latest-state acquisition inside the canonical SnapshotManager state boundary. */
@FunctionalInterface
public interface ArchiveStateBarrier {

  void run(ArchiveStateAction action) throws IOException;

  @FunctionalInterface
  interface ArchiveStateAction {

    void run() throws IOException;
  }
}
