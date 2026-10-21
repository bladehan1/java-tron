package org.tron.core.db2.archive.history;

import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotImpl;

/** Block identity continuity across the registered state Snapshot layers. */
public final class BlockSnapshotContinuity {

  private BlockSnapshotContinuity() {
  }

  /** The caller holds the canonical Snapshot monitor while inspecting its layers. */
  public static void validate(List<Chainbase> dbs, BlockSnapshotMeta meta) {
    BlockSnapshotMeta previousMeta = null;
    for (Chainbase db : dbs) {
      if (!ArchiveStoreScope.isStateDatabase(db.getDbName())) {
        continue;
      }
      Snapshot head = db.getHead();
      if (!Snapshot.isImpl(head)) {
        continue;
      }
      Snapshot previous = head.getPrevious();
      BlockSnapshotMeta candidate = Snapshot.isImpl(previous)
          ? ((SnapshotImpl) previous).getBlockSnapshotMeta() : null;
      if (candidate != null && previousMeta != null && !previousMeta.equals(candidate)) {
        throw new IllegalStateException("Previous block metadata differs across state databases");
      }
      if (candidate != null) {
        previousMeta = candidate;
      }
    }
    if (previousMeta == null) {
      return;
    }
    if (meta.getEpoch() != previousMeta.getEpoch() + 1
        || meta.getBlockNumber() != previousMeta.getBlockNumber() + 1
        || !Arrays.equals(meta.getParentHash(), previousMeta.getBlockHash())) {
      throw new IllegalStateException(
          "Non-contiguous block snapshot metadata: previous=" + previousMeta + ", current=" + meta);
    }
  }
}
