package org.tron.core.db2.archive.recovery.wal;

import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Fault construction kept beside the binding's package-private constructor. */
public final class ArchiveWalBindingTestSupport {

  private ArchiveWalBindingTestSupport() {
  }

  public static String checkpointDatabase() {
    return ArchiveWalBinding.CHECKPOINT_DATABASE;
  }

  public static ArchiveWalBinding withHistoryRefs(ArchiveWalBinding original, byte[] refs) {
    return new ArchiveWalBinding(original.getFirst(), original.getLast(),
        original.getPredecessorEpoch(), original.getPredecessorHash(), original.getBatchDigest(),
        original.getStoreScopeDigest(), refs, original.getBlockIndexRefsDigest());
  }

  public static ArchiveWalBinding create(BlockSnapshotMeta first, BlockSnapshotMeta last,
      long predecessorEpoch, byte[] predecessorHash, byte[] batchDigest, byte[] scopeDigest,
      byte[] historyRefs, byte[] indexRefs) {
    return new ArchiveWalBinding(first, last, predecessorEpoch, predecessorHash, batchDigest,
        scopeDigest, historyRefs, indexRefs);
  }
}
