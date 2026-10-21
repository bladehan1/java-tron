package org.tron.core.db2.archive.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.tron.common.error.TronDBException;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.ArchiveWalBinding;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.DurableHistoryMarkerRangeEvidence;
import org.tron.core.db2.archive.history.write.BlockReverseDiffSink;
import org.tron.core.db2.archive.history.write.DurableBlockReverseDiffSink;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotImpl;

/**
 * History durability gate before the Chainbase checkpoint is published.
 * All calls are serialized by the owning SnapshotManager's canonical monitor.
 */
public final class ArchiveHistoryFlush {

  private Long submittedArchiveHistoryEpoch;

  public boolean hasPendingSubmission() {
    return submittedArchiveHistoryEpoch != null;
  }

  /** Reset only when the owner completes its flush, detaches the runtime or shuts down. */
  public void reset() {
    submittedArchiveHistoryEpoch = null;
  }

  /** Submit once, then retry durability and marker evidence against the same pending target. */
  public ArchiveWalBinding publish(List<Chainbase> dbs, int flushCount,
      BlockReverseDiffSink blockReverseDiffSink) {
    if (!(blockReverseDiffSink instanceof DurableBlockReverseDiffSink)) {
      throw new TronDBException("Archive sink cannot prove durable history before checkpoint");
    }
    Chainbase stateDatabase = dbs.stream()
        .filter(db -> ArchiveStoreScope.isStateDatabase(db.getDbName()))
        .findFirst()
        .orElseThrow(() -> new TronDBException("Archive mode has no registered state database"));
    Snapshot next = stateDatabase.getHead().getRoot();
    List<BlockReverseDiff> prepared = new ArrayList<>(flushCount);
    BlockSnapshotMeta last = null;
    for (int i = 0; i < flushCount; i++) {
      next = next.getNext();
      if (!Snapshot.isImpl(next)) {
        throw new TronDBException("Archive flush range is missing a snapshot layer");
      }
      BlockSnapshotMeta meta = ((SnapshotImpl) next).getBlockSnapshotMeta();
      if (meta == null) {
        throw new TronDBException("Archive flush range contains a layer without block metadata");
      }
      if (last != null && meta.getEpoch() != last.getEpoch() + 1) {
        throw new TronDBException("Archive flush range is not epoch-contiguous");
      }
      BlockReverseDiff reverseDiff = ((SnapshotImpl) next).getPreparedArchiveBlock();
      if (reverseDiff == null || !meta.equals(reverseDiff.getMeta())) {
        throw new TronDBException(
            "Archive flush range contains a layer without its matching prepared payload");
      }
      prepared.add(reverseDiff);
      last = meta;
    }
    if (last == null) {
      return null;
    }
    try {
      DurableBlockReverseDiffSink durableSink =
          (DurableBlockReverseDiffSink) blockReverseDiffSink;
      if (submittedArchiveHistoryEpoch == null) {
        durableSink.acceptAll(prepared);
        submittedArchiveHistoryEpoch = last.getEpoch();
      } else if (submittedArchiveHistoryEpoch.longValue() != last.getEpoch()) {
        throw new ArchivePersistenceException(
            "Submitted archive history target does not match the pending flush range");
      }
      durableSink.awaitCommitted(last.getEpoch());
      List<BlockSnapshotMeta> expectedMetas = prepared.stream()
          .map(BlockReverseDiff::getMeta).collect(Collectors.toList());
      DurableHistoryMarkerRangeEvidence evidence =
          durableSink.createMarkerRangeEvidence(prepared.size());
      return ArchiveWalBinding.fromMarkers(evidence.read(expectedMetas));
    } catch (RuntimeException e) {
      throw new TronDBException("Archive history durability gate failed", e);
    }
  }
}
