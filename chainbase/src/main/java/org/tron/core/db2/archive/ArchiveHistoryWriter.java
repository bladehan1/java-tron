package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.tron.core.db2.archive.HistoryIndexStore.ScannedIndexRecord;
import org.tron.core.db2.archive.HistorySegmentStore.ScannedRecord;

/**
 * Ordered history body/index/marker writer. A marker is the only reader-visible commit boundary.
 */
public final class ArchiveHistoryWriter implements DurableBlockReverseDiffSink, Closeable {

  private final HistorySegmentStore bodies;
  private final HistoryIndexStore index;
  private final HistoryCommitStore commits;
  private final List<String> participatingDatabases;
  private final DurabilityHook hook;

  public ArchiveHistoryWriter(Path archiveDirectory, long maxSegmentSize,
      Set<String> participatingDatabases) throws IOException {
    this(archiveDirectory, maxSegmentSize, participatingDatabases, (stage, meta) -> { });
  }

  ArchiveHistoryWriter(Path archiveDirectory, long maxSegmentSize,
      Set<String> participatingDatabases, DurabilityHook hook) throws IOException {
    this.bodies = new HistorySegmentStore(archiveDirectory, new BlockHistoryCodec(),
        maxSegmentSize);
    this.index = new HistoryIndexStore(archiveDirectory, new HistoryIndexCodec());
    this.commits = new HistoryCommitStore(archiveDirectory, new HistoryCommitMarkerCodec());
    this.participatingDatabases = new ArrayList<>(participatingDatabases);
    this.participatingDatabases.sort(String::compareTo);
    this.hook = hook;
    recoverPreparedSuffix();
  }

  @Override
  public synchronized void accept(BlockReverseDiff diff) {
    try {
      validateNext(diff.getMeta());
      hook.before(Stage.APPEND_BODY, diff.getMeta());
      HistoryLocation bodyLocation = bodies.append(diff);
      hook.before(Stage.APPEND_INDEX, diff.getMeta());
      HistoryIndexRecord indexRecord = HistoryIndexRecord.from(diff, bodyLocation);
      HistoryIndexLocation indexLocation = index.append(indexRecord);
      hook.before(Stage.SYNC_BODY, diff.getMeta());
      bodies.sync();
      hook.before(Stage.SYNC_INDEX, diff.getMeta());
      index.sync();
      hook.before(Stage.COMMIT_MARKER, diff.getMeta());
      HistoryCommitMarker head = commits.head();
      long previousEpoch = head == null ? diff.getMeta().getEpoch() - 1
          : head.getMeta().getEpoch();
      commits.commit(new HistoryCommitMarker(diff.getMeta(), previousEpoch, bodyLocation,
          indexLocation, batchId(), participatingDatabases));
    } catch (IOException | RuntimeException e) {
      handleWriteFailure(diff.getMeta(), e);
    }
  }

  @Override
  public synchronized void revert(BlockSnapshotMeta meta) {
    try {
      HistoryCommitMarker head = commits.head();
      if (head != null && head.getMeta().equals(meta)) {
        commits.removeHead(meta);
        HistoryCommitMarker previous = commits.head();
        index.truncateAfter(previous == null ? null : previous.getIndexLocation());
        bodies.truncateAfter(previous == null ? null : previous.getHistoryLocation());
        return;
      }

      ScannedRecord bodyHead = last(bodies.getScanResult().getRecords());
      ScannedIndexRecord indexHead = last(index.getScanResult().getRecords());
      if (bodyHead != null && bodyHead.getDiff().getMeta().equals(meta)) {
        HistoryCommitMarker committed = commits.head();
        index.truncateAfter(committed == null ? null : committed.getIndexLocation());
        bodies.truncateAfter(committed == null ? null : committed.getHistoryLocation());
        return;
      }
      if (indexHead != null && indexHead.getRecord().getMeta().equals(meta)) {
        throw new ArchivePersistenceException("Index/body archive heads differ during revert");
      }
      throw new ArchivePersistenceException("Archive revert does not target the current head");
    } catch (IOException e) {
      throw new ArchivePersistenceException("Failed to revert archive history", e);
    }
  }

  public synchronized HistoryCommitMarker committedHead() {
    return commits.head();
  }

  @Override
  public synchronized void awaitCommitted(long epoch) {
    HistoryCommitMarker head = commits.head();
    if (head == null || head.getMeta().getEpoch() < epoch) {
      throw new ArchivePersistenceException("Archive history has not committed epoch " + epoch);
    }
  }

  @Override
  public void releaseThrough(long epoch) {
    // The synchronous writer has no queue bookkeeping to release.
  }

  public synchronized BlockReverseDiff readCommitted(long epoch) {
    HistoryCommitMarker marker = commits.get(epoch);
    if (marker == null) {
      throw new IllegalArgumentException("History epoch is not committed: " + epoch);
    }
    try {
      HistoryIndexRecord indexRecord = index.read(marker.getIndexLocation());
      validateMarkerReferences(marker, indexRecord);
      BlockReverseDiff diff = bodies.read(marker.getHistoryLocation());
      if (!marker.getMeta().equals(diff.getMeta())) {
        throw new ArchivePersistenceException("Marker does not match history body metadata");
      }
      return diff;
    } catch (IOException e) {
      throw new ArchivePersistenceException("Failed to read committed archive history", e);
    }
  }

  private void validateNext(BlockSnapshotMeta meta) {
    HistoryCommitMarker head = commits.head();
    if (head == null) {
      return;
    }
    BlockSnapshotMeta previous = head.getMeta();
    if (meta.getEpoch() != previous.getEpoch() + 1
        || meta.getBlockNumber() != previous.getBlockNumber() + 1
        || !Arrays.equals(meta.getParentHash(), previous.getBlockHash())) {
      throw new ArchivePersistenceException("Archive block metadata is not contiguous");
    }
  }

  private void handleWriteFailure(BlockSnapshotMeta meta, Exception failure) {
    if (commits.mayContain(meta.getEpoch())) {
      throw new ArchivePersistenceException(
          "History marker may be durable; refusing to roll back committed archive", failure);
    }
    try {
      HistoryCommitMarker committed = commits.head();
      index.truncateAfter(committed == null ? null : committed.getIndexLocation());
      bodies.truncateAfter(committed == null ? null : committed.getHistoryLocation());
    } catch (IOException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
    throw new ArchivePersistenceException("Failed to persist archive history", failure);
  }

  private void recoverPreparedSuffix() throws IOException {
    HistorySegmentStore.ScanResult bodyScan = bodies.getScanResult();
    HistoryIndexStore.ScanResult indexScan = index.getScanResult();
    int committedCount = commits.getMarkers().size();
    if (bodyScan.getRecords().size() < committedCount
        || indexScan.getRecords().size() < committedCount) {
      throw new ArchivePersistenceException("Committed marker references missing body/index data");
    }
    for (int i = 0; i < committedCount; i++) {
      HistoryCommitMarker marker = commits.getMarkers().get(i);
      ScannedRecord bodyRecord = bodyScan.getRecords().get(i);
      ScannedIndexRecord indexRecord = indexScan.getRecords().get(i);
      if (!marker.getMeta().equals(bodyRecord.getDiff().getMeta())
          || !marker.getMeta().equals(indexRecord.getRecord().getMeta())) {
        throw new ArchivePersistenceException("Committed history metadata does not align");
      }
      validateMarkerReferences(marker, indexRecord.getRecord());
      if (!same(marker.getHistoryLocation(), bodyRecord.getLocation())
          || !same(marker.getIndexLocation(), indexRecord.getLocation())) {
        throw new ArchivePersistenceException("Commit marker location/digest mismatch");
      }
    }

    if (bodyScan.getInvalidTail() != null) {
      if (bodyScan.getRecords().size() < committedCount) {
        throw new ArchivePersistenceException("Committed history body is corrupt");
      }
      bodies.truncateInvalidTail();
    }
    if (indexScan.getInvalidTailOffset() != null) {
      if (indexScan.getRecords().size() < committedCount) {
        throw new ArchivePersistenceException("Committed history index is corrupt");
      }
      index.truncateInvalidTail();
    }
    HistoryCommitMarker head = commits.head();
    index.truncateAfter(head == null ? null : head.getIndexLocation());
    bodies.truncateAfter(head == null ? null : head.getHistoryLocation());
  }

  private void validateMarkerReferences(HistoryCommitMarker marker,
      HistoryIndexRecord indexRecord) {
    if (!marker.getMeta().equals(indexRecord.getMeta())
        || !same(marker.getHistoryLocation(), indexRecord.getHistoryLocation())) {
      throw new ArchivePersistenceException("Marker does not match authoritative index delta");
    }
  }

  private static boolean same(HistoryLocation left, HistoryLocation right) {
    return left.getSegmentId() == right.getSegmentId()
        && left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && left.getBodyChecksum() == right.getBodyChecksum()
        && Arrays.equals(left.getBodyDigest(), right.getBodyDigest());
  }

  private static boolean same(HistoryIndexLocation left, HistoryIndexLocation right) {
    return left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && Arrays.equals(left.getDigest(), right.getDigest());
  }

  private static byte[] batchId() {
    UUID uuid = UUID.randomUUID();
    return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits()).array();
  }

  private static <T> T last(List<T> values) {
    return values.isEmpty() ? null : values.get(values.size() - 1);
  }

  @Override
  public synchronized void close() throws IOException {
    IOException failure = null;
    try {
      index.close();
    } catch (IOException e) {
      failure = e;
    }
    try {
      bodies.close();
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    commits.close();
    if (failure != null) {
      throw failure;
    }
  }

  enum Stage {
    APPEND_BODY,
    APPEND_INDEX,
    SYNC_BODY,
    SYNC_INDEX,
    COMMIT_MARKER
  }

  interface DurabilityHook {
    void before(Stage stage, BlockSnapshotMeta meta) throws IOException;
  }
}
