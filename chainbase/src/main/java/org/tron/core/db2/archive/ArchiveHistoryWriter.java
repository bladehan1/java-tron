package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

/**
 * Ordered history body/index/marker writer. A marker is the durable history boundary H; reader
 * visibility R is a separate recovery authority and is not yet integrated into this prototype.
 */
public final class ArchiveHistoryWriter
    implements DurableBlockReverseDiffSink, CommittedHistoryAuthority, Closeable {

  static final int MAX_RESTART_TAIL_RECORDS = 1024;

  private final HistorySegmentStore bodies;
  private final HistoryIndexStore index;
  private final HistoryCommitStore commits;
  private final AccountChangeIndex accountIndex;
  private final ArchiveBaseManifest manifest;
  private final Path archiveDirectory;
  private final HistoryCommitMarkerCodec commitCodec;
  private final List<String> participatingDatabases;
  private final DurabilityHook hook;

  public ArchiveHistoryWriter(Path archiveDirectory, long maxSegmentSize,
      Set<String> participatingDatabases) throws IOException {
    this(archiveDirectory, maxSegmentSize, participatingDatabases, (stage, meta) -> { });
  }

  ArchiveHistoryWriter(Path archiveDirectory, long maxSegmentSize,
      Set<String> participatingDatabases, DurabilityHook hook) throws IOException {
    this.archiveDirectory = archiveDirectory;
    this.commitCodec = new HistoryCommitMarkerCodec();
    this.participatingDatabases = new ArrayList<>(participatingDatabases);
    this.participatingDatabases.sort(String::compareTo);
    this.manifest = new ArchiveBaseManifest(archiveDirectory, this.participatingDatabases);
    new ArchiveTruncationRecovery(archiveDirectory, maxSegmentSize).recover();
    ArchiveRestartCheckpoint checkpoint = ArchiveRestartCheckpoint.load(archiveDirectory,
        commitCodec);
    this.bodies = new HistorySegmentStore(archiveDirectory, new BlockHistoryCodec(),
        maxSegmentSize, checkpoint);
    this.index = new HistoryIndexStore(archiveDirectory, new HistoryIndexCodec(), checkpoint);
    this.commits = new HistoryCommitStore(archiveDirectory, commitCodec, checkpoint);
    this.hook = hook;
    recoverPreparedSuffix();
    persistRestartCheckpoint();
    if (commits.head() != null) {
      manifest.ensureBase(commits.get(commits.firstEpoch()).getMeta());
    }
    this.accountIndex = new AccountChangeIndex(archiveDirectory.resolve("account-change-index"));
    try {
      catchUpAccountIndex();
    } catch (IOException | RuntimeException failure) {
      closeAfterFailedConstruction(failure);
      throw failure;
    }
  }

  @Override
  public synchronized void accept(BlockReverseDiff diff) {
    acceptAll(java.util.Collections.singletonList(diff));
  }

  @Override
  public synchronized void acceptAll(List<BlockReverseDiff> diffs) {
    if (diffs.isEmpty()) {
      return;
    }
    if (commits.head() == null) {
      try {
        manifest.ensureBase(diffs.get(0).getMeta());
      } catch (IOException failure) {
        throw new ArchivePersistenceException("Failed to establish archive base manifest", failure);
      }
    }
    BlockSnapshotMeta previous = commits.head() == null ? null : commits.head().getMeta();
    for (BlockReverseDiff diff : diffs) {
      validateNext(previous, diff.getMeta());
      previous = diff.getMeta();
    }
    try {
      for (int start = 0; start < diffs.size(); start += MAX_RESTART_TAIL_RECORDS) {
        int end = Math.min(diffs.size(), start + MAX_RESTART_TAIL_RECORDS);
        persistChunk(diffs.subList(start, end));
      }
      accountIndex.apply(diffs);
    } catch (IOException | RuntimeException e) {
      handleWriteFailure(diffs.get(diffs.size() - 1).getMeta(), e);
    }
  }

  @Override
  public synchronized void revert(BlockSnapshotMeta meta) {
    try {
      HistoryCommitMarker head = commits.head();
      if (head != null && head.getMeta().equals(meta)) {
        BlockReverseDiff reverted = readCommitted(meta.getEpoch());
        HistoryCommitMarker previous = commits.get(meta.getEpoch() - 1);
        accountIndex.revert(reverted, previous == null ? null : previous.getMeta());
        commits.removeHead(meta);
        persistRestartCheckpoint();
        previous = commits.head();
        index.truncateAfter(previous == null ? null : previous.getIndexLocation(), commits.size());
        bodies.truncateAfter(previous == null ? null : previous.getHistoryLocation(),
            commits.size());
        return;
      }

      HistorySegmentStore.ScannedRecord bodyHead = bodies.getScanResult().getHead();
      HistoryIndexStore.ScannedIndexRecord indexHead = index.getScanResult().getHead();
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
    return head();
  }

  synchronized HistoryCommitMarker committedMarker(long epoch) {
    HistoryCommitMarker marker = get(epoch);
    if (marker == null) {
      return null;
    }
    return commitCodec.decode(commitCodec.encode(marker));
  }

  @Override
  public synchronized HistoryCommitMarker head() {
    return commits.head();
  }

  @Override
  public synchronized HistoryCommitMarker get(long epoch) {
    return commits.get(epoch);
  }

  @Override
  public synchronized long firstEpoch() {
    return commits.firstEpoch();
  }

  @Override
  public synchronized HistoryCoverage coverage() {
    return commits.coverage();
  }

  @Override
  public synchronized void awaitCommitted(long epoch) {
    HistoryCommitMarker head = commits.head();
    if (head == null || head.getMeta().getEpoch() < epoch) {
      throw new ArchivePersistenceException("Archive history has not committed epoch " + epoch);
    }
  }

  @Override
  public DurableHistoryMarkerRangeEvidence createMarkerRangeEvidence(int maxMarkers) {
    return new DurableHistoryMarkerRangeEvidence(this, maxMarkers);
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

  public synchronized OldValue readAccountAt(long targetBlock, byte[] address,
      byte[] accountAtCommittedHead) {
    HistoryCommitMarker head = commits.head();
    if (head == null) {
      throw new IllegalStateException("State archive has no committed history");
    }
    long base = commits.firstEpoch() - 1;
    if (targetBlock < base || targetBlock > head.getMeta().getEpoch()) {
      throw new IllegalArgumentException("Account query is outside archive coverage");
    }
    try {
      java.util.OptionalLong changed = accountIndex.firstChangeAfter(address, targetBlock,
          head.getMeta().getEpoch());
      if (!changed.isPresent()) {
        return OldValue.fromNullable(accountAtCommittedHead);
      }
      BlockReverseDiff diff = readCommitted(changed.getAsLong());
      return findOldValue(diff, HistoricalAccountBalanceReader.ACCOUNT_DATABASE, address);
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Failed to query historical account", failure);
    }
  }

  public synchronized BlockSnapshotMeta committedHeadMeta() {
    HistoryCommitMarker marker = commits.head();
    return marker == null ? null : marker.getMeta();
  }

  /** Builds one immutable persistent serving generation from the current committed prefix H. */
  public synchronized PersistentServingKeyIndexGeneration buildServingGeneration(
      Path shadowDirectory, String generationId) throws IOException {
    return buildServingGeneration(shadowDirectory, generationId, new byte[32]);
  }

  public synchronized PersistentServingKeyIndexGeneration buildServingGeneration(
      Path shadowDirectory, String generationId, byte[] latestSourceIdentityDigest)
      throws IOException {
    HistoryCommitMarker first = commits.head() == null ? null : commits.get(commits.firstEpoch());
    if (first == null) {
      throw new IllegalStateException("Cannot build a serving generation from empty history");
    }
    long firstEpoch = commits.firstEpoch();
    long lastEpoch = commits.head().getMeta().getEpoch();
    Iterable<HistoryCommitMarker> committed = () -> new Iterator<HistoryCommitMarker>() {
      private long nextEpoch = firstEpoch;

      @Override
      public boolean hasNext() {
        return nextEpoch <= lastEpoch;
      }

      @Override
      public HistoryCommitMarker next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return commits.get(nextEpoch++);
      }
    };
    return PersistentServingKeyIndexGeneration.build(shadowDirectory, generationId,
        firstEpoch - 1, first.getMeta().getParentHash(), committed, index::read,
        participatingDatabases, latestSourceIdentityDigest);
  }

  long getStartupScannedRecords() {
    return bodies.getStartupScannedRecords() + index.getStartupScannedRecords()
        + commits.getStartupScannedRecords();
  }

  private void persistRestartCheckpoint() throws IOException {
    HistoryCommitMarker head = commits.head();
    if (head != null) {
      ArchiveRestartCheckpoint.persist(archiveDirectory, commits.firstEpoch(), commits.size(),
          commits.getRecordLength(), head, commitCodec);
    } else {
      Files.deleteIfExists(archiveDirectory.resolve("restart.checkpoint"));
      HistorySegmentStore.syncDirectory(archiveDirectory);
    }
  }

  private void persistChunk(List<BlockReverseDiff> diffs) throws IOException {
    List<HistoryLocation> bodyLocations = new ArrayList<>(diffs.size());
    List<HistoryIndexLocation> indexLocations = new ArrayList<>(diffs.size());
    for (BlockReverseDiff diff : diffs) {
      hook.before(Stage.APPEND_BODY, diff.getMeta());
      HistoryLocation bodyLocation = bodies.append(diff);
      bodyLocations.add(bodyLocation);
      hook.before(Stage.APPEND_INDEX, diff.getMeta());
      indexLocations.add(index.append(HistoryIndexRecord.from(diff, bodyLocation)));
    }
    BlockSnapshotMeta lastMeta = diffs.get(diffs.size() - 1).getMeta();
    hook.before(Stage.SYNC_BODY, lastMeta);
    bodies.sync();
    hook.before(Stage.SYNC_INDEX, lastMeta);
    index.sync();
    HistoryCommitMarker head = commits.head();
    long previousEpoch = head == null ? diffs.get(0).getMeta().getEpoch() - 1
        : head.getMeta().getEpoch();
    List<HistoryCommitMarker> markers = new ArrayList<>(diffs.size());
    for (int i = 0; i < diffs.size(); i++) {
      BlockReverseDiff diff = diffs.get(i);
      hook.before(Stage.COMMIT_MARKER, diff.getMeta());
      markers.add(new HistoryCommitMarker(diff.getMeta(), previousEpoch,
          bodyLocations.get(i), indexLocations.get(i), batchId(), participatingDatabases));
      previousEpoch = diff.getMeta().getEpoch();
    }
    commits.commitAll(markers);
    persistRestartCheckpoint();
  }

  private void validateNext(BlockSnapshotMeta previous, BlockSnapshotMeta meta) {
    if (previous == null) {
      return;
    }
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
    long committedCount = commits.size();
    if (bodyScan.getRecordCount() < committedCount
        || indexScan.getRecordCount() < committedCount) {
      throw new ArchivePersistenceException("Committed marker references missing body/index data");
    }

    if (bodyScan.getInvalidTail() != null) {
      if (bodyScan.getRecordCount() < committedCount) {
        throw new ArchivePersistenceException("Committed history body is corrupt");
      }
    }
    if (indexScan.getInvalidTailOffset() != null) {
      if (indexScan.getRecordCount() < committedCount) {
        throw new ArchivePersistenceException("Committed history index is corrupt");
      }
    }
    HistoryCommitMarker head = commits.head();
    if (head != null) {
      HistoryIndexRecord indexRecord = index.read(head.getIndexLocation());
      validateMarkerReferences(head, indexRecord);
      BlockReverseDiff body = bodies.read(head.getHistoryLocation());
      if (!head.getMeta().equals(body.getMeta())) {
        throw new ArchivePersistenceException("Commit head does not match history body metadata");
      }
    }
    index.truncateAfter(head == null ? null : head.getIndexLocation(), commits.size());
    bodies.truncateAfter(head == null ? null : head.getHistoryLocation(), commits.size());
  }

  private void catchUpAccountIndex() throws IOException {
    HistoryCommitMarker head = commits.head();
    if (head == null) {
      return;
    }
    long indexed = accountIndex.getIndexedThrough();
    long first = commits.firstEpoch();
    if (indexed >= 0) {
      HistoryCommitMarker indexedMarker = commits.get(indexed);
      if (indexedMarker == null || !accountIndex.headMatches(indexedMarker.getMeta())) {
        throw new ArchivePersistenceException(
            "Account index head differs from committed history");
      }
    }
    if (indexed >= head.getMeta().getEpoch()) {
      if (indexed > head.getMeta().getEpoch()) {
        throw new ArchivePersistenceException("Account index is ahead of committed history");
      }
      return;
    }
    long next = indexed < 0 ? first : indexed + 1;
    List<BlockReverseDiff> batch = new ArrayList<>(1024);
    for (long epoch = next; epoch <= head.getMeta().getEpoch(); epoch++) {
      batch.add(readCommitted(epoch));
      if (batch.size() == 1024 || epoch == head.getMeta().getEpoch()) {
        accountIndex.apply(batch);
        batch.clear();
      }
    }
  }

  private void closeAfterFailedConstruction(Exception failure) {
    try {
      accountIndex.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
    try {
      index.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
    try {
      bodies.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
    try {
      commits.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static OldValue findOldValue(BlockReverseDiff diff, String dbName, byte[] rawKey) {
    for (BlockReverseDiff.DbGroup group : diff.getGroups()) {
      if (!dbName.equals(group.getDbName())) {
        continue;
      }
      for (BlockReverseDiff.Entry entry : group.getEntries()) {
        if (Arrays.equals(rawKey, entry.getKey())) {
          return entry.getOldValue();
        }
      }
    }
    throw new ArchivePersistenceException("Account index references a missing history key");
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

  @Override
  public synchronized void close() throws IOException {
    IOException failure = null;
    try {
      index.close();
    } catch (IOException e) {
      failure = e;
    }
    try {
      accountIndex.close();
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
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
