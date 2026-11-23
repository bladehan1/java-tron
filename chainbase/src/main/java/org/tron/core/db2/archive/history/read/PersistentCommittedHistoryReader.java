package org.tron.core.db2.archive.history.read;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.contract.PinnedHistory;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;
import org.tron.core.db2.archive.history.model.HistoryIndexRecord;
import org.tron.core.db2.archive.history.model.HistoryIndexRecord.KeyGroup;
import org.tron.core.db2.archive.history.model.HistoryLocation;
import org.tron.core.db2.archive.history.store.ArchiveHistoryReadHandles;
import org.tron.core.db2.archive.serving.index.PinnedServingKeyIndex;

/** Request-owned commit/index/segment handles pinned to one persistent serving generation. */
public final class PersistentCommittedHistoryReader
    implements PinnedHistory {

  private final long indexedFrom;
  private final long indexedThrough;
  private final byte[] headHash;
  private final byte[] sourceDigest;
  private final ArchiveHistoryReadHandles handles;
  private boolean closed;

  private PersistentCommittedHistoryReader(Path archiveDirectory, long maxSegmentSize,
      PinnedServingKeyIndex serving) throws IOException {
    Objects.requireNonNull(serving, "serving");
    ArchiveHistoryReadHandles opened = ArchiveHistoryReadHandles.open(
        archiveDirectory, maxSegmentSize);
    try {
      validatePinnedAuthority(serving, opened);
    } catch (IOException | RuntimeException failure) {
      opened.closeAfterFailedConstruction(failure);
      throw failure;
    }
    this.handles = opened;
    this.indexedFrom = serving.getIndexedFrom();
    this.indexedThrough = serving.getIndexedThrough();
    this.headHash = serving.getHeadHash();
    this.sourceDigest = serving.getAuthoritativePrefixDigest();
  }

  public static PersistentCommittedHistoryReader open(Path archiveDirectory,
      long maxSegmentSize, PinnedServingKeyIndex serving) throws IOException {
    return new PersistentCommittedHistoryReader(archiveDirectory, maxSegmentSize, serving);
  }

  @Override
  public synchronized OldValue read(String dbName, byte[] rawKey, long firstChangeBlock)
      throws IOException {
    ensureOpen();
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(rawKey, "rawKey");
    if (firstChangeBlock <= indexedFrom || firstChangeBlock > indexedThrough) {
      throw new ArchivePersistenceException(
          "Serving index references history outside its pinned generation");
    }
    HistoryCommitMarker marker = handles.get(firstChangeBlock);
    if (marker == null) {
      throw new ArchivePersistenceException("Serving index references an uncommitted epoch");
    }
    HistoryIndexRecord indexRecord = handles.readIndex(marker.getIndexLocation());
    validateMarker(marker, indexRecord);
    BlockReverseDiff body = handles.readBody(marker.getHistoryLocation());
    if (!marker.getMeta().equals(body.getMeta()) || !sameKeys(indexRecord, body)) {
      throw new ArchivePersistenceException("Authoritative history index/body key mismatch");
    }
    if (!contains(indexRecord, dbName, rawKey)) {
      throw new ArchivePersistenceException(
          "Serving index key is absent from authoritative history index");
    }
    for (DbGroup group : body.getGroups()) {
      if (dbName.equals(group.getDbName())) {
        for (Entry entry : group.getEntries()) {
          if (Arrays.equals(rawKey, entry.getKey())) {
            return entry.getOldValue();
          }
        }
      }
    }
    throw new ArchivePersistenceException("Authoritative history body is missing the indexed key");
  }

  @Override
  public long getIndexedFrom() {
    return indexedFrom;
  }

  @Override
  public long getIndexedThrough() {
    return indexedThrough;
  }

  @Override
  public byte[] getHeadHash() {
    return Arrays.copyOf(headHash, headHash.length);
  }

  @Override
  public byte[] getAuthoritativePrefixDigest() {
    return Arrays.copyOf(sourceDigest, sourceDigest.length);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    handles.close();
  }

  private static void validatePinnedAuthority(PinnedServingKeyIndex serving,
      ArchiveHistoryReadHandles handles)
      throws IOException {
    HistoryCommitMarker marker = handles.get(serving.getIndexedThrough());
    if (marker == null || !Arrays.equals(marker.getMeta().getBlockHash(), serving.getHeadHash())
        || !marker.getDatabases().equals(serving.getParticipatingDatabases())) {
      throw new ArchivePersistenceException(
          "Serving generation does not match committed history authority");
    }
    HistoryIndexRecord indexRecord = handles.readIndex(marker.getIndexLocation());
    validateMarker(marker, indexRecord);
    BlockReverseDiff body = handles.readBody(marker.getHistoryLocation());
    if (!marker.getMeta().equals(body.getMeta()) || !sameKeys(indexRecord, body)) {
      throw new ArchivePersistenceException(
          "Serving generation head does not match authoritative history files");
    }
  }

  private static void validateMarker(HistoryCommitMarker marker, HistoryIndexRecord record) {
    if (!marker.getMeta().equals(record.getMeta())
        || !same(marker.getHistoryLocation(), record.getHistoryLocation())) {
      throw new ArchivePersistenceException(
          "Commit marker does not match authoritative history index");
    }
  }

  private static boolean contains(HistoryIndexRecord record, String dbName, byte[] rawKey) {
    for (KeyGroup group : record.getGroups()) {
      if (dbName.equals(group.getDbName())) {
        for (byte[] key : group.getKeys()) {
          if (Arrays.equals(key, rawKey)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean sameKeys(HistoryIndexRecord record, BlockReverseDiff body) {
    List<KeyGroup> indexed = record.getGroups();
    List<DbGroup> stored = body.getGroups();
    if (indexed.size() != stored.size()) {
      return false;
    }
    for (int groupIndex = 0; groupIndex < indexed.size(); groupIndex++) {
      KeyGroup indexedGroup = indexed.get(groupIndex);
      DbGroup storedGroup = stored.get(groupIndex);
      if (!indexedGroup.getDbName().equals(storedGroup.getDbName())
          || indexedGroup.getKeys().size() != storedGroup.getEntries().size()) {
        return false;
      }
      for (int keyIndex = 0; keyIndex < indexedGroup.getKeys().size(); keyIndex++) {
        if (!Arrays.equals(indexedGroup.getKeys().get(keyIndex),
            storedGroup.getEntries().get(keyIndex).getKey())) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean same(HistoryLocation left, HistoryLocation right) {
    return left.getSegmentId() == right.getSegmentId()
        && left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && left.getBodyChecksum() == right.getBodyChecksum()
        && Arrays.equals(left.getBodyDigest(), right.getBodyDigest());
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Persistent committed history reader is closed");
    }
  }
}
