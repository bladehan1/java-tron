package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;

/**
 * Fail-closed authoritative old-value reader for one immutable committed prefix.
 *
 * <p>The current prototype validates its source identity by rebuilding a throwaway serving
 * generation. A production reader will pin commit/index/segment generations without rebuilding.
 */
public final class CommittedHistoryReader implements ArchiveReadSnapshot.PinnedHistory {

  private final long indexedFrom;
  private final long indexedThrough;
  private final byte[] headHash;
  private final byte[] authoritativePrefixDigest;
  private final Map<Long, HistoryCommitMarker> markers;
  private final ServingKeyIndexGeneration.AuthoritativeIndexReader indexReader;
  private final BodyReader bodyReader;
  private final Closeable release;
  private boolean closed;

  public CommittedHistoryReader(long baseEpoch, byte[] baseHash,
      List<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader indexReader, BodyReader bodyReader)
      throws IOException {
    this(baseEpoch, baseHash, committed, indexReader, bodyReader, null, () -> { });
  }

  public CommittedHistoryReader(long baseEpoch, byte[] baseHash,
      List<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader indexReader, BodyReader bodyReader,
      List<String> participatingDatabases) throws IOException {
    this(baseEpoch, baseHash, committed, indexReader, bodyReader, participatingDatabases,
        () -> { });
  }

  public CommittedHistoryReader(long baseEpoch, byte[] baseHash,
      List<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader indexReader, BodyReader bodyReader,
      Closeable release) throws IOException {
    this(baseEpoch, baseHash, committed, indexReader, bodyReader, null, release);
  }

  private CommittedHistoryReader(long baseEpoch, byte[] baseHash,
      List<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader indexReader, BodyReader bodyReader,
      List<String> participatingDatabases, Closeable release) throws IOException {
    Objects.requireNonNull(committed, "committed");
    this.indexReader = Objects.requireNonNull(indexReader, "indexReader");
    this.bodyReader = Objects.requireNonNull(bodyReader, "bodyReader");
    this.release = Objects.requireNonNull(release, "release");
    ServingKeyIndexGeneration verified;
    try {
      verified = participatingDatabases == null
          ? ServingKeyIndexGeneration.rebuild(
              "authoritative-history-reader", baseEpoch, baseHash, committed, indexReader)
          : ServingKeyIndexGeneration.rebuild(
              "authoritative-history-reader", baseEpoch, baseHash, committed, indexReader,
              participatingDatabases,
              ServingKeyIndexGeneration.IndexLayout.prototypeDefaults());
    } catch (IOException | RuntimeException failure) {
      closeAfterFailedConstruction(release, failure);
      throw failure;
    }
    this.indexedFrom = verified.getIndexedFrom();
    this.indexedThrough = verified.getIndexedThrough();
    this.headHash = verified.getHeadHash();
    this.authoritativePrefixDigest = verified.getAuthoritativePrefixDigest();
    this.markers = new HashMap<>();
    for (HistoryCommitMarker marker : committed) {
      if (markers.put(marker.getMeta().getEpoch(), marker) != null) {
        throw new IllegalArgumentException("Duplicate committed history epoch");
      }
    }
  }

  @Override
  public synchronized OldValue read(String dbName, byte[] rawKey, long firstChangeBlock)
      throws IOException {
    ensureOpen();
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(rawKey, "rawKey");
    HistoryCommitMarker marker = markers.get(firstChangeBlock);
    if (marker == null) {
      throw new ArchivePersistenceException(
          "Serving index references an uncommitted history epoch: " + firstChangeBlock);
    }
    HistoryIndexRecord index = indexReader.read(marker.getIndexLocation());
    validateMarker(marker, index);
    BlockReverseDiff body = bodyReader.read(marker.getHistoryLocation());
    if (!marker.getMeta().equals(body.getMeta()) || !sameKeys(index, body)) {
      throw new ArchivePersistenceException("Authoritative history index/body key mismatch");
    }
    if (!contains(index, dbName, rawKey)) {
      throw new ArchivePersistenceException(
          "Serving index key is absent from authoritative history index");
    }
    for (DbGroup group : body.getGroups()) {
      if (!dbName.equals(group.getDbName())) {
        continue;
      }
      for (Entry entry : group.getEntries()) {
        if (Arrays.equals(rawKey, entry.getKey())) {
          return entry.getOldValue();
        }
      }
    }
    throw new ArchivePersistenceException(
        "Authoritative history body is missing the indexed key");
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
    return Arrays.copyOf(authoritativePrefixDigest, authoritativePrefixDigest.length);
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      release.close();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Committed history reader is closed");
    }
  }

  private static void validateMarker(HistoryCommitMarker marker, HistoryIndexRecord index) {
    if (!marker.getMeta().equals(index.getMeta())
        || !same(marker.getHistoryLocation(), index.getHistoryLocation())) {
      throw new ArchivePersistenceException(
          "Commit marker does not match authoritative history index");
    }
  }

  private static boolean contains(HistoryIndexRecord index, String dbName, byte[] rawKey) {
    for (KeyGroup group : index.getGroups()) {
      if (dbName.equals(group.getDbName())) {
        return group.getKeys().stream().anyMatch(key -> Arrays.equals(key, rawKey));
      }
    }
    return false;
  }

  private static boolean sameKeys(HistoryIndexRecord index, BlockReverseDiff body) {
    List<KeyGroup> indexed = index.getGroups();
    List<DbGroup> stored = body.getGroups();
    if (indexed.size() != stored.size()) {
      return false;
    }
    for (int groupIndex = 0; groupIndex < indexed.size(); groupIndex++) {
      KeyGroup indexedGroup = indexed.get(groupIndex);
      DbGroup storedGroup = stored.get(groupIndex);
      List<byte[]> indexedKeys = indexedGroup.getKeys();
      List<Entry> storedEntries = storedGroup.getEntries();
      if (!indexedGroup.getDbName().equals(storedGroup.getDbName())
          || indexedKeys.size() != storedEntries.size()) {
        return false;
      }
      for (int keyIndex = 0; keyIndex < indexedKeys.size(); keyIndex++) {
        if (!Arrays.equals(indexedKeys.get(keyIndex), storedEntries.get(keyIndex).getKey())) {
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

  private static void closeAfterFailedConstruction(Closeable release, Exception failure) {
    try {
      release.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  @FunctionalInterface
  public interface BodyReader {
    BlockReverseDiff read(HistoryLocation location) throws IOException;
  }
}
