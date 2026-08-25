package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestStateFactory;
import org.tron.core.db2.common.DB;

/** Fail-closed adapter for one exact latest-state engine generation. */
public final class LatestStateGenerationAdapter implements PinnedLatestStateFactory {

  private final List<String> participants;
  private final Map<String, SnapshotCapableStore> stores;
  private final Map<String, String> sourceIdentities;
  private final byte[] sourceIdentityDigest;

  public LatestStateGenerationAdapter(List<String> participants,
      Map<String, SnapshotCapableStore> stores) {
    this.participants = validateParticipants(participants);
    TreeMap<String, SnapshotCapableStore> sorted = new TreeMap<>(
        Objects.requireNonNull(stores, "stores"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Latest-state snapshot Store set mismatch");
    }
    TreeMap<String, String> identities = new TreeMap<>();
    for (Map.Entry<String, SnapshotCapableStore> entry : sorted.entrySet()) {
      SnapshotCapableStore store = entry.getValue();
      if (!entry.getKey().equals(store.getDbName())
          || store.getSourceIdentity() == null || store.getSourceIdentity().isEmpty()) {
        throw new IllegalArgumentException("Latest-state snapshot source identity is invalid");
      }
      identities.put(entry.getKey(), store.getSourceIdentity());
    }
    this.stores = Collections.unmodifiableMap(sorted);
    this.sourceIdentities = Collections.unmodifiableMap(identities);
    this.sourceIdentityDigest = sourceIdentityDigest(identities);
  }

  /**
   * Converts the current DB abstraction only when every Store explicitly implements the stable
   * snapshot lifecycle capability. Ordinary {@code get()} is never accepted as a substitute.
   */
  public static LatestStateGenerationAdapter fromDatabases(List<String> participants,
      Map<String, ? extends DB<byte[], byte[]>> databases) throws ArchivePersistenceException {
    TreeMap<String, SnapshotCapableStore> capable = new TreeMap<>();
    for (Map.Entry<String, ? extends DB<byte[], byte[]>> entry :
        Objects.requireNonNull(databases, "databases").entrySet()) {
      if (!(entry.getValue() instanceof SnapshotCapableStore)) {
        throw new ArchivePersistenceException(
            "DB does not expose a stable snapshot lifecycle: " + entry.getKey());
      }
      capable.put(entry.getKey(), (SnapshotCapableStore) entry.getValue());
    }
    return new LatestStateGenerationAdapter(participants, capable);
  }

  /** Adapts one out-of-registry native Store to the same stable snapshot contract. */
  public static SnapshotCapableStore fromDataSource(String dbName,
      DbSourceInter<byte[]> source) throws ArchivePersistenceException {
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(source, "source");
    if (!dbName.equals(source.getDBName())) {
      throw new ArchivePersistenceException("Supplemental latest Store name mismatch: " + dbName);
    }
    if (source instanceof LevelDbDataSourceImpl) {
      LevelDbDataSourceImpl level = (LevelDbDataSourceImpl) source;
      return capable(dbName, level.getSnapshotSourceIdentity(), (blockNumber, blockHash) -> {
        LevelDbDataSourceImpl.PinnedSnapshot pinned = level.pinSnapshot();
        return snapshot(dbName, pinned.getSourceIdentity(), blockNumber, blockHash,
            pinned::get, pinned::range, pinned::close);
      });
    }
    if (source instanceof RocksDbDataSourceImpl) {
      RocksDbDataSourceImpl rocks = (RocksDbDataSourceImpl) source;
      return capable(dbName, rocks.getSnapshotSourceIdentity(), (blockNumber, blockHash) -> {
        RocksDbDataSourceImpl.PinnedSnapshot pinned = rocks.pinSnapshot();
        return snapshot(dbName, pinned.getSourceIdentity(), blockNumber, blockHash,
            pinned::get, pinned::range, pinned::close);
      });
    }
    throw new ArchivePersistenceException(
        "Supplemental latest Store lacks a supported native snapshot engine: " + dbName);
  }

  private static SnapshotCapableStore capable(String dbName, String sourceIdentity,
      SnapshotFactory factory) {
    return new SnapshotCapableStore() {
      @Override
      public String getDbName() {
        return dbName;
      }

      @Override
      public String getSourceIdentity() {
        return sourceIdentity;
      }

      @Override
      public StoreSnapshot pin(long blockNumber, byte[] blockHash) throws IOException {
        return factory.pin(blockNumber, blockHash);
      }
    };
  }

  private static StoreSnapshot snapshot(String dbName, String sourceIdentity, long blockNumber,
      byte[] blockHash, PointReader reader, RangeReader rangeReader, CloseableAction close) {
    byte[] expectedHash = Arrays.copyOf(blockHash, blockHash.length);
    return new StoreSnapshot() {
      @Override
      public String getDbName() {
        return dbName;
      }

      @Override
      public String getSourceIdentity() {
        return sourceIdentity;
      }

      @Override
      public long getBlockNumber() {
        return blockNumber;
      }

      @Override
      public byte[] getBlockHash() {
        return Arrays.copyOf(expectedHash, expectedHash.length);
      }

      @Override
      public byte[] get(byte[] physicalRawKey) {
        return reader.get(physicalRawKey);
      }

      @Override
      public List<Map.Entry<byte[], byte[]>> range(byte[] lowerInclusive,
          byte[] upperExclusive, int maxEntries) {
        return rangeReader.range(lowerInclusive, upperExclusive, maxEntries);
      }

      @Override
      public void close() throws IOException {
        close.close();
      }
    };
  }

  @Override
  public PinnedLatestState pin(PersistentServingKeyIndexGeneration serving) throws IOException {
    Objects.requireNonNull(serving, "serving");
    return pin(serving.getGenerationId(), serving.getIndexedThrough(), serving.getHeadHash(),
        serving.getParticipatingDatabases());
  }

  PinnedLatestState pin(String generationId, long blockNumber, byte[] blockHash,
      List<String> expectedParticipants) throws IOException {
    if (generationId == null || generationId.isEmpty() || blockNumber < 0
        || blockHash == null || blockHash.length != 32
        || !participants.equals(expectedParticipants)) {
      throw new ArchivePersistenceException("Latest-state generation identity mismatch");
    }
    TreeMap<String, StoreSnapshot> acquired = new TreeMap<>();
    try {
      for (Map.Entry<String, SnapshotCapableStore> entry : stores.entrySet()) {
        SnapshotCapableStore store = entry.getValue();
        String expectedSource = sourceIdentities.get(entry.getKey());
        if (!expectedSource.equals(store.getSourceIdentity())) {
          throw new ArchivePersistenceException(
              "Latest-state Store source was replaced before pin: " + entry.getKey());
        }
        StoreSnapshot snapshot = Objects.requireNonNull(
            store.pin(blockNumber, blockHash), "pinned Store snapshot");
        acquired.put(entry.getKey(), snapshot);
        validateSnapshot(entry.getKey(), expectedSource, blockNumber, blockHash, snapshot);
      }
      return new PinnedGeneration(generationId, blockNumber, blockHash, acquired,
          sourceIdentityDigest);
    } catch (IOException | RuntimeException failure) {
      closeAll(acquired, failure);
      throw failure;
    }
  }

  public byte[] getSourceIdentityDigest() {
    return Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
  }

  private static void validateSnapshot(String dbName, String sourceIdentity, long blockNumber,
      byte[] blockHash, StoreSnapshot snapshot) throws ArchivePersistenceException {
    if (!dbName.equals(snapshot.getDbName())
        || !sourceIdentity.equals(snapshot.getSourceIdentity())
        || snapshot.getBlockNumber() != blockNumber
        || !Arrays.equals(blockHash, snapshot.getBlockHash())) {
      throw new ArchivePersistenceException(
          "Pinned latest-state Store snapshot identity mismatch: " + dbName);
    }
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Latest-state participant set must not be empty");
    }
    String previous = null;
    for (String participant : copy) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Latest-state participants must be non-empty, unique, and sorted");
      }
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  private static byte[] sourceIdentityDigest(Map<String, String> identities) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(identities.size()).array());
    for (Map.Entry<String, String> entry : identities.entrySet()) {
      update(digest, entry.getKey());
      update(digest, entry.getValue());
    }
    return digest.digest();
  }

  private static void update(MessageDigest digest, String value) {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(encoded.length).array());
    digest.update(encoded);
  }

  private static void closeAll(Map<String, StoreSnapshot> snapshots, Exception failure) {
    List<StoreSnapshot> reverse = new ArrayList<>(snapshots.values());
    Collections.reverse(reverse);
    for (StoreSnapshot snapshot : reverse) {
      try {
        snapshot.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  /** Minimum capability that RocksDB/LevelDB wrappers must implement before production pinning. */
  public interface SnapshotCapableStore {
    String getDbName();

    String getSourceIdentity();

    StoreSnapshot pin(long blockNumber, byte[] blockHash) throws IOException;
  }

  /** Stable point-read view whose lifetime prevents the underlying engine from being replaced. */
  public interface StoreSnapshot extends Closeable {
    String getDbName();

    String getSourceIdentity();

    long getBlockNumber();

    byte[] getBlockHash();

    byte[] get(byte[] physicalRawKey) throws IOException;

    default List<Map.Entry<byte[], byte[]>> range(byte[] lowerInclusive, byte[] upperExclusive,
        int maxEntries) throws IOException {
      throw new UnsupportedOperationException("Pinned Store range is unsupported");
    }
  }

  @FunctionalInterface
  private interface SnapshotFactory {
    StoreSnapshot pin(long blockNumber, byte[] blockHash) throws IOException;
  }

  @FunctionalInterface
  private interface PointReader {
    byte[] get(byte[] key);
  }

  @FunctionalInterface
  private interface RangeReader {
    List<Map.Entry<byte[], byte[]>> range(byte[] lowerInclusive, byte[] upperExclusive,
        int maxEntries);
  }

  @FunctionalInterface
  private interface CloseableAction {
    void close() throws IOException;
  }

  private static final class PinnedGeneration implements PinnedLatestState {
    private final String generationId;
    private final long blockNumber;
    private final byte[] blockHash;
    private final Map<String, StoreSnapshot> snapshots;
    private final byte[] sourceIdentityDigest;
    private boolean closed;

    private PinnedGeneration(String generationId, long blockNumber, byte[] blockHash,
        Map<String, StoreSnapshot> snapshots, byte[] sourceIdentityDigest) {
      this.generationId = generationId;
      this.blockNumber = blockNumber;
      this.blockHash = Arrays.copyOf(blockHash, blockHash.length);
      this.snapshots = Collections.unmodifiableMap(new TreeMap<>(snapshots));
      this.sourceIdentityDigest = Arrays.copyOf(sourceIdentityDigest,
          sourceIdentityDigest.length);
    }

    @Override
    public long getBlockNumber() {
      return blockNumber;
    }

    @Override
    public byte[] getBlockHash() {
      return Arrays.copyOf(blockHash, blockHash.length);
    }

    @Override
    public byte[] getSourceIdentityDigest() {
      return Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
    }

    @Override
    public synchronized OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
      ensureOpen();
      StoreSnapshot snapshot = snapshots.get(dbName);
      if (snapshot == null) {
        throw new ArchivePersistenceException(
            "Database is outside pinned latest generation: " + dbName);
      }
      return OldValue.fromNullable(snapshot.get(physicalRawKey));
    }

    @Override
    public synchronized List<HistoricalRangeOverlay.Entry> range(String dbName,
        byte[] lowerInclusive, byte[] upperExclusive, int maxEntries) throws IOException {
      ensureOpen();
      if (!"account-asset".equals(dbName) || maxEntries <= 0
          || maxEntries == Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "Latest-generation range is limited to bounded account-asset queries");
      }
      StoreSnapshot snapshot = snapshots.get(dbName);
      if (snapshot == null) {
        throw new ArchivePersistenceException(
            "Database is outside pinned latest generation: " + dbName);
      }
      int scanLimit = maxEntries == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxEntries + 1;
      List<Map.Entry<byte[], byte[]>> raw = snapshot.range(
          lowerInclusive, upperExclusive, scanLimit);
      if (raw.size() > maxEntries) {
        throw new ArchiveQueryLimitExceededException(
            "latest AccountAsset candidate-key budget exceeded");
      }
      List<HistoricalRangeOverlay.Entry> result = new ArrayList<>(raw.size());
      for (Map.Entry<byte[], byte[]> entry : raw) {
        result.add(new HistoricalRangeOverlay.Entry(entry.getKey(), entry.getValue()));
      }
      return Collections.unmodifiableList(result);
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      IOException failure = null;
      List<StoreSnapshot> reverse = new ArrayList<>(snapshots.values());
      Collections.reverse(reverse);
      for (StoreSnapshot snapshot : reverse) {
        try {
          snapshot.close();
        } catch (IOException closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else {
            failure.addSuppressed(closeFailure);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException(
            "Pinned latest-state generation is closed: " + generationId);
      }
    }
  }
}
