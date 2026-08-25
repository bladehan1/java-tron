package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Immutable backend-neutral prototype of one derived serving-key-index generation.
 *
 * <p>This class deliberately defines no persistent page encoding. A production LSM backend can
 * preserve this exact-key, committed-prefix and coverage contract after H1 format approval.
 */
public final class ServingKeyIndexGeneration implements ServingKeyIndex {

  private final String generationId;
  private final long indexedFrom;
  private final long indexedThrough;
  private final byte[] headHash;
  private final byte[] authoritativePrefixDigest;
  private final IndexLayout layout;
  private final Map<KeyIdentity, KeyChangeIndex> changes;
  private final Map<String, StoreCoverage> storeCoverage;

  private ServingKeyIndexGeneration(String generationId, long indexedFrom, long indexedThrough,
      byte[] headHash, byte[] authoritativePrefixDigest, IndexLayout layout,
      Map<KeyIdentity, KeyChangeIndex> changes, Map<String, StoreCoverage> storeCoverage) {
    this.generationId = generationId;
    this.indexedFrom = indexedFrom;
    this.indexedThrough = indexedThrough;
    this.headHash = Arrays.copyOf(headHash, headHash.length);
    this.authoritativePrefixDigest = Arrays.copyOf(authoritativePrefixDigest,
        authoritativePrefixDigest.length);
    this.layout = layout;
    this.changes = Collections.unmodifiableMap(new HashMap<>(changes));
    this.storeCoverage = Collections.unmodifiableMap(new HashMap<>(storeCoverage));
  }

  /** Rebuilds a complete immutable generation from commit-marker-proven index records. */
  public static ServingKeyIndexGeneration rebuild(String generationId, long baseEpoch,
      byte[] baseHash, List<HistoryCommitMarker> committed,
      AuthoritativeIndexReader reader) throws IOException {
    return rebuild(generationId, baseEpoch, baseHash, committed, reader,
        IndexLayout.prototypeDefaults());
  }

  /** Rebuilds using an explicit prototype layout without defining a persistent page ABI. */
  public static ServingKeyIndexGeneration rebuild(String generationId, long baseEpoch,
      byte[] baseHash, List<HistoryCommitMarker> committed,
      AuthoritativeIndexReader reader, IndexLayout layout) throws IOException {
    return rebuild(generationId, baseEpoch, baseHash, committed, reader, null, layout);
  }

  /** Rebuilds against an explicit descriptor participant set, including an empty prefix. */
  public static ServingKeyIndexGeneration rebuild(String generationId, long baseEpoch,
      byte[] baseHash, List<HistoryCommitMarker> committed,
      AuthoritativeIndexReader reader, List<String> expectedParticipatingDatabases,
      IndexLayout layout) throws IOException {
    if (generationId == null || generationId.isEmpty()) {
      throw new IllegalArgumentException("generationId must not be empty");
    }
    if (baseEpoch < 0) {
      throw new IllegalArgumentException("baseEpoch must not be negative");
    }
    requireHash(baseHash, "baseHash");
    Objects.requireNonNull(committed, "committed");
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(layout, "layout");

    Map<KeyIdentity, List<Long>> mutable = new HashMap<>();
    MessageDigest sourceDigest = sha256();
    updateLong(sourceDigest, baseEpoch);
    sourceDigest.update(baseHash);
    long previousEpoch = baseEpoch;
    long previousBlock = baseEpoch;
    byte[] previousHash = Arrays.copyOf(baseHash, baseHash.length);
    List<String> participatingDatabases = expectedParticipatingDatabases == null ? null
        : sortedParticipants(expectedParticipatingDatabases);
    if (participatingDatabases != null) {
      updateStringDigest(sourceDigest,
          ArchiveParticipantDescriptor.scopeIdentity(participatingDatabases));
      updateParticipantDigest(sourceDigest, participatingDatabases);
    }

    for (HistoryCommitMarker marker : committed) {
      Objects.requireNonNull(marker, "committed marker");
      BlockSnapshotMeta meta = marker.getMeta();
      if (marker.getPreviousEpoch() != previousEpoch
          || meta.getEpoch() != previousEpoch + 1
          || meta.getBlockNumber() != previousBlock + 1
          || meta.getEpoch() != meta.getBlockNumber()
          || !Arrays.equals(meta.getParentHash(), previousHash)) {
        throw new IllegalArgumentException("Serving index source commit prefix is not contiguous");
      }
      if (participatingDatabases == null) {
        participatingDatabases = marker.getDatabases();
        validateParticipantSet(participatingDatabases);
        updateStringDigest(sourceDigest,
            ArchiveParticipantDescriptor.scopeIdentity(participatingDatabases));
        updateParticipantDigest(sourceDigest, participatingDatabases);
      } else if (!participatingDatabases.equals(marker.getDatabases())) {
        throw new IllegalArgumentException(
            "Serving index source participant set changes inside one generation");
      }

      HistoryIndexRecord record = reader.read(marker.getIndexLocation());
      validateMarker(marker, record, participatingDatabases);
      addRecord(mutable, record);
      updateSourceDigest(sourceDigest, marker);
      previousEpoch = meta.getEpoch();
      previousBlock = meta.getBlockNumber();
      previousHash = meta.getBlockHash();
    }

    Map<KeyIdentity, KeyChangeIndex> immutable = new HashMap<>();
    mutable.forEach((key, blocks) -> {
      immutable.put(key, KeyChangeIndex.from(blocks, layout));
    });
    byte[] prefixDigest = sourceDigest.digest();
    Map<String, StoreCoverage> coverage = new HashMap<>();
    if (participatingDatabases != null) {
      for (String database : participatingDatabases) {
        coverage.put(database, new StoreCoverage(database, baseEpoch, previousEpoch,
            previousHash, prefixDigest));
      }
    }
    return new ServingKeyIndexGeneration(generationId, baseEpoch, previousEpoch, previousHash,
        prefixDigest, layout, immutable, coverage);
  }

  /** Returns the first changed block in {@code (targetBlock, upperBound]}. */
  public OptionalLong firstChangeAfter(String dbName, byte[] rawKey, long targetBlock,
      long upperBound) {
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(rawKey, "rawKey");
    requireStoreCoverage(dbName, targetBlock, upperBound);
    KeyChangeIndex index = changes.get(new KeyIdentity(dbName, rawKey));
    return index == null ? OptionalLong.empty()
        : index.firstChangeAfter(targetBlock, upperBound);
  }

  /**
   * Returns sorted exact keys in one database which changed in {@code (targetBlock, upperBound]}.
   *
   * <p>The in-memory prototype scans all key metadata. A production ordered LSM must preserve the
   * result and budget contract without materializing the entire database keyspace.
   */
  public List<ChangedKey> changesInRange(String dbName, byte[] lowerInclusive,
      byte[] upperExclusive, long targetBlock, long upperBound, int maxChangedKeys) {
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(lowerInclusive, "lowerInclusive");
    if (maxChangedKeys <= 0) {
      throw new IllegalArgumentException("maxChangedKeys must be positive");
    }
    if (upperExclusive != null
        && BlockReverseDiff.compareUnsigned(lowerInclusive, upperExclusive) > 0) {
      throw new IllegalArgumentException("lowerInclusive must not exceed upperExclusive");
    }
    requireStoreCoverage(dbName, targetBlock, upperBound);
    List<ChangedKey> result = new ArrayList<>();
    for (Map.Entry<KeyIdentity, KeyChangeIndex> entry : changes.entrySet()) {
      KeyIdentity identity = entry.getKey();
      if (!identity.matchesDatabase(dbName) || !inRange(identity.rawKey, lowerInclusive,
          upperExclusive)) {
        continue;
      }
      OptionalLong first = entry.getValue().firstChangeAfter(targetBlock, upperBound);
      if (!first.isPresent()) {
        continue;
      }
      if (result.size() == maxChangedKeys) {
        throw new ArchiveQueryLimitExceededException("changed-key budget exceeded");
      }
      result.add(new ChangedKey(identity.rawKey, first.getAsLong()));
    }
    result.sort((left, right) -> BlockReverseDiff.compareUnsigned(left.key, right.key));
    return Collections.unmodifiableList(result);
  }

  private void validateCoverage(long targetBlock, long upperBound) {
    if (targetBlock < indexedFrom || upperBound > indexedThrough || targetBlock > upperBound) {
      throw new IllegalArgumentException("Query range is outside serving index coverage");
    }
  }

  private void requireStoreCoverage(String dbName, long targetBlock, long upperBound) {
    validateCoverage(targetBlock, upperBound);
    StoreCoverage coverage = storeCoverage.get(dbName);
    if (coverage == null || !coverage.covers(targetBlock, upperBound)) {
      throw new IllegalArgumentException("Database is outside serving index coverage: " + dbName);
    }
  }

  private static boolean inRange(byte[] key, byte[] lowerInclusive, byte[] upperExclusive) {
    return BlockReverseDiff.compareUnsigned(key, lowerInclusive) >= 0
        && (upperExclusive == null
        || BlockReverseDiff.compareUnsigned(key, upperExclusive) < 0);
  }

  private static OptionalLong firstChange(long[] blocks, long targetBlock, long upperBound) {
    int low = 0;
    int high = blocks.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (blocks[middle] <= targetBlock) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low < blocks.length && blocks[low] <= upperBound
        ? OptionalLong.of(blocks[low]) : OptionalLong.empty();
  }

  public String getGenerationId() {
    return generationId;
  }

  public long getIndexedFrom() {
    return indexedFrom;
  }

  public long getIndexedThrough() {
    return indexedThrough;
  }

  public byte[] getHeadHash() {
    return Arrays.copyOf(headHash, headHash.length);
  }

  public byte[] getAuthoritativePrefixDigest() {
    return Arrays.copyOf(authoritativePrefixDigest, authoritativePrefixDigest.length);
  }

  public IndexLayout getLayout() {
    return layout;
  }

  public Optional<StoreCoverage> getStoreCoverage(String dbName) {
    Objects.requireNonNull(dbName, "dbName");
    return Optional.ofNullable(storeCoverage.get(dbName));
  }

  public int getKeyMetadataCount() {
    return changes.size();
  }

  public int getInlineKeyCount() {
    return (int) changes.values().stream().filter(KeyChangeIndex::isInline).count();
  }

  public int getPagedKeyCount() {
    return getKeyMetadataCount() - getInlineKeyCount();
  }

  public int getEpochPageCount() {
    return changes.values().stream().mapToInt(KeyChangeIndex::getPageCount).sum();
  }

  public static final class ChangedKey {
    private final byte[] key;
    private final long firstChangeBlock;

    ChangedKey(byte[] key, long firstChangeBlock) {
      this.key = Arrays.copyOf(key, key.length);
      this.firstChangeBlock = firstChangeBlock;
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public long getFirstChangeBlock() {
      return firstChangeBlock;
    }
  }

  private static void addRecord(Map<KeyIdentity, List<Long>> changes,
      HistoryIndexRecord record) {
    String previousDb = null;
    for (HistoryIndexRecord.KeyGroup group : record.getGroups()) {
      if (previousDb != null && previousDb.compareTo(group.getDbName()) >= 0) {
        throw new IllegalArgumentException("Serving index database groups are not sorted");
      }
      previousDb = group.getDbName();
      byte[] previousKey = null;
      for (byte[] key : group.getKeys()) {
        if (previousKey != null && BlockReverseDiff.compareUnsigned(previousKey, key) >= 0) {
          throw new IllegalArgumentException("Serving index keys are not sorted");
        }
        previousKey = key;
        List<Long> blocks = changes.computeIfAbsent(new KeyIdentity(group.getDbName(), key),
            ignored -> new ArrayList<>());
        long block = record.getMeta().getBlockNumber();
        if (!blocks.isEmpty() && blocks.get(blocks.size() - 1) >= block) {
          throw new IllegalArgumentException("Serving index changes are not strictly increasing");
        }
        blocks.add(block);
      }
    }
  }

  private static void validateMarker(HistoryCommitMarker marker, HistoryIndexRecord record,
      List<String> participatingDatabases) {
    if (record == null || !marker.getMeta().equals(record.getMeta())
        || !same(marker.getHistoryLocation(), record.getHistoryLocation())) {
      throw new IllegalArgumentException(
          "Commit marker does not match authoritative history index record");
    }
    for (HistoryIndexRecord.KeyGroup group : record.getGroups()) {
      if (Collections.binarySearch(participatingDatabases, group.getDbName()) < 0) {
        throw new IllegalArgumentException(
            "History index contains a database outside the participant set");
      }
    }
  }

  private static boolean same(HistoryLocation left, HistoryLocation right) {
    return left.getSegmentId() == right.getSegmentId()
        && left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && left.getBodyChecksum() == right.getBodyChecksum()
        && Arrays.equals(left.getBodyDigest(), right.getBodyDigest());
  }

  private static void updateSourceDigest(MessageDigest digest, HistoryCommitMarker marker) {
    updateLong(digest, marker.getMeta().getEpoch());
    updateLong(digest, marker.getMeta().getBlockNumber());
    digest.update(marker.getMeta().getBlockHash());
    digest.update(marker.getMeta().getParentHash());
    updateLong(digest, marker.getIndexLocation().getOffset());
    updateLong(digest, marker.getIndexLocation().getRecordLength());
    digest.update(marker.getIndexLocation().getDigest());
    digest.update(marker.getHistoryLocation().getBodyDigest());
  }

  private static void updateParticipantDigest(MessageDigest digest, List<String> databases) {
    updateLong(digest, databases.size());
    for (String database : databases) {
      byte[] encoded = database.getBytes(StandardCharsets.UTF_8);
      updateLong(digest, encoded.length);
      digest.update(encoded);
    }
  }

  private static void updateStringDigest(MessageDigest digest, String value) {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    updateLong(digest, encoded.length);
    digest.update(encoded);
  }

  private static void validateParticipantSet(List<String> databases) {
    if (databases.isEmpty()) {
      throw new IllegalArgumentException("Serving index participant set must not be empty");
    }
    String previous = null;
    for (String database : databases) {
      if (database == null || database.isEmpty()
          || (previous != null && previous.compareTo(database) >= 0)) {
        throw new IllegalArgumentException(
            "Serving index participant set must be non-empty, unique, and sorted");
      }
      previous = database;
    }
  }

  private static List<String> sortedParticipants(List<String> databases) {
    List<String> result = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    Collections.sort(result);
    validateParticipantSet(result);
    return Collections.unmodifiableList(result);
  }

  private static void updateLong(MessageDigest digest, long value) {
    digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static void requireHash(byte[] hash, String name) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
  }

  @FunctionalInterface
  public interface AuthoritativeIndexReader {
    HistoryIndexRecord read(HistoryIndexLocation location) throws IOException;
  }

  /** Tunable in-memory prototype layout; this is not a persistent format contract. */
  public static final class IndexLayout {
    private static final int DEFAULT_INLINE_EPOCH_LIMIT = 4;
    private static final int DEFAULT_MAX_EPOCHS_PER_PAGE = 512;

    private final int inlineEpochLimit;
    private final int maxEpochsPerPage;

    public IndexLayout(int inlineEpochLimit, int maxEpochsPerPage) {
      if (inlineEpochLimit <= 0 || maxEpochsPerPage <= 0) {
        throw new IllegalArgumentException("Serving index layout limits must be positive");
      }
      this.inlineEpochLimit = inlineEpochLimit;
      this.maxEpochsPerPage = maxEpochsPerPage;
    }

    public static IndexLayout prototypeDefaults() {
      return new IndexLayout(DEFAULT_INLINE_EPOCH_LIMIT, DEFAULT_MAX_EPOCHS_PER_PAGE);
    }

    public int getInlineEpochLimit() {
      return inlineEpochLimit;
    }

    public int getMaxEpochsPerPage() {
      return maxEpochsPerPage;
    }
  }

  /** Completeness identity for one Store in this immutable serving generation. */
  public static final class StoreCoverage {
    private final String dbName;
    private final long indexedFrom;
    private final long indexedThrough;
    private final byte[] headHash;
    private final byte[] authoritativePrefixDigest;

    private StoreCoverage(String dbName, long indexedFrom, long indexedThrough,
        byte[] headHash, byte[] authoritativePrefixDigest) {
      this.dbName = dbName;
      this.indexedFrom = indexedFrom;
      this.indexedThrough = indexedThrough;
      this.headHash = Arrays.copyOf(headHash, headHash.length);
      this.authoritativePrefixDigest = Arrays.copyOf(authoritativePrefixDigest,
          authoritativePrefixDigest.length);
    }

    public String getDbName() {
      return dbName;
    }

    public long getIndexedFrom() {
      return indexedFrom;
    }

    public long getIndexedThrough() {
      return indexedThrough;
    }

    public byte[] getHeadHash() {
      return Arrays.copyOf(headHash, headHash.length);
    }

    public byte[] getAuthoritativePrefixDigest() {
      return Arrays.copyOf(authoritativePrefixDigest,
          authoritativePrefixDigest.length);
    }

    private boolean covers(long targetBlock, long upperBound) {
      return targetBlock >= indexedFrom && upperBound <= indexedThrough;
    }
  }

  private static final class KeyChangeIndex {
    private final long firstChangedEpoch;
    private final long lastChangedEpoch;
    private final long[] inlineEpochs;
    private final List<EpochPage> pages;

    private KeyChangeIndex(long firstChangedEpoch, long lastChangedEpoch, long[] inlineEpochs,
        List<EpochPage> pages) {
      this.firstChangedEpoch = firstChangedEpoch;
      this.lastChangedEpoch = lastChangedEpoch;
      this.inlineEpochs = inlineEpochs;
      this.pages = pages;
    }

    private static KeyChangeIndex from(List<Long> epochs, IndexLayout layout) {
      if (epochs.isEmpty()) {
        throw new IllegalArgumentException("Serving key change list must not be empty");
      }
      long first = epochs.get(0);
      long last = epochs.get(epochs.size() - 1);
      if (epochs.size() <= layout.getInlineEpochLimit()) {
        return new KeyChangeIndex(first, last, toArray(epochs), Collections.emptyList());
      }
      List<EpochPage> pages = new ArrayList<>();
      for (int start = 0; start < epochs.size(); start += layout.getMaxEpochsPerPage()) {
        int end = Math.min(start + layout.getMaxEpochsPerPage(), epochs.size());
        pages.add(new EpochPage(toArray(epochs.subList(start, end))));
      }
      return new KeyChangeIndex(first, last, null,
          Collections.unmodifiableList(pages));
    }

    private OptionalLong firstChangeAfter(long targetBlock, long upperBound) {
      if (lastChangedEpoch <= targetBlock || firstChangedEpoch > upperBound) {
        return OptionalLong.empty();
      }
      if (isInline()) {
        return firstChange(inlineEpochs, targetBlock, upperBound);
      }
      int pageIndex = pageAtOrBefore(targetBlock);
      OptionalLong changed = pages.get(pageIndex).firstChangeAfter(targetBlock, upperBound);
      if (changed.isPresent() || pageIndex + 1 >= pages.size()) {
        return changed;
      }
      return pages.get(pageIndex + 1).firstChangeAfter(targetBlock, upperBound);
    }

    private int pageAtOrBefore(long targetBlock) {
      int low = 0;
      int high = pages.size();
      while (low < high) {
        int middle = (low + high) >>> 1;
        if (pages.get(middle).baseEpoch <= targetBlock) {
          low = middle + 1;
        } else {
          high = middle;
        }
      }
      return Math.max(0, low - 1);
    }

    private boolean isInline() {
      return inlineEpochs != null;
    }

    private int getPageCount() {
      return pages.size();
    }

    private static long[] toArray(List<Long> epochs) {
      long[] result = new long[epochs.size()];
      for (int i = 0; i < epochs.size(); i++) {
        result[i] = epochs.get(i);
      }
      return result;
    }
  }

  private static final class EpochPage {
    private final long baseEpoch;
    private final long maxEpoch;
    private final long[] epochs;

    private EpochPage(long[] epochs) {
      this.epochs = epochs;
      this.baseEpoch = epochs[0];
      this.maxEpoch = epochs[epochs.length - 1];
    }

    private OptionalLong firstChangeAfter(long targetBlock, long upperBound) {
      if (maxEpoch <= targetBlock || baseEpoch > upperBound) {
        return OptionalLong.empty();
      }
      return firstChange(epochs, targetBlock, upperBound);
    }
  }

  private static final class KeyIdentity {
    private final byte[] dbName;
    private final byte[] rawKey;
    private final int hashCode;

    private KeyIdentity(String dbName, byte[] rawKey) {
      this.dbName = dbName.getBytes(StandardCharsets.UTF_8);
      this.rawKey = Arrays.copyOf(rawKey, rawKey.length);
      this.hashCode = 31 * Arrays.hashCode(this.dbName) + Arrays.hashCode(this.rawKey);
    }

    private boolean matchesDatabase(String candidate) {
      return Arrays.equals(dbName, candidate.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof KeyIdentity)) {
        return false;
      }
      KeyIdentity that = (KeyIdentity) other;
      return Arrays.equals(dbName, that.dbName) && Arrays.equals(rawKey, that.rawKey);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
