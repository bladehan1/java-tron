package org.tron.core.db2.archive.serving;

import com.google.common.hash.Hashing;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.bouncycastle.util.encoders.Hex;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.history.io.ArchiveFileDurability;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.Descriptor;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.PersistentStoreCoverage;
import org.tron.core.db2.archive.serving.ServingFailure.Reason;
import org.tron.core.db2.archive.serving.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation;
import org.tron.core.db2.archive.storage.index.StateArchiveIndexEngineManifest;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * The single long-lived write handle of the V5 serving index (ch04 §3.2, WP-S2).
 *
 * <p>Layout of {@code single-v1/keys}: KeyMeta ({@code 0x03}), KeyEpochPage ({@code 0x04}),
 * StoreCoverage ({@code 0x05}), the exact-v5 progress descriptor ({@code 7f 'S' 'I' 01}) and the
 * state record ({@code 7f 'S' 'S' 01}). Every normal batch writes its index data, the 27
 * coverage rows and the descriptor in one {@code sync} batch (S-INV-3). A solo batch is split by
 * key (S-INV-13): all chunks but the last carry only KeyMeta and pages; the last chunk carries
 * the coverage and the descriptor. R-IDEM makes a replay of an interrupted solo idempotent.
 *
 * <p>Only the serving build thread writes. Query leases take native snapshots on this handle
 * through {@link #snapshot()}, never through the static open registry (ch04 §3.11).
 */
final class ServingIndexStore implements Closeable {

  static final String DIRECTORY = "single-v1";
  static final String DATABASE = "keys";
  static final byte[] STATE_KEY = new byte[]{0x7f, 'S', 'S', 1};
  private static final String LEGACY_CATALOG = "current";

  private final Path directory;
  private final Engine engine;
  private final ArchiveIndexStorage.Writer writer;
  private final boolean initialized;
  private final AtomicInteger useAfterClose = new AtomicInteger();
  private volatile Descriptor descriptor;
  private volatile ServingStateRecord state;
  private long sequence;
  private volatile boolean failed;
  private volatile boolean closed;

  private ServingIndexStore(Path directory, Engine engine,
      ArchiveIndexStorage.Writer writer, Descriptor descriptor, ServingStateRecord state,
      boolean initialized) {
    this.directory = directory;
    this.engine = engine;
    this.writer = writer;
    this.descriptor = descriptor;
    this.state = state;
    this.initialized = initialized;
  }

  /**
   * Opens or creates {@code single-v1} and validates everything that does not need History
   * (ENGINE, descriptor version and CRC, the 27 coverage rows, the state record against
   * {@code (B0, hash(B0 - 1))}, {@code indexedFrom == B0 - 1}). A database without descriptor
   * whose only key is the legacy ARCHIVE_TAIL key is an empty database (ch04 §3.4.3).
   *
   * @param markRebuildOnInit sets REBUILD_IN_PROGRESS when a fresh descriptor is written
   *     (History already has blocks, ch04 §3.4.3).
   */
  static ServingIndexStore open(Path directory, Engine engine, long historyStartBlock,
      byte[] baselineParentHash, boolean markRebuildOnInit, long nowMillis)
      throws IOException {
    Path admitted = Objects.requireNonNull(directory, "directory");
    Objects.requireNonNull(engine, "engine");
    requireHash(baselineParentHash);
    if (historyStartBlock < 1) {
      throw new IllegalArgumentException("History start block must be positive");
    }
    Path parent = admitted.toAbsolutePath().getParent();
    if (parent != null && Files.exists(parent.resolve(LEGACY_CATALOG), LinkOption.NOFOLLOW_LINKS)) {
      throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
          "Legacy serving generations (current) require an explicit rebuild");
    }
    if (!Files.isDirectory(admitted, LinkOption.NOFOLLOW_LINKS)) {
      ArchiveFileDurability.createDurably(admitted);
    }
    try {
      StateArchiveIndexEngineManifest.openOrCreate(admitted, engine);
    } catch (IOException incompatible) {
      throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
          "Serving ENGINE identity differs from configuration", incompatible);
    }
    ArchiveIndexStorage.Writer writer;
    try {
      writer = ArchiveIndexStorage.openWriter(admitted.resolve(DATABASE), engine);
    } catch (IOException | RuntimeException openFailure) {
      throw new ServingFaultException(Reason.TRANSIENT_IO,
          "Failed to open the serving index database", openFailure);
    }
    try {
      ServingIndexStore store = openWithWriter(admitted, engine, writer, historyStartBlock,
          baselineParentHash, markRebuildOnInit, nowMillis);
      ArchiveFileDurability.syncDirectory(admitted);
      return store;
    } catch (IOException | RuntimeException failure) {
      try {
        writer.close();
      } catch (IOException | RuntimeException closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  private static ServingIndexStore openWithWriter(Path directory, Engine engine,
      ArchiveIndexStorage.Writer writer, long historyStartBlock, byte[] baseHash,
      boolean markRebuildOnInit, long nowMillis) throws IOException {
    byte[] encoded = writer.get(PersistentServingKeyIndexGeneration.PROGRESS_KEY);
    if (encoded == null) {
      requireOnlyLegacyTail(writer);
      Descriptor initial = initialDescriptor(historyStartBlock - 1, baseHash);
      ServingStateRecord record = ServingStateRecord.of(historyStartBlock, baseHash,
          markRebuildOnInit, markRebuildOnInit ? nowMillis : 0);
      List<Mutation> batch = new ArrayList<>(coverage(initial));
      batch.add(ArchiveIndexStorage.put(PersistentServingKeyIndexGeneration.PROGRESS_KEY,
          PersistentServingKeyIndexGeneration.encodeDescriptor(initial)));
      batch.add(ArchiveIndexStorage.put(STATE_KEY, record.encode()));
      batch.add(ArchiveIndexStorage.delete(
          PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5));
      writer.write(batch, true);
      return new ServingIndexStore(directory, engine, writer, initial, record, true);
    }
    Descriptor descriptor = decode(encoded);
    if (descriptor.indexedFrom() != historyStartBlock - 1) {
      throw new ServingFaultException(Reason.IDENTITY,
          "Serving indexedFrom differs from History B0 - 1");
    }
    try (ArchiveIndexStorage.Reader snapshot = writer.openSnapshot()) {
      PersistentServingKeyIndexGeneration.validateExactStoreCoverage(snapshot, descriptor);
    } catch (ArchivePersistenceException corrupt) {
      throw new ServingFaultException(Reason.INDEX_CORRUPT,
          "Serving Store coverage differs from its progress descriptor", corrupt);
    }
    byte[] encodedState = writer.get(STATE_KEY);
    ServingStateRecord record;
    List<Mutation> maintenance = new ArrayList<>();
    if (encodedState == null) {
      // A PR #55 database: bind it to the History root it was built from (ch04 §3.2.2).
      record = ServingStateRecord.of(historyStartBlock, baseHash, false, 0);
      maintenance.add(ArchiveIndexStorage.put(STATE_KEY, record.encode()));
    } else {
      record = ServingStateRecord.decode(encodedState);
      if (record.getHistoryStartBlock() != historyStartBlock
          || !Arrays.equals(record.getHistoryBaseHash(), baseHash)) {
        throw new ServingFaultException(Reason.IDENTITY,
            "Serving state record belongs to a different History instance");
      }
    }
    if (writer.get(PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5) != null) {
      // Migration step 3 (ch04 §3.3.3): the History tail file is authoritative by now.
      maintenance.add(ArchiveIndexStorage.delete(
          PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5));
    }
    if (!maintenance.isEmpty()) {
      writer.write(maintenance, true);
    }
    return new ServingIndexStore(directory, engine, writer, descriptor, record, false);
  }

  private static void requireOnlyLegacyTail(ArchiveIndexStorage.Writer writer)
      throws IOException {
    try (ArchiveIndexStorage.Reader snapshot = writer.openSnapshot();
        ArchiveIndexStorage.Cursor cursor = snapshot.cursor()) {
      cursor.seek(new byte[0]);
      ArchiveIndexStorage.KeyValue entry;
      while ((entry = cursor.next()) != null) {
        if (!Arrays.equals(entry.getKey(), PersistentServingKeyIndexGeneration.ARCHIVE_TAIL_KEY_V5)
            && !Arrays.equals(entry.getKey(), STATE_KEY)) {
          throw new ServingFaultException(Reason.INDEX_CORRUPT,
              "Serving data exists without a progress descriptor");
        }
      }
    }
  }

  private static Descriptor decode(byte[] encoded) throws ServingFaultException {
    Descriptor descriptor;
    try {
      descriptor = PersistentServingKeyIndexGeneration.decodeDescriptor(encoded);
    } catch (IllegalArgumentException invalid) {
      String message = String.valueOf(invalid.getMessage());
      throw new ServingFaultException(message.contains("Unsupported")
          ? Reason.INDEX_INCOMPATIBLE : Reason.INDEX_CORRUPT,
          "Serving progress descriptor is invalid: " + message, invalid);
    }
    if (descriptor.formatVersion() != PersistentServingKeyIndexGeneration.EXACT_VERSION) {
      throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
          "Serving progress descriptor version is not exact v5");
    }
    if (!descriptor.participants().equals(exactParticipants())) {
      throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
          "Serving participant scope differs from exact-27");
    }
    return descriptor;
  }

  private static List<String> exactParticipants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    java.util.Collections.sort(participants);
    return java.util.Collections.unmodifiableList(participants);
  }

  private static Descriptor initialDescriptor(long base, byte[] baseHash) {
    List<String> participants = exactParticipants();
    byte[] seed = ServingIndexIncrementalPlan.sourceSeed(base, baseHash, participants);
    return new Descriptor(PersistentServingKeyIndexGeneration.EXACT_VERSION,
        ArchiveStoreScope.getFormatId(), generationId(base, baseHash, 0), base, base,
        baseHash, seed, new byte[32], participants, 0);
  }

  private static List<Mutation> coverage(Descriptor descriptor) {
    List<Mutation> rows = new ArrayList<>(descriptor.participants().size());
    for (String database : descriptor.participants()) {
      PersistentStoreCoverage coverage = new PersistentStoreCoverage(database,
          descriptor.indexedFrom(), descriptor.indexedThrough(), descriptor.headHash(),
          descriptor.sourceDigest(), descriptor.generationId(),
          PersistentServingKeyIndexGeneration.comparatorId(database));
      rows.add(ArchiveIndexStorage.put(
          PersistentServingKeyIndexGeneration.storeCoverageKey(database),
          PersistentServingKeyIndexGeneration.encodeCoverage(coverage)));
    }
    return rows;
  }

  private static String generationId(long block, byte[] hash, long sequence) {
    return String.format("append-v3-%020d-%s-%08d", block,
        Hex.toHexString(Arrays.copyOf(hash, 6)), sequence + 1);
  }

  boolean isInitialized() {
    return initialized;
  }

  long indexedFrom() {
    return descriptor.indexedFrom();
  }

  long indexedThrough() {
    return descriptor.indexedThrough();
  }

  byte[] headHash() {
    return descriptor.headHash();
  }

  long keyChanges() {
    return descriptor.keyChanges();
  }

  boolean isRebuildInProgress() {
    return state.isRebuildInProgress();
  }

  ServingStateRecord stateRecord() {
    return state;
  }

  Path getDirectory() {
    return directory;
  }

  Engine getEngine() {
    return engine;
  }

  /** Native snapshot on the write handle for a lease (ch04 §3.10.1 step 3). */
  ArchiveIndexStorage.Reader snapshot() throws IOException {
    if (closed) {
      useAfterClose.incrementAndGet();
      throw new IllegalStateException("Serving index handle used after close");
    }
    return writer.openSnapshot();
  }

  /** Reads and validates the progress descriptor in {@code snapshot} (lease step 4). */
  static Descriptor readDescriptor(ArchiveIndexStorage.Reader snapshot)
      throws IOException {
    byte[] encoded = snapshot.get(PersistentServingKeyIndexGeneration.PROGRESS_KEY);
    if (encoded == null) {
      throw new ServingFaultException(Reason.INDEX_CORRUPT,
          "Serving snapshot has no progress descriptor");
    }
    return decode(encoded);
  }

  /** Test hook (T21): calls made on this handle after close. */
  int useAfterCloseCount() {
    return useAfterClose.get();
  }

  /** Writes only a cleared state record (REBUILDING that already reached I == W at open). */
  void clearRebuildFlag() throws IOException {
    requireWritable();
    ServingStateRecord cleared = state.cleared();
    write(java.util.Collections.singletonList(
        ArchiveIndexStorage.put(STATE_KEY, cleared.encode())));
    state = cleared;
  }

  /**
   * Commits one normal batch atomically, or returns {@code false} without writing anything when
   * its mutations exceed the write budget (the caller then halves the batch, ch04 §3.5.3).
   */
  boolean commit(ServingIndexIncrementalPlan plan, WriteBudget budget, boolean clearRebuild)
      throws IOException {
    requireWritable();
    requireExtends(plan);
    List<List<Mutation>> groups = keyGroups(plan);
    Next next = next(plan, clearRebuild);
    long count = next.tail.size();
    long bytes = estimatedBytes(next.tail);
    for (List<Mutation> group : groups) {
      count += group.size();
      bytes += estimatedBytes(group);
      if (count > budget.maxMutations || bytes > budget.maxBytes) {
        return false;
      }
    }
    List<Mutation> batch = new ArrayList<>((int) count);
    groups.forEach(batch::addAll);
    batch.addAll(next.tail);
    write(batch);
    install(next);
    return true;
  }

  /**
   * Commits a solo batch of exactly one block split into key-atomic chunks (ch04 §3.5.4). Every
   * chunk but the last carries only KeyMeta and pages; the last carries coverage and progress.
   *
   * @return the number of physical write batches
   */
  int commitSolo(ServingIndexIncrementalPlan plan, WriteBudget budget, boolean clearRebuild,
      ChunkHook hook) throws IOException {
    requireWritable();
    requireExtends(plan);
    if (plan.getIndexedThrough() != descriptor.indexedThrough() + 1) {
      throw new IllegalArgumentException("A solo serving batch covers exactly one block");
    }
    List<List<Mutation>> groups = keyGroups(plan);
    Next next = next(plan, clearRebuild);
    List<List<Mutation>> chunks = new ArrayList<>();
    List<Mutation> current = new ArrayList<>();
    long count = 0;
    long bytes = 0;
    for (List<Mutation> group : groups) {
      long groupBytes = estimatedBytes(group);
      if (!current.isEmpty() && (count + group.size() > budget.maxMutations
          || bytes + groupBytes > budget.maxBytes)) {
        chunks.add(current);
        current = new ArrayList<>();
        count = 0;
        bytes = 0;
      }
      current.addAll(group);
      count += group.size();
      bytes += groupBytes;
    }
    long tailBytes = estimatedBytes(next.tail);
    if (!current.isEmpty() && (count + next.tail.size() > budget.maxMutations
        || bytes + tailBytes > budget.maxBytes)) {
      chunks.add(current);
      current = new ArrayList<>();
    }
    current.addAll(next.tail);
    chunks.add(current);
    for (int index = 0; index < chunks.size(); index++) {
      hook.beforeChunk(index, chunks.size());
      write(chunks.get(index));
    }
    install(next);
    return chunks.size();
  }

  private void requireExtends(ServingIndexIncrementalPlan plan) {
    Descriptor current = descriptor;
    if (plan.getIndexedFrom() != current.indexedThrough()
        || !Arrays.equals(plan.getIndexedFromHash(), current.headHash())
        || !plan.getParticipatingDatabases().equals(current.participants())) {
      throw new IllegalStateException("Serving increment does not extend durable I");
    }
  }

  /** Key-atomic mutation groups (S-INV-13): each group is one key's pages then its KeyMeta. */
  private List<List<Mutation>> keyGroups(ServingIndexIncrementalPlan plan) throws IOException {
    Map<ExactKey, List<Long>> changes = new LinkedHashMap<>();
    for (Map.Entry<String, List<ServingIndexIncrementalPlan.KeyChange>> database
        : plan.getChangesByDatabase().entrySet()) {
      for (ServingIndexIncrementalPlan.KeyChange change : database.getValue()) {
        changes.computeIfAbsent(new ExactKey(database.getKey(), change.getRawKey()),
            ignored -> new ArrayList<>()).add(change.getEpoch());
      }
    }
    long durable = descriptor.indexedThrough();
    List<List<Mutation>> groups = new ArrayList<>(changes.size());
    long started = System.nanoTime();
    for (Map.Entry<ExactKey, List<Long>> entry : changes.entrySet()) {
      List<Mutation> group = new ArrayList<>(2);
      PersistentServingKeyIndexGeneration.appendExactChanges(writer, group, entry.getKey(),
          entry.getValue(), durable);
      if (!group.isEmpty()) {
        groups.add(group);
      }
    }
    ServingIndexTiming.record(ServingIndexTiming.Stage.BUILD, started);
    return groups;
  }

  private Next next(ServingIndexIncrementalPlan plan, boolean clearRebuild) {
    Descriptor previous = descriptor;
    long added = 0;
    for (List<ServingIndexIncrementalPlan.KeyChange> database
        : plan.getChangesByDatabase().values()) {
      added += database.size();
    }
    List<byte[]> steps = plan.getSourceStepDigests();
    byte[] digest = PersistentServingKeyIndexGeneration.rollSourceDigest(
        previous.sourceDigest(), steps);
    // S14: latestSourceIdentity is diagnostic only, the last block's step digest.
    byte[] latest = steps.isEmpty() ? previous.latestSourceIdentityDigest()
        : steps.get(steps.size() - 1);
    Descriptor replacement = new Descriptor(PersistentServingKeyIndexGeneration.EXACT_VERSION,
        previous.scopeIdentity(), generationId(plan.getIndexedThrough(),
            plan.getHeadHash(), sequence), previous.indexedFrom(), plan.getIndexedThrough(),
        plan.getHeadHash(), digest, latest, previous.participants(),
        previous.keyChanges() + added);
    List<Mutation> tail = new ArrayList<>(coverage(replacement));
    tail.add(ArchiveIndexStorage.put(PersistentServingKeyIndexGeneration.PROGRESS_KEY,
        PersistentServingKeyIndexGeneration.encodeDescriptor(replacement)));
    ServingStateRecord record = state;
    if (clearRebuild && record.isRebuildInProgress()) {
      record = record.cleared();
      tail.add(ArchiveIndexStorage.put(STATE_KEY, record.encode()));
    }
    return new Next(replacement, record, tail);
  }

  private void install(Next next) {
    descriptor = next.descriptor;
    state = next.state;
    sequence++;
  }

  private void write(List<Mutation> batch) throws IOException {
    long started = System.nanoTime();
    try {
      writer.write(batch, true);
    } catch (IOException | RuntimeException failure) {
      // A lost acknowledgement may have committed: only reopening resolves durable I.
      failed = true;
      throw failure;
    } finally {
      ServingIndexTiming.record(ServingIndexTiming.Stage.WRITE_SYNC, started);
    }
  }

  private void requireWritable() throws IOException {
    if (closed) {
      useAfterClose.incrementAndGet();
      throw new IllegalStateException("Serving index handle used after close");
    }
    if (failed) {
      throw new ServingFaultException(Reason.TRANSIENT_IO,
          "Serving index handle failed a write; reopen required");
    }
  }

  private static long estimatedBytes(List<Mutation> mutations) {
    long bytes = 0;
    for (Mutation mutation : mutations) {
      bytes += mutation.estimatedBytes();
    }
    return bytes;
  }

  private static void requireHash(byte[] hash) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException("Serving baseline hash must be 32 bytes");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      writer.close();
    }
  }

  /** Write budget used for both normal batches and solo chunks (ch04 §3.5.2). */
  static final class WriteBudget {
    private final long maxMutations;
    private final long maxBytes;

    WriteBudget(long maxMutations, long maxBytes) {
      if (maxMutations < 64 || maxBytes < 64 * 1024) {
        throw new IllegalArgumentException("Serving write budget is too small");
      }
      this.maxMutations = maxMutations;
      this.maxBytes = maxBytes;
    }
  }

  @FunctionalInterface
  interface ChunkHook {
    ChunkHook NONE = (index, count) -> { };

    void beforeChunk(int index, int count) throws IOException;
  }

  private static final class Next {
    private final Descriptor descriptor;
    private final ServingStateRecord state;
    private final List<Mutation> tail;

    private Next(Descriptor descriptor, ServingStateRecord state, List<Mutation> tail) {
      this.descriptor = descriptor;
      this.state = state;
      this.tail = tail;
    }
  }

  /** The serving state record (ch04 §3.2.2), 60 bytes big-endian with a CRC32C. */
  static final class ServingStateRecord {
    static final int MAGIC = 0x53565354; // SVST
    static final short VERSION = 1;
    static final short REBUILD_IN_PROGRESS = 1;
    static final int LENGTH = 60;

    private final short flags;
    private final long historyStartBlock;
    private final byte[] historyBaseHash;
    private final long rebuildStartedAtMillis;

    private ServingStateRecord(short flags, long historyStartBlock, byte[] historyBaseHash,
        long rebuildStartedAtMillis) {
      this.flags = flags;
      this.historyStartBlock = historyStartBlock;
      this.historyBaseHash = Arrays.copyOf(historyBaseHash, historyBaseHash.length);
      this.rebuildStartedAtMillis = rebuildStartedAtMillis;
    }

    static ServingStateRecord of(long historyStartBlock, byte[] historyBaseHash,
        boolean rebuild, long startedMillis) {
      return new ServingStateRecord(rebuild ? REBUILD_IN_PROGRESS : 0, historyStartBlock,
          historyBaseHash, rebuild ? startedMillis : 0);
    }

    ServingStateRecord cleared() {
      return new ServingStateRecord((short) 0, historyStartBlock, historyBaseHash, 0);
    }

    boolean isRebuildInProgress() {
      return (flags & REBUILD_IN_PROGRESS) != 0;
    }

    long getHistoryStartBlock() {
      return historyStartBlock;
    }

    byte[] getHistoryBaseHash() {
      return Arrays.copyOf(historyBaseHash, historyBaseHash.length);
    }

    long getRebuildStartedAtMillis() {
      return rebuildStartedAtMillis;
    }

    byte[] encode() {
      ByteBuffer buffer = ByteBuffer.allocate(LENGTH).putInt(MAGIC).putShort(VERSION)
          .putShort(flags).putLong(historyStartBlock).put(historyBaseHash)
          .putLong(rebuildStartedAtMillis);
      buffer.putInt(Hashing.crc32c().hashBytes(buffer.array(), 0, LENGTH - 4).asInt());
      return buffer.array();
    }

    static ServingStateRecord decode(byte[] encoded) throws ServingFaultException {
      if (encoded == null || encoded.length != LENGTH) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving state record length is invalid");
      }
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      if (buffer.getInt(LENGTH - 4)
          != Hashing.crc32c().hashBytes(encoded, 0, LENGTH - 4).asInt()) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving state record checksum differs");
      }
      if (buffer.getInt() != MAGIC) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT, "Serving state record magic");
      }
      short version = buffer.getShort();
      if (version != VERSION) {
        throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
            "Serving state record version is unsupported");
      }
      short flags = buffer.getShort();
      long start = buffer.getLong();
      byte[] hash = new byte[32];
      buffer.get(hash);
      long started = buffer.getLong();
      if ((flags & ~REBUILD_IN_PROGRESS) != 0 || start < 1) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving state record fields are invalid");
      }
      return new ServingStateRecord(flags, start, hash, started);
    }
  }
}
