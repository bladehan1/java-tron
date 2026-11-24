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
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;
import org.tron.core.db2.archive.serving.status.ServingFailure.Reason;
import org.tron.core.db2.archive.serving.status.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.serving.telemetry.ServingIndexTiming;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage.Mutation;
import org.tron.core.db2.archive.storage.index.StateArchiveIndexEngineManifest;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/**
 * The single long-lived write handle of the V5 serving index (ch04 §3.2, WP-S2).
 *
 * <p>Layout of {@code single-v1/keys}: KeyMeta ({@code 0x03}), KeyEpochPage ({@code 0x04}),
 * StoreCoverage ({@code 0x05}), the exact-v5 progress descriptor ({@code 7f 'S' 'I' 01}), the
 * state record ({@code 7f 'S' 'S' 01}) and the transient flush intent ({@code 7f 'S' 'F' 01}).
 * The page and KeyMeta encodings are unchanged from PR #55, so an existing index opens and
 * keeps appending from its durable progress. A flush fitting one write still commits its index
 * data, the 27 coverage rows and the descriptor in one {@code sync} batch (S-INV-3); a larger
 * flush writes a durable intent and key-atomic data batches first and publishes coverage and
 * progress last. R-IDEM still recovers interrupted solo writes by older versions.
 *
 * <p>Only the serving build thread writes. Query leases take native snapshots on this handle
 * through {@link #snapshot()}, never through the static open registry (ch04 §3.11).
 */
final class ServingIndexStore implements Closeable {

  static final String DIRECTORY = "single-v1";
  static final String DATABASE = "keys";
  static final byte[] STATE_KEY = new byte[]{0x7f, 'S', 'S', 1};
  /** Crash recovery intent of a multi-write flush; absent once the final publish is durable. */
  static final byte[] FLUSH_INTENT_KEY = new byte[]{0x7f, 'S', 'F', 1};
  private static final String LEGACY_CATALOG = "current";

  private final Path directory;
  private final Engine engine;
  private final ArchiveIndexStorage.Writer writer;
  private final boolean initialized;
  private final ServingIndexParallelAppender appender;
  private final AtomicInteger useAfterClose = new AtomicInteger();
  private volatile Descriptor descriptor;
  private volatile ServingStateRecord state;
  private volatile FlushIntent pendingIntent;
  private long sequence;
  private volatile boolean failed;
  private volatile boolean closed;
  private volatile FlushFaultHook faultHook = FlushFaultHook.NONE;

  private ServingIndexStore(Path directory, Engine engine,
      ArchiveIndexStorage.Writer writer, Descriptor descriptor, ServingStateRecord state,
      boolean initialized, FlushIntent pendingIntent) {
    this.directory = directory;
    this.engine = engine;
    this.writer = writer;
    this.descriptor = descriptor;
    this.state = state;
    this.initialized = initialized;
    this.pendingIntent = pendingIntent;
    this.appender = new ServingIndexParallelAppender(
        ServingIndexParallelAppender.resolveParallelism());
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
      return new ServingIndexStore(directory, engine, writer, initial, record, true, null);
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
    FlushIntent pendingIntent = readPendingIntent(writer, descriptor);
    return new ServingIndexStore(directory, engine, writer, descriptor, record, false,
        pendingIntent);
  }

  /** Decodes and validates the recovery intent of an interrupted multi-write flush. */
  private static FlushIntent readPendingIntent(ArchiveIndexStorage.Writer writer,
      Descriptor descriptor) throws IOException {
    byte[] encoded = writer.get(FLUSH_INTENT_KEY);
    if (encoded == null) {
      return null;
    }
    FlushIntent intent = FlushIntent.decode(encoded);
    if (intent.baseThrough != descriptor.indexedThrough()
        || !Arrays.equals(intent.baseHash, descriptor.headHash())) {
      throw new ServingFaultException(Reason.INDEX_CORRUPT,
          "Serving flush intent does not match the durable progress descriptor");
    }
    if (intent.targetThrough <= intent.baseThrough) {
      throw new ServingFaultException(Reason.INDEX_CORRUPT,
          "Serving flush intent target is invalid");
    }
    return intent;
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

  /** All data and progress are atomic; soft flush estimates never reject a valid plan. */
  void commit(ServingIndexIncrementalPlan plan, boolean clearRebuild) throws IOException {
    long grouped = System.nanoTime();
    GroupedAppend append = toGroupedAppend(plan);
    ServingIndexTiming.record(ServingIndexTiming.Stage.GROUP, grouped);
    // A plan-sized flush never exceeds one write, so it keeps today's single atomic commit.
    commit(append, clearRebuild, Long.MAX_VALUE);
  }

  /**
   * Commits one sealed accumulation. Small flushes stay one atomic write; a flush larger than
   * {@code dataBatchBytes} streams key-atomic data batches after a durable flush intent and
   * publishes coverage and progress in the final write, Geth-style data-then-progress.
   */
  void commit(GroupedAppend append, boolean clearRebuild, long dataBatchBytes)
      throws IOException {
    requireWritable();
    requireExtends(append);
    Next next = next(append, clearRebuild);
    FlushFlow flow = new FlushFlow(append, next, clearRebuild, dataBatchBytes, false);
    long started = System.nanoTime();
    try {
      appender.appendAll(writer, append.changes, descriptor.indexedThrough(), flow);
      flow.finish();
    } catch (IOException | RuntimeException | Error failure) {
      if (flow.durableWrites()) {
        // Intent or data batches may already be durable; only reopening may resume.
        failed = true;
      }
      throw failure;
    }
    ServingIndexTiming.record(ServingIndexTiming.Stage.BUILD, started);
    install(next);
  }

  /**
   * Resumes an interrupted multi-write flush: keys still at the old boundary append normally,
   * keys the crash already wrote are verified against the replay and skipped, then the final
   * publish batch installs coverage, progress and deletes the intent. Replaying is idempotent
   * and may itself be interrupted.
   */
  void recoverFlush(GroupedAppend append, FlushIntent intent, long dataBatchBytes)
      throws IOException {
    requireWritable();
    requireExtends(append);
    if (intent.baseThrough != append.indexedFrom
        || !Arrays.equals(intent.baseHash, append.indexedFromHash)
        || intent.targetThrough != append.indexedThrough
        || !Arrays.equals(intent.targetHash, append.headHash)
        || !Arrays.equals(intent.targetSourceDigest, append.sourceDigest)
        || intent.keyChanges != append.keyChanges) {
      throw new ServingFaultException(Reason.INDEX_CORRUPT,
          "Serving flush recovery replay differs from the pending intent");
    }
    Next next = next(append, intent.clearRebuild);
    FlushFlow flow = new FlushFlow(append, next, intent.clearRebuild, dataBatchBytes, true);
    long started = System.nanoTime();
    try {
      appender.recoverAll(writer, append.changes, intent.baseThrough, intent.targetThrough,
          flow);
      flow.finish();
    } catch (IOException | RuntimeException | Error failure) {
      if (flow.durableWrites()) {
        failed = true;
      }
      throw failure;
    }
    pendingIntent = null;
    ServingIndexTiming.record(ServingIndexTiming.Stage.BUILD, started);
    install(next);
  }

  /** The recovery replay target of an interrupted flush, or null when the last one completed. */
  FlushIntent pendingFlushIntent() {
    return pendingIntent;
  }

  byte[] sourceDigest() {
    return descriptor.sourceDigest();
  }

  byte[] latestIdentityDigest() {
    return descriptor.latestSourceIdentityDigest();
  }

  List<String> participantsList() {
    return descriptor.participants();
  }

  /** Test injection points around the multi-write flush protocol; production keeps NONE. */
  void setFlushFaultHook(FlushFaultHook hook) {
    this.faultHook = hook == null ? FlushFaultHook.NONE : hook;
  }

  private GroupedAppend toGroupedAppend(ServingIndexIncrementalPlan plan) {
    Map<ExactKey, List<Long>> grouped = new LinkedHashMap<>();
    for (Map.Entry<String, List<ServingIndexIncrementalPlan.KeyChange>> database
        : plan.getChangesByDatabase().entrySet()) {
      for (ServingIndexIncrementalPlan.KeyChange change : database.getValue()) {
        grouped.computeIfAbsent(ExactKey.owned(database.getKey(), change.getRawKey()),
            ignored -> new ArrayList<>()).add(change.getEpoch());
      }
    }
    Map<ExactKey, long[]> changes = new LinkedHashMap<>();
    grouped.forEach((key, epochs) -> {
      long[] values = new long[epochs.size()];
      for (int index = 0; index < values.length; index++) {
        values[index] = epochs.get(index);
      }
      changes.put(key, values);
    });
    List<byte[]> steps = plan.getSourceStepDigests();
    byte[] latest = steps.isEmpty() ? descriptor.latestSourceIdentityDigest()
        : steps.get(steps.size() - 1);
    return new GroupedAppend(plan.getIndexedFrom(), plan.getIndexedFromHash(),
        plan.getIndexedThrough(), plan.getHeadHash(), plan.getParticipatingDatabases(),
        PersistentServingKeyIndexGeneration.rollSourceDigest(
            descriptor.sourceDigest(), steps),
        latest, totalChanges(plan), changes);
  }

  private static long totalChanges(ServingIndexIncrementalPlan plan) {
    long total = 0;
    for (List<ServingIndexIncrementalPlan.KeyChange> database
        : plan.getChangesByDatabase().values()) {
      total += database.size();
    }
    return total;
  }

  private void requireExtends(ServingIndexIncrementalPlan plan) {
    Descriptor current = descriptor;
    if (plan.getIndexedFrom() != current.indexedThrough()
        || !Arrays.equals(plan.getIndexedFromHash(), current.headHash())
        || !plan.getParticipatingDatabases().equals(current.participants())) {
      throw new IllegalStateException("Serving increment does not extend durable I");
    }
  }

  private void requireExtends(GroupedAppend append) {
    Descriptor current = descriptor;
    if (append.indexedFrom != current.indexedThrough()
        || !Arrays.equals(append.indexedFromHash, current.headHash())
        || !append.participants.equals(current.participants())) {
      throw new IllegalStateException("Serving increment does not extend durable I");
    }
  }

  private Next next(GroupedAppend append, boolean clearRebuild) {
    Descriptor previous = descriptor;
    Descriptor replacement = new Descriptor(PersistentServingKeyIndexGeneration.EXACT_VERSION,
        previous.scopeIdentity(), generationId(append.indexedThrough, append.headHash, sequence),
        previous.indexedFrom(), append.indexedThrough, append.headHash, append.sourceDigest,
        append.latestStepDigest, previous.participants(),
        previous.keyChanges() + append.keyChanges);
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

  /**
   * Batches prepared key chunks into writes. A flush fitting {@code batchBytes} keeps data,
   * coverage and progress in one atomic write, exactly like a plan-sized commit. A larger
   * flush first writes a durable intent, then key-atomic data batches, and publishes coverage
   * and progress in the final write that also deletes the intent. Chunks arrive in worker
   * completion order; each key's mutations always share one write batch, so the order between
   * distinct keys never matters.
   */
  private final class FlushFlow implements ServingIndexParallelAppender.ChunkSink {
    private final GroupedAppend append;
    private final Next next;
    private final boolean clearRebuild;
    private final long batchBytes;
    /** An interrupted flush left an intent on disk; any completion path must remove it. */
    private final boolean existingIntent;
    private final List<Mutation> current = new ArrayList<>();
    private long currentBytes;
    private long totalBytes;
    private long dataBatches;
    private boolean intentWritten;

    private FlushFlow(GroupedAppend append, Next next, boolean clearRebuild, long batchBytes,
        boolean existingIntent) {
      this.append = append;
      this.next = next;
      this.clearRebuild = clearRebuild;
      this.batchBytes = batchBytes;
      this.existingIntent = existingIntent;
    }

    @Override
    public void accept(List<Mutation> chunk) throws IOException {
      long chunkBytes = 0;
      for (Mutation mutation : chunk) {
        chunkBytes += mutation.estimatedBytes();
      }
      totalBytes += chunkBytes;
      if (!current.isEmpty() && currentBytes + chunkBytes > batchBytes) {
        if (!intentWritten) {
          writeIntent();
        }
        writeData();
      }
      current.addAll(chunk);
      currentBytes += chunkBytes;
      if (intentWritten && currentBytes >= batchBytes) {
        writeData();
      }
    }

    void finish() throws IOException {
      if (!intentWritten) {
        // Everything fits one write: data, coverage and progress stay atomic together; a
        // recovering flush still clears the intent the interrupted attempt left behind.
        for (Mutation mutation : next.tail) {
          totalBytes += mutation.estimatedBytes();
        }
        current.addAll(next.tail);
        if (existingIntent) {
          Mutation delete = ArchiveIndexStorage.delete(FLUSH_INTENT_KEY);
          totalBytes += delete.estimatedBytes();
          current.add(delete);
        }
        write(current);
        return;
      }
      if (!current.isEmpty()) {
        writeData();
      }
      faultHook.beforeFinalPublish();
      List<Mutation> finalBatch = new ArrayList<>(next.tail);
      for (Mutation mutation : next.tail) {
        totalBytes += mutation.estimatedBytes();
      }
      Mutation delete = ArchiveIndexStorage.delete(FLUSH_INTENT_KEY);
      totalBytes += delete.estimatedBytes();
      finalBatch.add(delete);
      write(finalBatch);
    }

    boolean durableWrites() {
      return intentWritten;
    }

    long totalBytes() {
      return totalBytes;
    }

    private void writeIntent() throws IOException {
      Mutation intent = ArchiveIndexStorage.put(FLUSH_INTENT_KEY,
          new FlushIntent(append.indexedFrom, append.indexedFromHash, append.indexedThrough,
              append.headHash, append.sourceDigest, append.keyChanges, clearRebuild).encode());
      totalBytes += intent.estimatedBytes();
      write(java.util.Collections.singletonList(intent));
      intentWritten = true;
      faultHook.afterIntent();
    }

    private void writeData() throws IOException {
      write(current);
      dataBatches++;
      faultHook.afterDataBatch((int) dataBatches - 1);
      current.clear();
      currentBytes = 0;
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

  private static void requireHash(byte[] hash) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException("Serving baseline hash must be 32 bytes");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        appender.close();
      } finally {
        writer.close();
      }
    }
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

  /** One sealed change accumulation: identity, rolled digest and per-key ordered epochs. */
  static final class GroupedAppend {
    private final long indexedFrom;
    private final byte[] indexedFromHash;
    private final long indexedThrough;
    private final byte[] headHash;
    private final List<String> participants;
    /** Source digest rolled from the base descriptor through every step of this batch. */
    private final byte[] sourceDigest;
    private final byte[] latestStepDigest;
    private final long keyChanges;
    private final Map<ExactKey, long[]> changes;

    GroupedAppend(long indexedFrom, byte[] indexedFromHash, long indexedThrough,
        byte[] headHash, List<String> participants, byte[] sourceDigest,
        byte[] latestStepDigest, long keyChanges, Map<ExactKey, long[]> changes) {
      if (indexedFrom < 0 || indexedThrough < indexedFrom) {
        throw new IllegalArgumentException("Serving append range is invalid");
      }
      this.indexedFrom = indexedFrom;
      this.indexedFromHash = Objects.requireNonNull(indexedFromHash, "indexedFromHash").clone();
      this.indexedThrough = indexedThrough;
      this.headHash = Objects.requireNonNull(headHash, "headHash").clone();
      this.participants = java.util.Collections.unmodifiableList(
          new ArrayList<>(Objects.requireNonNull(participants, "participants")));
      this.sourceDigest = Objects.requireNonNull(sourceDigest, "sourceDigest").clone();
      this.latestStepDigest = Objects.requireNonNull(latestStepDigest,
          "latestStepDigest").clone();
      this.keyChanges = keyChanges;
      this.changes = java.util.Collections.unmodifiableMap(
          new LinkedHashMap<>(Objects.requireNonNull(changes, "changes")));
    }

    long indexedFrom() {
      return indexedFrom;
    }

    long indexedThrough() {
      return indexedThrough;
    }

    byte[] headHash() {
      return headHash.clone();
    }

    byte[] sourceDigest() {
      return sourceDigest.clone();
    }

    long keyChanges() {
      return keyChanges;
    }
  }

  /**
   * The durable intent of an interrupted multi-write flush: while it exists, data between the
   * durable progress and {@code targetThrough} may be partly written and must be recovered by
   * replay before normal building resumes.
   */
  static final class FlushIntent {
    private static final int MAGIC = 0x53465449; // SFTI
    private static final short VERSION = 1;
    private static final int LENGTH = 132;

    private final long baseThrough;
    private final byte[] baseHash;
    private final long targetThrough;
    private final byte[] targetHash;
    private final byte[] targetSourceDigest;
    private final long keyChanges;
    private final boolean clearRebuild;

    private FlushIntent(long baseThrough, byte[] baseHash, long targetThrough,
        byte[] targetHash, byte[] targetSourceDigest, long keyChanges, boolean clearRebuild) {
      this.baseThrough = baseThrough;
      this.baseHash = baseHash.clone();
      this.targetThrough = targetThrough;
      this.targetHash = targetHash.clone();
      this.targetSourceDigest = targetSourceDigest.clone();
      this.keyChanges = keyChanges;
      this.clearRebuild = clearRebuild;
    }

    private byte[] encode() {
      ByteBuffer buffer = ByteBuffer.allocate(LENGTH).putInt(MAGIC).putShort(VERSION)
          .putShort(clearRebuild ? (short) 1 : 0).putLong(baseThrough).put(baseHash)
          .putLong(targetThrough).put(targetHash).put(targetSourceDigest)
          .putLong(keyChanges);
      buffer.putInt(Hashing.crc32c().hashBytes(buffer.array(), 0, LENGTH - 4).asInt());
      return buffer.array();
    }

    private static FlushIntent decode(byte[] encoded) throws ServingFaultException {
      if (encoded == null || encoded.length != LENGTH) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving flush intent length is invalid");
      }
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      if (buffer.getInt(LENGTH - 4)
          != Hashing.crc32c().hashBytes(encoded, 0, LENGTH - 4).asInt()) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving flush intent checksum differs");
      }
      if (buffer.getInt() != MAGIC) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT, "Serving flush intent magic");
      }
      short version = buffer.getShort();
      if (version != VERSION) {
        throw new ServingFaultException(Reason.INDEX_INCOMPATIBLE,
            "Serving flush intent version is unsupported");
      }
      short flags = buffer.getShort();
      if ((flags & ~1) != 0) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving flush intent flags are invalid");
      }
      long base = buffer.getLong();
      byte[] baseHash = new byte[32];
      buffer.get(baseHash);
      long target = buffer.getLong();
      byte[] targetHash = new byte[32];
      buffer.get(targetHash);
      byte[] digest = new byte[32];
      buffer.get(digest);
      long keyChanges = buffer.getLong();
      if (base < 0 || target <= base || keyChanges < 0) {
        throw new ServingFaultException(Reason.INDEX_CORRUPT,
            "Serving flush intent fields are invalid");
      }
      return new FlushIntent(base, baseHash, target, targetHash, digest, keyChanges,
          flags == 1);
    }

    long baseThrough() {
      return baseThrough;
    }

    byte[] baseHash() {
      return baseHash.clone();
    }

    long targetThrough() {
      return targetThrough;
    }

    byte[] targetHash() {
      return targetHash.clone();
    }

    byte[] targetSourceDigest() {
      return targetSourceDigest.clone();
    }

    long keyChanges() {
      return keyChanges;
    }

    boolean clearRebuild() {
      return clearRebuild;
    }
  }

  /** Test injection points of the multi-write flush protocol; production keeps NONE. */
  interface FlushFaultHook {
    FlushFaultHook NONE = new FlushFaultHook() { };

    /** After the durable intent write. */
    default void afterIntent() throws IOException {
    }

    /** After the durable write of the zero-based data batch. */
    default void afterDataBatch(int index) throws IOException {
    }

    /** Before the final coverage and progress publish write. */
    default void beforeFinalPublish() throws IOException {
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
