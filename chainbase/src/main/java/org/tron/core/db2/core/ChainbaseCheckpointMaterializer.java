package org.tron.core.db2.core;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.stateroot.CommonCheckpointVersionStore;

/** Chainbase participant for common-checkpoint idempotent materialization and publication. */
public final class ChainbaseCheckpointMaterializer implements CommonCheckpointMaterializer {

  private static final Logger logger = LoggerFactory.getLogger("DB");

  static final String CURRENT_FILE = "CHAINBASE_CURRENT";
  static final String MATERIALIZED_DIRECTORY = "chainbase-checkpoint-materialized";
  private static final int MAGIC = 0x43424354; // CBCT
  private static final short VERSION = 1;
  private static final int DIGEST_LENGTH = 32;
  private static final int MAX_PARALLEL_STORE_WRITES = 8;
  private static final int RECORD_LENGTH = Integer.BYTES + 2 * Short.BYTES
      + 4 * DIGEST_LENGTH + 2 * Long.BYTES + DIGEST_LENGTH;

  private final Path directory;
  private final byte[] formatIdentity;
  private final Map<String, Chainbase> databases;
  private final FaultHook faultHook;
  private final CommonCheckpointBaseline baseline;
  private final CommonCheckpointMaterializedStore materializedStore;
  private ExecutorService storeWriteExecutor;

  public ChainbaseCheckpointMaterializer(Path directory, byte[] formatIdentity,
      List<Chainbase> databases) {
    this(directory, formatIdentity, databases, null, null, (stage, dbName) -> { });
  }

  public ChainbaseCheckpointMaterializer(Path directory, byte[] formatIdentity,
      List<Chainbase> databases, CommonCheckpointBaseline baseline) {
    this(directory, formatIdentity, databases, baseline, null, (stage, dbName) -> { });
  }

  public ChainbaseCheckpointMaterializer(Path directory, byte[] formatIdentity,
      List<Chainbase> databases, CommonCheckpointBaseline baseline,
      CommonCheckpointMaterializedStore materializedStore) {
    this(directory, formatIdentity, databases, baseline, materializedStore,
        (stage, dbName) -> { });
  }

  ChainbaseCheckpointMaterializer(Path directory, byte[] formatIdentity,
      List<Chainbase> databases, FaultHook faultHook) {
    this(directory, formatIdentity, databases, null, null, faultHook);
  }

  private ChainbaseCheckpointMaterializer(Path directory, byte[] formatIdentity,
      List<Chainbase> databases, CommonCheckpointBaseline baseline,
      CommonCheckpointMaterializedStore materializedStore, FaultHook faultHook) {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.formatIdentity = digest(formatIdentity, "formatIdentity");
    this.databases = index(databases);
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    this.baseline = baseline;
    this.materializedStore = materializedStore;
  }

  @Override
  public Authority authority() {
    return Authority.CHAINBASE;
  }

  /** Loads the compact next-format head published beside the Chainbase databases. */
  public static PublishedHead loadPublishedHead(Path directory, byte[] expectedFormatIdentity)
      throws IOException {
    Marker marker = load(Objects.requireNonNull(directory, "directory").resolve(CURRENT_FILE));
    if (!Arrays.equals(marker.formatIdentity,
        digest(expectedFormatIdentity, "expectedFormatIdentity"))) {
      throw new IOException("Chainbase published target format identity differs");
    }
    return new PublishedHead(marker.lastEpoch, marker.lastBlockNumber, marker.lastBlockHash,
        marker.stateRoot, marker.payloadDigest);
  }

  @Override
  public synchronized Status inspect(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    byte[] expected = encode(admitted);
    Path currentPath = directory.resolve(CURRENT_FILE);
    if (Files.exists(currentPath, LinkOption.NOFOLLOW_LINKS)) {
      Marker current = load(currentPath);
      if (Arrays.equals(current.encoded, expected)) {
        requireMaterialized(admitted, expected);
        return Status.PUBLISHED;
      }
      requireParent(current, admitted);
    } else if (baseline != null) {
      baseline.requireParent(admitted, "Chainbase");
    }
    if (!isMaterialized(admitted, expected)) {
      return Status.NEEDS_MATERIALIZATION;
    }
    return Status.MATERIALIZED;
  }

  /**
   * Materializes every payload Store in parallel on a bounded daemon executor. Store lookups and
   * payload validation happen on the calling thread; each Store is written by exactly one task
   * (chunked unsynced data, see {@link SnapshotRoot#applyCheckpointMutations}). Durability is
   * anchored by the common checkpoint version store, so Stores never fsync, and no internal keys
   * are written into business databases (some self-iterate at startup, e.g. TxCacheDB over
   * recent-transaction). The central materialized marker is recorded only after every Store
   * write has completed. Fault hooks may fire on worker threads.
   */
  @Override
  public synchronized void materialize(CommonCheckpointPayload payload,
      CommonCheckpointTarget target) throws IOException {
    CommonCheckpointPayload admittedPayload = Objects.requireNonNull(payload, "payload");
    CommonCheckpointTarget admittedTarget = requireTarget(target);
    if (!admittedTarget.equals(CommonCheckpointTarget.from(admittedPayload))) {
      throw new IOException("Chainbase checkpoint payload and target differ");
    }
    Status status = inspect(admittedTarget);
    if (status != Status.NEEDS_MATERIALIZATION) {
      return;
    }
    List<Runnable> writes = new ArrayList<>();
    for (CommonCheckpointPayload.StoreMutations store
        : admittedPayload.getChainbaseStores()) {
      SnapshotRoot rootSnapshot = requireRoot(store.getDbName());
      writes.add(() -> {
        boolean observe = Boolean.getBoolean("tron.chainbase.executionAttribution");
        long started = observe ? System.nanoTime() : 0;
        rootSnapshot.applyCheckpointMutations(batch(store));
        if (observe) {
          ExecutionAttribution.checkpoint(admittedTarget, store.getDbName(),
              store.getMutations().size(), System.nanoTime() - started);
        }
        afterHook(Stage.AFTER_STORE_BATCH, store.getDbName());
      });
    }
    awaitStoreWrites(writes);
    recordMaterialized(admittedTarget, encode(admittedTarget));
    faultHook.after(Stage.AFTER_MATERIALIZED_TARGET, null);
  }

  /**
   * Repairs Stores whose unsynced tail was lost in a power loss. Business databases carry no
   * internal progress keys, so the durable boundary of each Store is verified by CONTENT:
   * checkpoint writes are the only root-level writes of a Store, so its WAL is a sequence of
   * per-version runs and a power loss can only truncate a suffix. Walking versions newest-first,
   * a version is fully applied iff every shard key still holds the expected post-version value
   * (keys rewritten by a later version are excluded — their current values reflect the later
   * write). The newest content-matching version is the durable boundary; every newer retained
   * version is re-applied in order (same-key idempotent puts). When no version can be confirmed,
   * every retained version is re-applied; this stays sound because the retained window (1000
   * blocks) is far longer than any OS dirty-page horizon a power loss can truncate. A version
   * store that cannot provide a complete window fails closed.
   */
  @Override
  public synchronized void replayFromVersionStore(CommonCheckpointVersionStore versions,
      long latestHead) throws IOException {
    List<Runnable> replays = new ArrayList<>();
    for (Map.Entry<String, Chainbase> entry : databases.entrySet()) {
      SnapshotRoot rootSnapshot = requireRoot(entry.getKey());
      replays.add(() -> replayStore(entry.getKey(), rootSnapshot, versions, latestHead));
    }
    awaitStoreWrites(replays);
  }

  private void replayStore(String dbName, SnapshotRoot rootSnapshot,
      CommonCheckpointVersionStore versions, long latestHead) {
    try {
      List<Long> retained = versions.versions();
      if (!retained.isEmpty() && retained.get(retained.size() - 1) != latestHead) {
        throw new IOException("common checkpoint version store window does not end at "
            + latestHead);
      }
      long applied = verifyAppliedHead(dbName, rootSnapshot, versions, latestHead);
      if (applied >= latestHead) {
        return;
      }
      for (long version : retained) {
        if (version <= applied) {
          continue;
        }
        byte[] shard = versions.chainbaseShard(version, dbName);
        if (shard != null) {
          Map<WrappedByteArray, WrappedByteArray> batch = new LinkedHashMap<>();
          for (CommonCheckpointPayload.Mutation mutation
              : CommonCheckpointVersionStore.decodeMutations(shard)) {
            batch.put(WrappedByteArray.of(mutation.getKey()),
                WrappedByteArray.of(mutation.getValue()));
          }
          rootSnapshot.applyCheckpointMutations(batch);
        }
      }
      logger.info("Chainbase checkpoint replay: store={}, from={}, to={}", dbName,
          applied, latestHead);
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  /**
   * Returns the newest version whose writes to this Store verifiably survived, latestHead when
   * the Store has no checkpoint writes in the window (nothing to replay), or -1 when a loss is
   * detected but no version matches (the caller then re-applies the whole retained window).
   */
  private long verifyAppliedHead(String dbName, SnapshotRoot rootSnapshot,
      CommonCheckpointVersionStore versions, long latestHead) throws IOException {
    List<Long> all = versions.versions();
    Set<WrappedByteArray> shadowed = new HashSet<>();
    boolean hadShards = false;
    for (int index = all.size() - 1; index >= 0; index--) {
      long version = all.get(index);
      if (version > latestHead) {
        continue;
      }
      byte[] shard = versions.chainbaseShard(version, dbName);
      if (shard == null) {
        continue;
      }
      hadShards = true;
      List<CommonCheckpointPayload.Mutation> mutations =
          CommonCheckpointVersionStore.decodeMutations(shard);
      boolean matches = true;
      boolean checked = false;
      for (CommonCheckpointPayload.Mutation mutation : mutations) {
        WrappedByteArray key = WrappedByteArray.of(mutation.getKey());
        if (shadowed.contains(key)) {
          continue;
        }
        checked = true;
        if (!Arrays.equals(mutation.getValue(), rootSnapshot.get(mutation.getKey()))) {
          matches = false;
          break;
        }
      }
      for (CommonCheckpointPayload.Mutation mutation : mutations) {
        shadowed.add(WrappedByteArray.of(mutation.getKey()));
      }
      if (matches && checked) {
        return version;
      }
    }
    return hadShards ? -1 : latestHead;
  }

  private SnapshotRoot requireRoot(String dbName) throws IOException {
    Chainbase database = databases.get(dbName);
    if (database == null) {
      throw new IOException("Chainbase checkpoint Store is not registered: " + dbName);
    }
    Snapshot root = database.getHead().getRoot();
    if (!(root instanceof SnapshotRoot)) {
      throw new IOException("Chainbase checkpoint Store has no SnapshotRoot: " + dbName);
    }
    return (SnapshotRoot) root;
  }

  /** Waits for every parallel Store write and propagates the first failure, if any. */
  private void awaitStoreWrites(List<Runnable> writes) throws IOException {
    List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
    for (Runnable write : writes) {
      futures.add(storeWriteExecutor().submit(write));
    }
    IOException failure = null;
    boolean interrupted = false;
    for (java.util.concurrent.Future<?> future : futures) {
      boolean complete = false;
      while (!complete) {
        try {
          future.get();
          complete = true;
        } catch (InterruptedException interruptedFailure) {
          interrupted = true;
        } catch (ExecutionException writeFailure) {
          complete = true;
          Throwable cause = writeFailure.getCause();
          if (cause instanceof java.io.UncheckedIOException) {
            cause = cause.getCause();
          }
          IOException storeFailure = cause instanceof IOException
              ? (IOException) cause
              : new IOException("Chainbase checkpoint Store batch failed", cause);
          if (failure == null) {
            failure = storeFailure;
          } else {
            failure.addSuppressed(storeFailure);
          }
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      IOException waitFailure = new IOException(
          "Chainbase checkpoint Store batch wait was interrupted");
      if (failure == null) {
        failure = waitFailure;
      } else {
        failure.addSuppressed(waitFailure);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private ExecutorService storeWriteExecutor() {
    if (storeWriteExecutor == null) {
      storeWriteExecutor = Executors.newFixedThreadPool(
          Math.min(Math.max(databases.size(), 1), MAX_PARALLEL_STORE_WRITES), task -> {
            Thread thread = new Thread(task, "chainbase-checkpoint-write");
            thread.setDaemon(true);
            return thread;
          });
    }
    return storeWriteExecutor;
  }

  private void afterHook(Stage stage, String dbName) {
    try {
      faultHook.after(stage, dbName);
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  /** Shuts down the parallel Store writer; the Stores themselves are owned by the caller. */
  @Override
  public synchronized void close() {
    if (storeWriteExecutor != null) {
      storeWriteExecutor.shutdownNow();
      storeWriteExecutor = null;
    }
  }

  @Override
  public synchronized void publish(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    Status status = inspect(admitted);
    if (status == Status.PUBLISHED) {
      return;
    }
    if (status != Status.MATERIALIZED) {
      throw new IOException("Chainbase checkpoint target is not fully materialized");
    }
    replace(directory.resolve(CURRENT_FILE), encode(admitted));
    faultHook.after(Stage.AFTER_CURRENT, null);
  }

  private CommonCheckpointTarget requireTarget(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    if (!Arrays.equals(formatIdentity, admitted.getFormatIdentity())) {
      throw new IOException("Chainbase checkpoint format identity differs");
    }
    return admitted;
  }

  private void requireParent(Marker current, CommonCheckpointTarget target) throws IOException {
    BlockSnapshotMeta first = target.getFirstBlock();
    if (!Arrays.equals(current.formatIdentity, target.getFormatIdentity())
        || current.lastEpoch + 1 != first.getEpoch()
        || current.lastBlockNumber + 1 != first.getBlockNumber()
        || !Arrays.equals(current.lastBlockHash, first.getParentHash())
        || !Arrays.equals(current.stateRoot, target.getParentStateRoot())) {
      throw new IOException("Chainbase CURRENT is not the checkpoint parent target");
    }
  }

  private Path materializedPath(CommonCheckpointTarget target) {
    return directory.resolve(MATERIALIZED_DIRECTORY).resolve(hex(target.getPayloadDigest()));
  }

  private boolean isMaterialized(CommonCheckpointTarget target, byte[] expected)
      throws IOException {
    if (materializedStore != null && materializedStore.exists(Authority.CHAINBASE)) {
      return materializedStore.matches(Authority.CHAINBASE, expected);
    }
    Path legacy = materializedPath(target);
    if (!Files.exists(legacy, LinkOption.NOFOLLOW_LINKS)) {
      return false;
    }
    requireExact(legacy, expected);
    return true;
  }

  private void requireMaterialized(CommonCheckpointTarget target, byte[] expected)
      throws IOException {
    if (!isMaterialized(target, expected)) {
      throw new IOException("Chainbase materialized checkpoint target is missing");
    }
  }

  private void recordMaterialized(CommonCheckpointTarget target, byte[] encoded)
      throws IOException {
    if (materializedStore == null) {
      publishImmutable(materializedPath(target), encoded);
    } else {
      materializedStore.replace(Authority.CHAINBASE, encoded);
    }
  }

  private static Map<WrappedByteArray, WrappedByteArray> batch(
      CommonCheckpointPayload.StoreMutations store) {
    Map<WrappedByteArray, WrappedByteArray> batch = new LinkedHashMap<>();
    for (CommonCheckpointPayload.Mutation mutation : store.getMutations()) {
      batch.put(WrappedByteArray.of(mutation.getKey()),
          WrappedByteArray.of(mutation.getValue()));
    }
    return batch;
  }

  private static Map<String, Chainbase> index(List<Chainbase> supplied) {
    Map<String, Chainbase> indexed = new LinkedHashMap<>();
    for (Chainbase database : Objects.requireNonNull(supplied, "databases")) {
      Chainbase admitted = Objects.requireNonNull(database, "database");
      if (indexed.putIfAbsent(admitted.getDbName(), admitted) != null) {
        throw new IllegalArgumentException("duplicate Chainbase checkpoint Store: "
            + admitted.getDbName());
      }
    }
    return indexed;
  }

  private static byte[] encode(CommonCheckpointTarget target) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(RECORD_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.write(target.getFormatIdentity());
      output.write(target.getPayloadDigest());
      output.writeLong(target.getLastBlock().getEpoch());
      output.writeLong(target.getLastBlock().getBlockNumber());
      output.write(target.getLastBlock().getBlockHash());
      output.write(target.getStateRoot());
      output.flush();
      byte[] body = bytes.toByteArray();
      output.write(Hashing.sha256().hashBytes(body).asBytes());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory Chainbase target encoding failed", impossible);
    }
  }

  private static Marker load(Path path) throws IOException {
    byte[] encoded = readBounded(path, RECORD_LENGTH);
    if (encoded.length != RECORD_LENGTH) {
      throw new IOException("Chainbase checkpoint target length is invalid");
    }
    int bodyLength = encoded.length - DIGEST_LENGTH;
    byte[] body = Arrays.copyOf(encoded, bodyLength);
    byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
    if (!Arrays.equals(checksum, Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IOException("Chainbase checkpoint target checksum differs");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new IOException("Chainbase checkpoint target format is unsupported");
      }
      return new Marker(encoded, readDigest(input), readDigest(input), input.readLong(),
          input.readLong(), readDigest(input), readDigest(input));
    } catch (EOFException truncated) {
      throw new IOException("Chainbase checkpoint target is truncated", truncated);
    }
  }

  private static void requireExact(Path path, byte[] expected) throws IOException {
    if (!Arrays.equals(load(path).encoded, expected)) {
      throw new IOException("Chainbase checkpoint target identity differs");
    }
  }

  private static void publishImmutable(Path path, byte[] bytes) throws IOException {
    createDirectory(path.getParent());
    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      if (!Arrays.equals(readBounded(path, bytes.length), bytes)) {
        throw new IOException("Chainbase immutable checkpoint target differs");
      }
      return;
    }
    Path temporary = path.resolveSibling(path.getFileName() + ".tmp-" + UUID.randomUUID());
    try {
      writeForced(temporary, bytes);
      try {
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("Chainbase filesystem lacks atomic target publication",
            unsupported);
      } catch (java.nio.file.FileAlreadyExistsException raced) {
        if (!Arrays.equals(readBounded(path, bytes.length), bytes)) {
          throw new IOException("Chainbase immutable target publication raced", raced);
        }
      }
      syncDirectory(path.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void replace(Path path, byte[] bytes) throws IOException {
    createDirectory(path.getParent());
    Path temporary = path.resolveSibling(path.getFileName() + ".tmp-" + UUID.randomUUID());
    try {
      writeForced(temporary, bytes);
      try {
        Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new IOException("Chainbase filesystem lacks atomic CURRENT replacement",
            unsupported);
      }
      syncDirectory(path.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void writeForced(Path path, byte[] bytes) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
  }

  private static byte[] readBounded(Path path, long maximum) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Chainbase checkpoint path is not a regular file");
    }
    long size = Files.size(path);
    if (size <= 0 || size > maximum) {
      throw new IOException("Chainbase checkpoint target length is invalid");
    }
    return Files.readAllBytes(path);
  }

  private static void createDirectory(Path path) throws IOException {
    Files.createDirectories(path);
    if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Chainbase checkpoint path is not a directory");
    }
  }

  private static void syncDirectory(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static byte[] readDigest(DataInputStream input) throws IOException {
    byte[] value = new byte[DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static byte[] digest(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return copy;
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte current : value) {
      encoded.append(Character.forDigit(current >>> 4 & 0xf, 16));
      encoded.append(Character.forDigit(current & 0xf, 16));
    }
    return encoded.toString();
  }

  enum Stage {
    AFTER_STORE_BATCH,
    AFTER_MATERIALIZED_TARGET,
    AFTER_CURRENT
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage, String dbName) throws IOException;
  }

  private static final class Marker {

    private final byte[] encoded;
    private final byte[] formatIdentity;
    private final byte[] payloadDigest;
    private final long lastEpoch;
    private final long lastBlockNumber;
    private final byte[] lastBlockHash;
    private final byte[] stateRoot;

    private Marker(byte[] encoded, byte[] formatIdentity, byte[] payloadDigest, long lastEpoch,
        long lastBlockNumber, byte[] lastBlockHash, byte[] stateRoot) {
      this.encoded = encoded;
      this.formatIdentity = formatIdentity;
      this.payloadDigest = payloadDigest;
      this.lastEpoch = lastEpoch;
      this.lastBlockNumber = lastBlockNumber;
      this.lastBlockHash = lastBlockHash;
      this.stateRoot = stateRoot;
    }
  }

  /** Minimal restart identity retained by CHAINBASE_CURRENT. */
  public static final class PublishedHead {

    private final long epoch;
    private final long blockNumber;
    private final byte[] blockHash;
    private final byte[] stateRoot;
    private final byte[] payloadDigest;

    private PublishedHead(long epoch, long blockNumber, byte[] blockHash, byte[] stateRoot,
        byte[] payloadDigest) {
      this.epoch = epoch;
      this.blockNumber = blockNumber;
      this.blockHash = Arrays.copyOf(blockHash, blockHash.length);
      this.stateRoot = Arrays.copyOf(stateRoot, stateRoot.length);
      this.payloadDigest = Arrays.copyOf(payloadDigest, payloadDigest.length);
    }

    public long getEpoch() {
      return epoch;
    }

    public long getBlockNumber() {
      return blockNumber;
    }

    public byte[] getBlockHash() {
      return Arrays.copyOf(blockHash, blockHash.length);
    }

    public byte[] getStateRoot() {
      return Arrays.copyOf(stateRoot, stateRoot.length);
    }

    public byte[] getPayloadDigest() {
      return Arrays.copyOf(payloadDigest, payloadDigest.length);
    }
  }
}
