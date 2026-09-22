package org.tron.core.db2.stateroot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/**
 * Single durable store holding the last retained window of common-checkpoint payloads. It is the
 * only fsync point of a checkpoint: every version's per-store mutation shards and its index key
 * are written unsynced, then the {@code latest} key is written with sync=true. LevelDB/RocksDB
 * append to one sequential WAL, so syncing {@code latest} covers every preceding write of that
 * version. After power loss, authority stores whose unsynced tail was lost show an older
 * per-store checkpoint head and are repaired by replaying the missing versions from this store
 * (same-key idempotent puts).
 *
 * <p>Key layout (all keys ASCII, head zero-padded to 20 digits so lexicographic order matches
 * block-number order):
 * <ul>
 *   <li>{@code latest} → 8-byte head + 32-byte payload digest of the newest complete version;
 *   <li>{@code first} → 8-byte head of the oldest retained version;
 *   <li>{@code i:<head>} → presence index of one version, written after its shards and deleted
 *   last during pruning, so a present index always means complete shards;
 *   <li>{@code v:<head>:c:<dbName>} → encoded Chainbase Store mutations of that version;
 *   <li>{@code v:<head>:p:<storeId>} → encoded PathState participant flat+node mutations
 *   (storeId 0 is the super store);
 *   <li>{@code v:<head>:m} → the 64-byte PathState per-store checkpoint marker of that version;
 *   <li>{@code h:<head>:c:<dbName>} / {@code h:<head>:p:<storeId>} → 8-byte head journal of
 *   every store anchored by that version, pruned together with it. This is a progress JOURNAL
 *   only: it is written with the version batch (before the business stores are touched), so it
 *   can never prove what an unsynced business store actually persisted across a power loss —
 *   replay progress is instead derived from per-store content (Chainbase) or in-store markers
 *   (PathState internal stores). Business databases never carry internal keys: several of them
 *   self-iterate and parse every entry (TxCacheDB over recent-transaction, WitnessStore over
 *   witness).
 * </ul>
 */
public final class CommonCheckpointVersionStore implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger("DB");

  /** Default replay window: the newest 1000 blocks (100 ten-block checkpoints). */
  public static final long DEFAULT_RETAINED_BLOCKS = 1_000L;
  private static final int PRUNE_DELETE_CHUNK = 4096;
  private static final byte[] LATEST_KEY = bytes("latest");
  private static final byte[] FIRST_KEY = bytes("first");
  private static final byte[] INDEX_PREFIX = bytes("i:");
  private static final byte[] VERSION_PREFIX = bytes("v:");
  private static final byte[] INDEX_VALUE = new byte[]{1};
  private static final int HEAD_LENGTH = Long.BYTES;
  private static final int DIGEST_LENGTH = 32;

  private final PathStateNativeNodeStore store;
  private final long retainedBlocks;

  /** Opens the version store following the configured db engine with a 128 MiB write buffer. */
  public static CommonCheckpointVersionStore open(Path directory, Engine engine)
      throws IOException {
    return open(directory, engine, DEFAULT_RETAINED_BLOCKS);
  }

  public static CommonCheckpointVersionStore open(Path directory, Engine engine,
      long retainedBlocks) throws IOException {
    if (retainedBlocks <= 0) {
      throw new IllegalArgumentException("retained checkpoint blocks must be positive");
    }
    NativeDbConfig config = NativeDbConfig.large();
    config.setWriteBufferSize(128 * 1024 * 1024);
    config.setCacheSize(64L * 1024 * 1024);
    config.setMaxOpenFiles(256);
    return new CommonCheckpointVersionStore(PathStateNativeNodeStore.open(
        Objects.requireNonNull(directory, "directory"), Objects.requireNonNull(engine, "engine"),
        "common-checkpoint-versions", config), retainedBlocks);
  }

  /** Test seam: wraps an already opened native store. */
  static CommonCheckpointVersionStore wrap(PathStateNativeNodeStore store, long retainedBlocks) {
    return new CommonCheckpointVersionStore(store, retainedBlocks);
  }

  private CommonCheckpointVersionStore(PathStateNativeNodeStore store, long retainedBlocks) {
    this.store = Objects.requireNonNull(store, "store");
    this.retainedBlocks = retainedBlocks;
  }

  /**
   * Durably records one checkpoint version: mutation shards and the version index go in one
   * unsynced batch, {@code first} is seeded on first use, and {@code latest} is written last
   * with sync=true as the single fsync point of the whole checkpoint.
   */
  public synchronized void publish(CommonCheckpointPayload payload) throws IOException {
    CommonCheckpointTarget target = CommonCheckpointTarget.from(
        Objects.requireNonNull(payload, "payload"));
    long head = target.getLastBlock().getBlockNumber();
    long started = System.nanoTime();
    List<PathStateNativeNodeStore.BatchMutation> batch = new ArrayList<>();
    for (CommonCheckpointPayload.StoreMutations storeMutations : payload.getChainbaseStores()) {
      batch.add(PathStateNativeNodeStore.BatchMutation.put(
          chainbaseShardKey(head, storeMutations.getDbName()),
          encodeMutations(storeMutations.getMutations())));
      batch.add(PathStateNativeNodeStore.BatchMutation.put(
          progressKey(head, "c:" + storeMutations.getDbName()), encodeHead(head)));
    }
    for (CommonCheckpointPayload.PathStoreTarget pathStore : payload.getPathStores()) {
      batch.add(PathStateNativeNodeStore.BatchMutation.put(
          pathStoreShardKey(head, pathStore.getStoreId()),
          encodePathShard(pathStore.getFlatMutations(), pathStore.getNodeMutations())));
      batch.add(PathStateNativeNodeStore.BatchMutation.put(
          progressKey(head, "p:" + pathStore.getStoreId()), encodeHead(head)));
    }
    if (!payload.getSuperNodeMutations().isEmpty()) {
      batch.add(PathStateNativeNodeStore.BatchMutation.put(pathStoreShardKey(head, 0),
          encodePathShard(Collections.emptyList(), payload.getSuperNodeMutations())));
    }
    batch.add(PathStateNativeNodeStore.BatchMutation.put(progressKey(head, "p:0"),
        encodeHead(head)));
    batch.add(PathStateNativeNodeStore.BatchMutation.put(versionMarkerKey(head),
        pathStoreMarker(target)));
    batch.add(PathStateNativeNodeStore.BatchMutation.put(indexKey(head), INDEX_VALUE));
    if (firstHead() < 0) {
      batch.add(PathStateNativeNodeStore.BatchMutation.put(FIRST_KEY, encodeHead(head)));
    }
    store.writeBatchUnsynced(batch);
    ByteBuffer latest = ByteBuffer.allocate(HEAD_LENGTH + DIGEST_LENGTH);
    latest.putLong(head);
    latest.put(target.getPayloadDigest());
    store.writeBatch(Collections.singletonList(
        PathStateNativeNodeStore.BatchMutation.put(LATEST_KEY, latest.array())));
    logger.info("Common checkpoint version store: head={}, writeMs={}, shards={}", head,
        (System.nanoTime() - started) / 1_000_000L, batch.size());
  }

  /** Returns the newest complete checkpoint head, or -1 when no version was ever recorded. */
  public synchronized long latestHead() {
    byte[] latest = store.get(LATEST_KEY);
    if (latest == null) {
      return -1;
    }
    if (latest.length != HEAD_LENGTH + DIGEST_LENGTH) {
      throw new IllegalStateException("common checkpoint version store latest is corrupt");
    }
    return ByteBuffer.wrap(latest, 0, HEAD_LENGTH).getLong();
  }

  /** Returns the payload digest of the newest complete checkpoint version. */
  public synchronized byte[] latestDigest() {
    byte[] latest = store.get(LATEST_KEY);
    if (latest == null || latest.length != HEAD_LENGTH + DIGEST_LENGTH) {
      throw new IllegalStateException("common checkpoint version store latest is corrupt");
    }
    return Arrays.copyOfRange(latest, HEAD_LENGTH, HEAD_LENGTH + DIGEST_LENGTH);
  }

  /** Returns the oldest retained checkpoint head, or -1 when no version was ever recorded. */
  public synchronized long firstHead() {
    byte[] first = store.get(FIRST_KEY);
    if (first == null) {
      return -1;
    }
    if (first.length != HEAD_LENGTH) {
      throw new IllegalStateException("common checkpoint version store first is corrupt");
    }
    return ByteBuffer.wrap(first).getLong();
  }

  /** Lists every retained version head in ascending order, or fail-closed on a torn index. */
  public synchronized List<Long> versions() {
    List<Long> heads = new ArrayList<>();
    try {
      store.scanPrefix(INDEX_PREFIX, entry -> heads.add(decodeIndexKey(entry.getKey())));
    } catch (IOException failure) {
      throw new IllegalStateException("common checkpoint version store index is unreadable",
          failure);
    }
    Collections.sort(heads);
    return heads;
  }

  /**
   * Returns the versions in {@code (fromExclusive, toInclusive]} in ascending order, requiring
   * every version in the retained window to be present. A missing version means it was pruned
   * away or partially lost, so the caller must fail closed.
   */
  public synchronized List<Long> versionsBetween(long fromExclusive, long toInclusive)
      throws IOException {
    if (fromExclusive >= toInclusive) {
      return Collections.emptyList();
    }
    long first = firstHead();
    if (fromExclusive < first) {
      throw new IOException("common checkpoint version window " + first + ".." + toInclusive
          + " does not cover store head " + fromExclusive
          + "; the authority store must be rebuilt");
    }
    List<Long> replay = new ArrayList<>();
    for (long head : versions()) {
      if (head > fromExclusive && head <= toInclusive) {
        replay.add(head);
      }
    }
    if (replay.isEmpty() || replay.get(replay.size() - 1) != toInclusive) {
      throw new IOException("common checkpoint version store is missing versions in ("
          + fromExclusive + ", " + toInclusive + "]; the authority store must be rebuilt");
    }
    return replay;
  }

  /** Returns the encoded Chainbase Store mutations of one version, or null when none. */
  public synchronized byte[] chainbaseShard(long head, String dbName) {
    return store.get(chainbaseShardKey(head, Objects.requireNonNull(dbName, "dbName")));
  }

  /** Returns the encoded PathState store mutations of one version, or null when none. */
  public synchronized byte[] pathStoreShard(long head, int storeId) {
    return store.get(pathStoreShardKey(head, storeId));
  }

  /** Returns the 64-byte PathState per-store checkpoint marker of one version. */
  public synchronized byte[] versionMarker(long head) throws IOException {
    byte[] marker = store.get(versionMarkerKey(head));
    if (marker == null || marker.length != 2 * DIGEST_LENGTH) {
      throw new IOException("common checkpoint version store is missing the marker of " + head);
    }
    return marker;
  }

  /**
   * Advances the store's baseline to {@code head}: deletes every version at or below it and
   * writes {@code latest} (synced) and {@code first} for the published target. Used at startup
   * when the published authorities are AHEAD of this store — that can only mean the missing
   * checkpoints were written by old code with synced business-store writes (no power-loss gap),
   * so no replay is needed and the store simply starts over from the published target.
   */
  public synchronized void rebaseline(long head, byte[] digest) throws IOException {
    long latest = latestHead();
    if (latest >= head) {
      throw new IOException("common checkpoint version store rebaseline must move forward: "
          + "latest=" + latest + ", head=" + head);
    }
    Objects.requireNonNull(digest, "digest");
    if (digest.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException("digest must be exactly 32 bytes");
    }
    int removed = 0;
    for (long version : versions()) {
      List<PathStateNativeNodeStore.KeyValue> entries;
      try {
        entries = store.scanPrefix(versionPrefix(version));
        entries.addAll(store.scanPrefix(progressPrefix(version)));
      } catch (IOException failure) {
        throw new IOException("common checkpoint version rebaseline cannot scan " + version,
            failure);
      }
      List<PathStateNativeNodeStore.BatchMutation> deletes = new ArrayList<>(entries.size() + 1);
      for (PathStateNativeNodeStore.KeyValue entry : entries) {
        deletes.add(PathStateNativeNodeStore.BatchMutation.delete(entry.getKey()));
      }
      deletes.add(PathStateNativeNodeStore.BatchMutation.delete(indexKey(version)));
      store.writeBatchUnsynced(deletes);
      removed++;
    }
    store.writeBatchUnsynced(Collections.singletonList(
        PathStateNativeNodeStore.BatchMutation.put(FIRST_KEY, encodeHead(head))));
    ByteBuffer newLatest = ByteBuffer.allocate(HEAD_LENGTH + DIGEST_LENGTH);
    newLatest.putLong(head);
    newLatest.put(digest);
    store.writeBatch(Collections.singletonList(
        PathStateNativeNodeStore.BatchMutation.put(LATEST_KEY, newLatest.array())));
    logger.info("Common checkpoint version store: rebaseline to head={}, removedVersions={}",
        head, removed);
  }

  /** Returns the journaled applied head of one store for one version, or null when absent. */
  public synchronized Long progressHead(long version, String storeKey) {
    byte[] value = store.get(progressKey(version, Objects.requireNonNull(storeKey, "storeKey")));
    if (value == null) {
      return null;
    }
    if (value.length != HEAD_LENGTH) {
      throw new IllegalStateException("common checkpoint progress journal entry is corrupt");
    }
    return ByteBuffer.wrap(value).getLong();
  }

  /** Returns whether the retained window has grown beyond the configured block count. */
  public synchronized boolean needsPrune() {
    long latest = latestHead();
    long first = firstHead();
    return latest >= 0 && first >= 0 && latest - first > retainedBlocks;
  }

  /**
   * Prunes every version older than the boundary anchor: the newest head that is at least
   * {@code retainedBlocks} behind the latest one. The boundary version itself is RETAINED so a
   * store whose applied head sits exactly on it can still replay everything newer. {@code first}
   * is advanced BEFORE any delete so a crash mid-prune can only leave extra data, never a
   * replayable-looking gap. Returns the new oldest retained head, or -1 when nothing was pruned.
   */
  public synchronized long pruneIfNeeded() throws IOException {
    long latest = latestHead();
    if (latest < 0) {
      return -1;
    }
    long cutoff = latest - retainedBlocks;
    long boundary = -1;
    List<Long> expired = new ArrayList<>();
    for (long head : versions()) {
      if (head <= cutoff) {
        expired.add(head);
        boundary = head;
      }
    }
    if (boundary < 0 || boundary <= firstHead()) {
      return -1;
    }
    expired.remove(expired.size() - 1); // the boundary version stays as the replay anchor
    store.writeBatchUnsynced(Collections.singletonList(
        PathStateNativeNodeStore.BatchMutation.put(FIRST_KEY, encodeHead(boundary))));
    for (long head : expired) {
      List<PathStateNativeNodeStore.KeyValue> entries;
      try {
        entries = store.scanPrefix(versionPrefix(head));
        entries.addAll(store.scanPrefix(progressPrefix(head)));
      } catch (IOException failure) {
        throw new IOException("common checkpoint version prune cannot scan " + head, failure);
      }
      List<PathStateNativeNodeStore.BatchMutation> deletes = new ArrayList<>(PRUNE_DELETE_CHUNK);
      for (PathStateNativeNodeStore.KeyValue entry : entries) {
        deletes.add(PathStateNativeNodeStore.BatchMutation.delete(entry.getKey()));
        if (deletes.size() == PRUNE_DELETE_CHUNK) {
          store.writeBatchUnsynced(new ArrayList<>(deletes));
          deletes.clear();
        }
      }
      deletes.add(PathStateNativeNodeStore.BatchMutation.delete(indexKey(head)));
      store.writeBatchUnsynced(deletes);
    }
    logger.info("Common checkpoint version store: prunedThrough={}, latest={}, expired={}",
        boundary, latest, expired.size());
    return boundary;
  }

  @Override
  public synchronized void close() throws IOException {
    store.close();
  }

  /** Encodes one mutation list as count + (keyLength, key, valueLength, value)*; -1 = delete. */
  public static byte[] encodeMutations(List<CommonCheckpointPayload.Mutation> mutations) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      List<CommonCheckpointPayload.Mutation> supplied = Objects.requireNonNull(mutations,
          "mutations");
      output.writeInt(supplied.size());
      for (CommonCheckpointPayload.Mutation mutation : supplied) {
        output.writeInt(mutation.getKey().length);
        output.write(mutation.getKey());
        byte[] value = mutation.getValue();
        output.writeInt(value == null ? -1 : value.length);
        if (value != null) {
          output.write(value);
        }
      }
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory version shard encoding failed", impossible);
    }
  }

  /** Decodes one mutation list written by {@link #encodeMutations}. */
  public static List<CommonCheckpointPayload.Mutation> decodeMutations(byte[] encoded)
      throws IOException {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        Objects.requireNonNull(encoded, "encoded")))) {
      int count = input.readInt();
      if (count < 0) {
        throw new IOException("version shard mutation count is invalid");
      }
      List<CommonCheckpointPayload.Mutation> mutations = new ArrayList<>(count);
      for (int index = 0; index < count; index++) {
        byte[] key = new byte[input.readInt()];
        input.readFully(key);
        int valueLength = input.readInt();
        byte[] value = null;
        if (valueLength >= 0) {
          value = new byte[valueLength];
          input.readFully(value);
        }
        mutations.add(new CommonCheckpointPayload.Mutation(key, value));
      }
      return mutations;
    } catch (EOFException truncated) {
      throw new IOException("version shard is truncated", truncated);
    } catch (NegativeArraySizeException | OutOfMemoryError corrupt) {
      throw new IOException("version shard is corrupt", corrupt);
    }
  }

  private static byte[] encodePathShard(List<CommonCheckpointPayload.Mutation> flat,
      List<CommonCheckpointPayload.Mutation> nodes) {
    byte[] flatEncoded = encodeMutations(flat);
    byte[] nodeEncoded = encodeMutations(nodes);
    return ByteBuffer.allocate(Integer.BYTES + flatEncoded.length + nodeEncoded.length)
        .putInt(flatEncoded.length).put(flatEncoded).put(nodeEncoded).array();
  }

  /** Decoded PathState shard: flat mutations plus node mutations. */
  public static final class PathShard {

    private final List<CommonCheckpointPayload.Mutation> flatMutations;
    private final List<CommonCheckpointPayload.Mutation> nodeMutations;

    private PathShard(List<CommonCheckpointPayload.Mutation> flatMutations,
        List<CommonCheckpointPayload.Mutation> nodeMutations) {
      this.flatMutations = flatMutations;
      this.nodeMutations = nodeMutations;
    }

    public List<CommonCheckpointPayload.Mutation> getFlatMutations() {
      return flatMutations;
    }

    public List<CommonCheckpointPayload.Mutation> getNodeMutations() {
      return nodeMutations;
    }
  }

  /** Decodes one PathState shard written by {@link #encodePathShard}. */
  public static PathShard decodePathShard(byte[] encoded) throws IOException {
    byte[] supplied = Objects.requireNonNull(encoded, "encoded");
    if (supplied.length < Integer.BYTES) {
      throw new IOException("version path shard is truncated");
    }
    int flatLength = ByteBuffer.wrap(supplied, 0, Integer.BYTES).getInt();
    if (flatLength < Integer.BYTES || supplied.length < Integer.BYTES + flatLength) {
      throw new IOException("version path shard length is invalid");
    }
    return new PathShard(
        decodeMutations(Arrays.copyOfRange(supplied, Integer.BYTES, Integer.BYTES + flatLength)),
        decodeMutations(Arrays.copyOfRange(supplied, Integer.BYTES + flatLength,
            supplied.length)));
  }

  private static byte[] pathStoreMarker(CommonCheckpointTarget target) {
    byte[] marker = new byte[2 * DIGEST_LENGTH];
    System.arraycopy(target.getPayloadDigest(), 0, marker, 0, DIGEST_LENGTH);
    System.arraycopy(target.getStateRoot(), 0, marker, DIGEST_LENGTH, DIGEST_LENGTH);
    return marker;
  }

  private static byte[] chainbaseShardKey(long head, String dbName) {
    return bytes("v:" + pad(head) + ":c:" + dbName);
  }

  private static byte[] pathStoreShardKey(long head, int storeId) {
    return bytes("v:" + pad(head) + ":p:" + storeId);
  }

  private static byte[] versionMarkerKey(long head) {
    return bytes("v:" + pad(head) + ":m");
  }

  private static byte[] progressKey(long head, String storeKey) {
    return bytes("h:" + pad(head) + ":" + storeKey);
  }

  private static byte[] progressPrefix(long head) {
    return bytes("h:" + pad(head) + ":");
  }

  private static byte[] versionPrefix(long head) {
    return bytes("v:" + pad(head) + ":");
  }

  private static byte[] indexKey(long head) {
    return bytes("i:" + pad(head));
  }

  private static long decodeIndexKey(byte[] key) {
    String text = new String(key, java.nio.charset.StandardCharsets.US_ASCII);
    return Long.parseLong(text.substring(2));
  }

  private static byte[] encodeHead(long head) {
    return ByteBuffer.allocate(HEAD_LENGTH).putLong(head).array();
  }

  private static String pad(long head) {
    return String.format(Locale.ROOT, "%020d", head);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  }
}
