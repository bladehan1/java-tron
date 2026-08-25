package org.tron.core.db2.archive;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable history identity stored in the same Chainbase checkpoint batch as state changes. */
public final class ArchiveWalBinding {

  static final String CHECKPOINT_DATABASE = "__state_archive_wal__";
  private static final byte[] CHECKPOINT_KEY = checkpointKey();

  private final BlockSnapshotMeta first;
  private final BlockSnapshotMeta last;
  private final long predecessorEpoch;
  private final byte[] predecessorHash;
  private final byte[] batchDigest;
  private final byte[] storeScopeDigest;
  private final byte[] historyRefsDigest;
  private final byte[] blockIndexRefsDigest;

  ArchiveWalBinding(BlockSnapshotMeta first, BlockSnapshotMeta last, long predecessorEpoch,
      byte[] predecessorHash, byte[] batchDigest, byte[] storeScopeDigest,
      byte[] historyRefsDigest, byte[] blockIndexRefsDigest) {
    this.first = Objects.requireNonNull(first, "first");
    this.last = Objects.requireNonNull(last, "last");
    this.predecessorEpoch = predecessorEpoch;
    this.predecessorHash = hash(predecessorHash, "predecessorHash");
    this.batchDigest = hash(batchDigest, "batchDigest");
    this.storeScopeDigest = hash(storeScopeDigest, "storeScopeDigest");
    this.historyRefsDigest = hash(historyRefsDigest, "historyRefsDigest");
    this.blockIndexRefsDigest = hash(blockIndexRefsDigest, "blockIndexRefsDigest");
    if (predecessorEpoch != first.getEpoch() - 1
        || !Arrays.equals(this.predecessorHash, first.getParentHash())
        || first.getEpoch() > last.getEpoch()
        || first.getBlockNumber() > last.getBlockNumber()) {
      throw new IllegalArgumentException("Archive WAL binding range is invalid");
    }
  }

  public static ArchiveWalBinding fromMarkers(List<HistoryCommitMarker> source) {
    List<HistoryCommitMarker> markers = new ArrayList<>(
        Objects.requireNonNull(source, "markers"));
    if (markers.isEmpty()) {
      throw new IllegalArgumentException("Archive WAL binding markers must not be empty");
    }
    HistoryCommitMarkerCodec codec = new HistoryCommitMarkerCodec();
    Hasher batch = Hashing.sha256().newHasher();
    Hasher history = Hashing.sha256().newHasher();
    Hasher index = Hashing.sha256().newHasher();
    HistoryCommitMarker previous = null;
    List<String> databases = null;
    for (HistoryCommitMarker marker : markers) {
      HistoryCommitMarker current = Objects.requireNonNull(marker, "marker");
      if (previous != null && (current.getMeta().getEpoch()
          != previous.getMeta().getEpoch() + 1
          || current.getMeta().getBlockNumber()
          != previous.getMeta().getBlockNumber() + 1
          || current.getPreviousEpoch() != previous.getMeta().getEpoch()
          || !Arrays.equals(current.getMeta().getParentHash(),
          previous.getMeta().getBlockHash()))) {
        throw new IllegalArgumentException("Archive WAL binding markers are not contiguous");
      }
      if (databases == null) {
        databases = current.getDatabases();
      } else if (!databases.equals(current.getDatabases())) {
        throw new IllegalArgumentException("Archive WAL binding Store scope changed in batch");
      }
      batch.putBytes(codec.encode(current));
      putHistoryReference(history, current);
      putBlockIndexReference(index, current);
      previous = current;
    }
    return new ArchiveWalBinding(markers.get(0).getMeta(), previous.getMeta(),
        markers.get(0).getPreviousEpoch(), markers.get(0).getMeta().getParentHash(),
        batch.hash().asBytes(), scopeDigest(databases), history.hash().asBytes(),
        index.hash().asBytes());
  }

  public BlockSnapshotMeta getFirst() {
    return first;
  }

  public BlockSnapshotMeta getLast() {
    return last;
  }

  public long getPredecessorEpoch() {
    return predecessorEpoch;
  }

  public byte[] getPredecessorHash() {
    return Arrays.copyOf(predecessorHash, predecessorHash.length);
  }

  public byte[] getBatchDigest() {
    return Arrays.copyOf(batchDigest, batchDigest.length);
  }

  public byte[] getStoreScopeDigest() {
    return Arrays.copyOf(storeScopeDigest, storeScopeDigest.length);
  }

  public byte[] getHistoryRefsDigest() {
    return Arrays.copyOf(historyRefsDigest, historyRefsDigest.length);
  }

  public byte[] getBlockIndexRefsDigest() {
    return Arrays.copyOf(blockIndexRefsDigest, blockIndexRefsDigest.length);
  }

  public static byte[] getCheckpointKey() {
    return Arrays.copyOf(CHECKPOINT_KEY, CHECKPOINT_KEY.length);
  }

  public static boolean isCheckpointKey(byte[] key) {
    return Arrays.equals(CHECKPOINT_KEY, key);
  }

  public static ArchiveWalBinding fromCheckpointBatch(Map<byte[], byte[]> batch) {
    for (Map.Entry<byte[], byte[]> entry : batch.entrySet()) {
      if (isCheckpointKey(entry.getKey())) {
        return new ArchiveWalBindingCodec().decode(entry.getValue());
      }
    }
    return null;
  }

  private static byte[] checkpointKey() {
    byte[] database = CHECKPOINT_DATABASE.getBytes(StandardCharsets.UTF_8);
    byte[] field = "binding".getBytes(StandardCharsets.UTF_8);
    byte[] key = new byte[Integer.BYTES + database.length + field.length];
    System.arraycopy(Ints.toByteArray(database.length), 0, key, 0, Integer.BYTES);
    System.arraycopy(database, 0, key, Integer.BYTES, database.length);
    System.arraycopy(field, 0, key, Integer.BYTES + database.length, field.length);
    return key;
  }

  private static byte[] scopeDigest(List<String> databases) {
    List<String> sorted = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    Collections.sort(sorted);
    Hasher digest = Hashing.sha256().newHasher();
    digest.putInt(sorted.size());
    for (String database : sorted) {
      byte[] encoded = database.getBytes(StandardCharsets.UTF_8);
      digest.putInt(encoded.length).putBytes(encoded);
    }
    return digest.hash().asBytes();
  }

  private static void putHistoryReference(Hasher digest, HistoryCommitMarker marker) {
    HistoryLocation location = marker.getHistoryLocation();
    digest.putLong(marker.getMeta().getEpoch()).putInt(location.getSegmentId())
        .putLong(location.getOffset()).putInt(location.getRecordLength())
        .putInt(location.getBodyChecksum()).putBytes(location.getBodyDigest());
  }

  private static void putBlockIndexReference(Hasher digest, HistoryCommitMarker marker) {
    HistoryIndexLocation location = marker.getIndexLocation();
    digest.putLong(marker.getMeta().getEpoch()).putLong(marker.getMeta().getBlockNumber())
        .putBytes(marker.getMeta().getBlockHash()).putLong(location.getOffset())
        .putInt(location.getRecordLength()).putBytes(location.getDigest());
  }

  private static byte[] hash(byte[] value, String name) {
    if (value == null || value.length != 32) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
    return Arrays.copyOf(value, value.length);
  }
}
