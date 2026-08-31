package org.tron.core.db2.stateroot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateNativeNodeStore.BatchMutation;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Disk-sorted, bounded-memory builder for one participant Store trie. */
final class PathStateStoreTrieBuilder implements Closeable {

  static final int DEFAULT_WRITE_BATCH_ROWS = 4096;
  private static final long BUILD_PROGRESS_ROWS = 1L << 20;

  private final PathStateNativeNodeStore spool;
  private final PathStateStackTrie.NodeSink nodeSink;
  private final LeafSink leafSink;
  private final BuildProgress buildProgress;
  private final byte[] generationPrefix;
  private final List<BatchMutation> pendingRows = new ArrayList<>(DEFAULT_WRITE_BATCH_ROWS);
  private boolean built;
  private long inputRows;
  private long sortedRows;

  PathStateStoreTrieBuilder(Path spoolDirectory, Engine engine,
      PathStateStackTrie.NodeSink nodeSink) throws IOException {
    this(spoolDirectory, engine, new byte[0], nodeSink,
        (secureKey, encodedValue) -> { });
  }

  PathStateStoreTrieBuilder(Path spoolDirectory, Engine engine,
      PathStateStackTrie.NodeSink nodeSink, LeafSink leafSink) throws IOException {
    this(spoolDirectory, engine, new byte[0], nodeSink, leafSink);
  }

  PathStateStoreTrieBuilder(Path spoolDirectory, Engine engine, byte[] generationPrefix,
      PathStateStackTrie.NodeSink nodeSink, LeafSink leafSink) throws IOException {
    this(spoolDirectory, engine, generationPrefix, nodeSink, leafSink,
        (sortedRows, elapsedMillis) -> { });
  }

  PathStateStoreTrieBuilder(Path spoolDirectory, Engine engine, byte[] generationPrefix,
      PathStateStackTrie.NodeSink nodeSink, LeafSink leafSink,
      BuildProgress buildProgress) throws IOException {
    spool = PathStateNativeNodeStore.open(Objects.requireNonNull(spoolDirectory, "spoolDirectory"),
        Objects.requireNonNull(engine, "engine"));
    this.nodeSink = Objects.requireNonNull(nodeSink, "nodeSink");
    this.leafSink = Objects.requireNonNull(leafSink, "leafSink");
    this.buildProgress = Objects.requireNonNull(buildProgress, "buildProgress");
    this.generationPrefix = Arrays.copyOf(
        Objects.requireNonNull(generationPrefix, "generationPrefix"), generationPrefix.length);
  }

  /**
   * Spools one canonical leaf by its secure key. Repeated keys overwrite the prior value.
   *
   * <p>Only one bounded native write batch is retained in heap; values already flushed remain in
   * the temporary database until the sorted build pass reads them.
   */
  void put(byte[] secureKey, byte[] encodedValue) {
    requireCollecting();
    byte[] key = requireSecureKey(secureKey);
    byte[] value = nonEmpty(encodedValue, "encodedValue");
    pendingRows.add(BatchMutation.put(spoolKey(key), value));
    inputRows = Math.addExact(inputRows, 1L);
    if (pendingRows.size() >= DEFAULT_WRITE_BATCH_ROWS) {
      flushRows();
    }
  }

  /** Flushes the spool, streams it in secure-key order, and returns the canonical Store root. */
  byte[] build() throws IOException {
    requireCollecting();
    flushRows();
    PathStateStackTrie trie = new PathStateStackTrie(nodeSink);
    long startedNanos = System.nanoTime();
    PathStateNativeNodeStore.EntryConsumer consumer = entry -> {
      byte[] key = secureKey(entry.getKey());
      byte[] value = entry.getValue();
      trie.update(key, value);
      leafSink.put(key, value);
      sortedRows = Math.addExact(sortedRows, 1L);
      if (sortedRows % BUILD_PROGRESS_ROWS == 0) {
        buildProgress.report(sortedRows, elapsedMillis(startedNanos));
      }
    };
    if (generationPrefix.length == 0) {
      spool.scanAll(consumer);
    } else {
      spool.scanPrefix(generationPrefix, consumer);
    }
    if (sortedRows % BUILD_PROGRESS_ROWS != 0) {
      buildProgress.report(sortedRows, elapsedMillis(startedNanos));
    }
    built = true;
    return trie.rootHash();
  }

  private static long elapsedMillis(long startedNanos) {
    return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - startedNanos);
  }

  long getInputRows() {
    return inputRows;
  }

  long getSortedRows() {
    return sortedRows;
  }

  int getPendingRows() {
    return pendingRows.size();
  }

  private void flushRows() {
    if (!pendingRows.isEmpty()) {
      spool.writeBatch(new ArrayList<>(pendingRows));
      pendingRows.clear();
    }
  }

  private void requireCollecting() {
    if (built) {
      throw new IllegalStateException("path-state Store trie builder is already built");
    }
  }

  private static byte[] requireSecureKey(byte[] value) {
    byte[] key = Arrays.copyOf(Objects.requireNonNull(value, "secureKey"), value.length);
    if (key.length != PathMerkleTrie.SECURE_KEY_LENGTH) {
      throw new IllegalArgumentException("secureKey must contain exactly 32 bytes");
    }
    return key;
  }

  private byte[] spoolKey(byte[] secureKey) {
    byte[] key = Arrays.copyOf(generationPrefix,
        generationPrefix.length + secureKey.length);
    System.arraycopy(secureKey, 0, key, generationPrefix.length, secureKey.length);
    return key;
  }

  private byte[] secureKey(byte[] spoolKey) {
    if (spoolKey.length != generationPrefix.length + PathMerkleTrie.SECURE_KEY_LENGTH) {
      throw new IllegalStateException("path-state Store spool key has invalid length");
    }
    return Arrays.copyOfRange(spoolKey, generationPrefix.length, spoolKey.length);
  }

  private static byte[] nonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  @Override
  public void close() throws IOException {
    if (!built) {
      flushRows();
    }
    spool.close();
  }

  @FunctionalInterface
  interface LeafSink {

    void put(byte[] secureKey, byte[] encodedValue);
  }

  @FunctionalInterface
  interface BuildProgress {

    void report(long sortedRows, long elapsedMillis);
  }
}
