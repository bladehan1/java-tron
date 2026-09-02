package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.db2.common.DB;
import org.tron.core.trie.TrieImpl;

/** Independent {@link TrieImpl} verifier for one offline physical-store oracle window. */
final class PathStatePhysicalOracle {

  private static final int HASH_BATCH_ENTRIES = 4096;

  private PathStatePhysicalOracle() {
  }

  static Result verify(PathStatePhysicalStoreSet stores,
      PathStatePhysicalOracleWindow window, Path scratchDirectory, int rowsPerFlush)
      throws IOException {
    PathStatePhysicalStoreSet source = Objects.requireNonNull(stores, "stores");
    PathStatePhysicalOracleWindow input = Objects.requireNonNull(window, "window");
    if (rowsPerFlush <= 0) {
      throw new IllegalArgumentException("physical oracle rows per flush must be positive");
    }
    Path scratch = Objects.requireNonNull(scratchDirectory, "scratchDirectory")
        .toAbsolutePath().normalize();
    if (Files.exists(scratch, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("physical oracle scratch directory must be fresh: " + scratch);
    }
    Files.createDirectories(scratch);

    List<PathStatePhysicalGlobalIntent> targets = input.targets();
    List<PathStatePhysicalReverseJournal> journals = input.journals();
    Map<Integer, List<byte[]>> rootsByStore = new LinkedHashMap<>();
    long totalRows = 0;
    boolean complete = false;
    try {
      for (PathStateParticipantDescriptor.StoreIdentity identity
          : PathStateParticipantDescriptor.current().getStores()) {
        int storeId = identity.getStoreId();
        Path participantScratch = scratch.resolve(String.format("%02d", storeId));
        List<byte[]> roots = new ArrayList<>(targets.size());
        boolean participantComplete = false;
        try (NativeTrieDatabase database = new NativeTrieDatabase(participantScratch)) {
          TrieImpl trie = new TrieImpl(database);
          trie.setAsync(false);
          long[] rows = new long[1];
          source.participant(identity.getDbName()).scanFlat(entry -> {
            byte[] storedKey = entry.getKey();
            if (storedKey.length != PathStateCommitmentCodec.ROOT_LENGTH + 1
                || storedKey[0] != 'F') {
              throw new IOException("physical oracle F key is corrupt: "
                  + identity.getDbName());
            }
            trie.put(Arrays.copyOfRange(storedKey, 1, storedKey.length), entry.getValue());
            rows[0]++;
            if (rows[0] % rowsPerFlush == 0) {
              flush(trie, database);
            }
          });
          byte[] currentRoot = rootAndFlush(trie, database);
          requireRoot(participantTarget(targets.get(0), storeId).getStoreRoot(), currentRoot,
              identity.getDbName(), targets.get(0).getMetadata().getBlockNumber());
          roots.add(currentRoot);

          for (int index = 0; index < journals.size(); index++) {
            PathStatePhysicalReverseJournal.StoreReverse reverse = reverseFor(
                journals.get(index), storeId);
            if (reverse != null) {
              for (PathStatePhysicalReverseJournal.Entry entry : reverse.getFlatEntries()) {
                byte[] oldValue = entry.getOldValue();
                if (oldValue == null) {
                  trie.delete(entry.getKey());
                } else {
                  trie.put(entry.getKey(), oldValue);
                }
              }
            }
            byte[] parentRoot = rootAndFlush(trie, database);
            PathStatePhysicalGlobalIntent parent = targets.get(index + 1);
            requireRoot(participantTarget(parent, storeId).getStoreRoot(), parentRoot,
                identity.getDbName(), parent.getMetadata().getBlockNumber());
            roots.add(parentRoot);
          }
          totalRows = Math.addExact(totalRows, rows[0]);
          participantComplete = true;
        } catch (ArithmeticException overflow) {
          throw new IOException("physical oracle row count overflow", overflow);
        } finally {
          if (participantComplete) {
            deleteOwnedTree(scratch, participantScratch);
          }
        }
        rootsByStore.put(storeId, immutableRoots(roots));
      }

      for (int targetIndex = 0; targetIndex < targets.size(); targetIndex++) {
        TrieImpl superTrie = new TrieImpl();
        superTrie.setAsync(false);
        for (PathStateParticipantDescriptor.StoreIdentity identity
            : PathStateParticipantDescriptor.current().getStores()) {
          PathStateParticipant participant = source.participantScope().require(
              identity.getDbName());
          byte[] storeRoot = rootsByStore.get(identity.getStoreId()).get(targetIndex);
          superTrie.put(PathStateCommitmentCodec.superLeafKey(identity.getStoreId()),
              PathStateCommitmentCodec.superLeafValue(identity.getStoreId(),
                  identity.getDbName(), participant.getStoreFormatVersion(), storeRoot));
        }
        PathStatePhysicalGlobalIntent target = targets.get(targetIndex);
        requireRoot(target.getSuperRoot(), superTrie.getRootHash(), "super",
            target.getMetadata().getBlockNumber());
      }
      complete = true;
      return new Result(input.getBlockCount(), totalRows,
          input.getCurrentMetadata(), input.getOldestMetadata());
    } finally {
      if (complete) {
        deleteOwnedTree(scratch.getParent(), scratch);
      }
    }
  }

  private static PathStatePhysicalReverseJournal.StoreReverse reverseFor(
      PathStatePhysicalReverseJournal journal, int storeId) {
    for (PathStatePhysicalReverseJournal.StoreReverse reverse : journal.getStores()) {
      if (reverse.getStoreId() == storeId) {
        return reverse;
      }
    }
    return null;
  }

  private static PathStatePhysicalGlobalIntent.ParticipantTarget participantTarget(
      PathStatePhysicalGlobalIntent target, int storeId) throws IOException {
    for (PathStatePhysicalGlobalIntent.ParticipantTarget participant
        : target.getParticipants()) {
      if (participant.getStoreId() == storeId) {
        return participant;
      }
    }
    throw new IOException("physical oracle target Store is absent: " + storeId);
  }

  private static byte[] rootAndFlush(TrieImpl trie, NativeTrieDatabase database) {
    flush(trie, database);
    byte[] root = trie.getRootHash();
    return Arrays.copyOf(root, root.length);
  }

  private static void flush(TrieImpl trie, NativeTrieDatabase database) {
    trie.flush();
    database.flush();
  }

  private static void requireRoot(byte[] expected, byte[] actual, String store,
      long blockNumber) throws IOException {
    if (!Arrays.equals(expected, actual)) {
      throw new IOException("physical oracle root differs: store=" + store + ", block="
          + blockNumber);
    }
  }

  private static List<byte[]> immutableRoots(List<byte[]> roots) {
    List<byte[]> copies = new ArrayList<>(roots.size());
    for (byte[] root : roots) {
      copies.add(Arrays.copyOf(root, root.length));
    }
    return Collections.unmodifiableList(copies);
  }

  private static void deleteOwnedTree(Path owner, Path target) throws IOException {
    Path parent = Objects.requireNonNull(owner, "owner").toAbsolutePath().normalize();
    Path child = Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
    if (child.equals(parent) || !child.startsWith(parent) || Files.isSymbolicLink(child)) {
      throw new IOException("physical oracle refuses to delete unowned scratch: " + child);
    }
    if (!Files.exists(child, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    Files.walkFileTree(child, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path directory, IOException failure)
          throws IOException {
        if (failure != null) {
          throw failure;
        }
        Files.delete(directory);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  static final class Result {

    private final int blockCount;
    private final long rowCount;
    private final PathStateRootMetadata current;
    private final PathStateRootMetadata oldest;

    private Result(int blockCount, long rowCount, PathStateRootMetadata current,
        PathStateRootMetadata oldest) {
      this.blockCount = blockCount;
      this.rowCount = rowCount;
      this.current = current;
      this.oldest = oldest;
    }

    int getBlockCount() {
      return blockCount;
    }

    long getRowCount() {
      return rowCount;
    }

    PathStateRootMetadata getCurrent() {
      return PathStateRootMetadata.decode(current.encode());
    }

    PathStateRootMetadata getOldest() {
      return PathStateRootMetadata.decode(oldest.encode());
    }
  }

  private static final class NativeTrieDatabase implements DB<byte[], BytesCapsule>,
      AutoCloseable {

    private final PathStateNativeNodeStore store;
    private final Map<Key, byte[]> pending = new LinkedHashMap<>();

    private NativeTrieDatabase(Path directory) throws IOException {
      store = PathStateNativeNodeStore.open(directory, PathStateStoreManifest.Engine.ROCKSDB);
    }

    @Override
    public BytesCapsule get(byte[] key) {
      Key owned = new Key(key);
      if (pending.containsKey(owned)) {
        byte[] value = pending.get(owned);
        return value == null ? null : new BytesCapsule(value);
      }
      byte[] value = store.get(key);
      return value == null ? null : new BytesCapsule(value);
    }

    @Override
    public void put(byte[] key, BytesCapsule value) {
      pending.put(new Key(key), Arrays.copyOf(Objects.requireNonNull(value, "value").getData(),
          value.getData().length));
      flushIfFull();
    }

    @Override
    public void remove(byte[] key) {
      pending.put(new Key(key), null);
      flushIfFull();
    }

    private void flushIfFull() {
      if (pending.size() >= HASH_BATCH_ENTRIES) {
        flush();
      }
    }

    private void flush() {
      if (pending.isEmpty()) {
        return;
      }
      List<PathStateNativeNodeStore.BatchMutation> mutations = new ArrayList<>(pending.size());
      for (Map.Entry<Key, byte[]> entry : pending.entrySet()) {
        mutations.add(entry.getValue() == null
            ? PathStateNativeNodeStore.BatchMutation.delete(entry.getKey().value)
            : PathStateNativeNodeStore.BatchMutation.put(entry.getKey().value,
                entry.getValue()));
      }
      store.writeBatchUnsynced(mutations);
      pending.clear();
    }

    @Override
    public long size() {
      return -1;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public Iterator<Map.Entry<byte[], BytesCapsule>> iterator() {
      return Collections.<Map.Entry<byte[], BytesCapsule>>emptyList().iterator();
    }

    @Override
    public void close() {
      flush();
      try {
        store.close();
      } catch (IOException failure) {
        throw new IllegalStateException("failed to close physical oracle scratch", failure);
      }
    }

    @Override
    public String getDbName() {
      return "path-state-physical-oracle";
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], BytesCapsule> newInstance() {
      return null;
    }
  }

  private static final class Key {

    private final byte[] value;

    private Key(byte[] value) {
      this.value = Arrays.copyOf(Objects.requireNonNull(value, "value"), value.length);
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof Key
          && Arrays.equals(value, ((Key) other).value);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }
  }
}
