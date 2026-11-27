package org.tron.core.db2.archive.serving;

import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration.ExactKey;

/**
 * Cross-window change accumulation for the serving index, Geth's {@code batchIndexer} shape:
 * every committed History block appends the real changes of each touched key to one in-memory
 * table, so a hot key is opened (KeyMeta, directory, tail page) once per flush instead of once
 * per read window.
 *
 * <p>Block validation is exactly the incremental plan builder's: contiguity, parent hashes,
 * the participant set, per-block store order and per-store key order. The source digest rolls
 * with the same per-step algorithm as the descriptor, so a sealed accumulation is
 * byte-identical to committing the same range as one window. Decoded block bodies are not
 * retained; only keys, native epochs and the rolling identities stay in memory.
 */
final class ServingChangeAccumulator {
  private final long baseThrough;
  private final byte[] baseHash;
  private final byte[] baseLatestIdentity;
  private final List<String> participants;
  private final Set<String> participantSet;
  private final Map<ExactKey, KeyEpochs> changes = new LinkedHashMap<>();
  private long pendingChanges;
  private long readThrough;
  private byte[] readHash;
  private byte[] rolledDigest;
  private byte[] lastStepDigest;
  private long previousBlock;
  private byte[] previousHash;
  private boolean sealed;

  ServingChangeAccumulator(long baseThrough, byte[] baseHash, byte[] baseSourceDigest,
      byte[] baseLatestIdentity, List<String> participants) {
    if (baseThrough < 0) {
      throw new IllegalArgumentException("baseThrough must not be negative");
    }
    this.baseThrough = baseThrough;
    this.baseHash = requireHash(baseHash, "baseHash");
    this.rolledDigest = requireHash(baseSourceDigest, "baseSourceDigest");
    this.baseLatestIdentity = requireHash(baseLatestIdentity, "baseLatestIdentity");
    this.participants = Collections.unmodifiableList(
        new ArrayList<>(Objects.requireNonNull(participants, "participants")));
    this.participantSet = new HashSet<>(participants);
    this.previousBlock = baseThrough;
    this.previousHash = this.baseHash;
    this.readThrough = baseThrough;
    this.readHash = this.baseHash;
  }

  /** Validates one committed block and appends its real changes; the whole block or nothing. */
  void addBlock(BlockReverseDiff diff, byte[] stepDigest) {
    if (sealed) {
      throw new IllegalStateException("Serving accumulation is already sealed");
    }
    BlockSnapshotMeta meta = Objects.requireNonNull(diff, "committedDiff").getMeta();
    if (meta.getEpoch() != previousBlock + 1 || meta.getBlockNumber() != previousBlock + 1
        || !Arrays.equals(meta.getParentHash(), previousHash)) {
      throw new IllegalArgumentException("Serving committed diff suffix is not contiguous");
    }
    String previousDatabase = null;
    for (BlockReverseDiff.DbGroup group : diff.getGroups()) {
      String dbName = group.getDbName();
      if (!participantSet.contains(dbName) || previousDatabase != null
          && previousDatabase.compareTo(dbName) >= 0) {
        throw new IllegalArgumentException("Serving committed diff Store coverage is invalid");
      }
      byte[] previousKey = null;
      for (BlockReverseDiff.Entry entry : group.getEntries()) {
        byte[] key = entry.getKey();
        if (previousKey != null
            && UnsignedBytes.lexicographicalComparator().compare(previousKey, key) >= 0) {
          throw new IllegalArgumentException("Serving committed diff keys are not unique");
        }
        KeyEpochs epochs = changes.get(ExactKey.owned(dbName, key));
        if (epochs == null) {
          // The probe borrows the diff's array; only inserted keys keep their own copy.
          changes.put(ExactKey.owned(dbName, Arrays.copyOf(key, key.length)),
              KeyEpochs.first(meta.getEpoch()));
        } else {
          epochs.add(meta.getEpoch());
        }
        pendingChanges++;
        previousKey = key;
      }
      previousDatabase = dbName;
    }
    byte[] step = requireHash(stepDigest, "stepDigest");
    rolledDigest = PersistentServingKeyIndexGeneration.rollSourceDigest(rolledDigest,
        Collections.singletonList(step));
    lastStepDigest = step.clone();
    readThrough = meta.getBlockNumber();
    readHash = meta.getBlockHash().clone();
    previousBlock = meta.getBlockNumber();
    previousHash = readHash;
  }

  long pendingChanges() {
    return pendingChanges;
  }

  long readThrough() {
    return readThrough;
  }

  /** Seals the accumulation into the store's flush input; the accumulator is spent afterwards. */
  ServingIndexStore.GroupedAppend seal() {
    if (sealed) {
      throw new IllegalStateException("Serving accumulation is already sealed");
    }
    sealed = true;
    Map<ExactKey, long[]> changes = new LinkedHashMap<>();
    this.changes.forEach((key, epochs) -> changes.put(key, epochs.toArray()));
    return new ServingIndexStore.GroupedAppend(baseThrough, baseHash, readThrough, readHash,
        participants, rolledDigest, lastStepDigest == null ? baseLatestIdentity : lastStepDigest,
        pendingChanges, changes);
  }

  private static byte[] requireHash(byte[] hash, String name) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException("Serving " + name + " must be 32 bytes");
    }
    return hash.clone();
  }

  /** Growable native epoch list; one boxed-Long-free array per distinct key. */
  private static final class KeyEpochs {
    private long[] values = new long[1];
    private int size;

    private static KeyEpochs first(long epoch) {
      KeyEpochs epochs = new KeyEpochs();
      epochs.add(epoch);
      return epochs;
    }

    private void add(long epoch) {
      if (size == values.length) {
        values = Arrays.copyOf(values, size * 2);
      }
      values[size++] = epoch;
    }

    private long[] toArray() {
      return Arrays.copyOfRange(values, 0, size);
    }
  }

}