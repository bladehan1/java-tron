package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.tron.common.crypto.Hash;

/**
 * Backend-neutral secure-key Merkle Patricia trie with path-addressed node persistence.
 *
 * <p>This TASK-016 P1 core deliberately owns no database, block, history, or recovery lifecycle.
 * It rebuilds the canonical node set from current leaves when committed, then reconciles that set
 * through {@link PathNodeStore}. The later durable backend can replace the rebuild strategy without
 * changing the node byte contract.
 */
public final class PathMerkleTrie {

  public static final int SECURE_KEY_LENGTH = 32;

  private static final byte[] EMPTY_PATH = new byte[0];
  private static final byte[] EMPTY_RLP_ITEM = new byte[]{(byte) 0x80};
  private static final Comparator<BytesKey> UNSIGNED_KEY_COMPARATOR = (left, right) -> {
    byte[] leftBytes = left.bytes;
    byte[] rightBytes = right.bytes;
    int length = Math.min(leftBytes.length, rightBytes.length);
    for (int i = 0; i < length; i++) {
      int compared = Integer.compare(leftBytes[i] & 0xff, rightBytes[i] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(leftBytes.length, rightBytes.length);
  };

  private final PathNodeStore nodeStore;
  private final Map<BytesKey, byte[]> leaves = new TreeMap<>(UNSIGNED_KEY_COMPARATOR);
  private final Set<BytesKey> committedPaths = new LinkedHashSet<>();
  private byte[] rootHash = Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length);
  private boolean dirty;

  public PathMerkleTrie(PathNodeStore nodeStore) {
    this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
  }

  public synchronized void put(byte[] secureKey, byte[] encodedValue) {
    BytesKey key = secureKey(secureKey);
    byte[] value = nonEmpty(encodedValue, "encodedValue");
    byte[] previous = leaves.put(key, value);
    dirty |= !Arrays.equals(previous, value);
  }

  public synchronized void delete(byte[] secureKey) {
    dirty |= leaves.remove(secureKey(secureKey)) != null;
  }

  public synchronized byte[] get(byte[] secureKey) {
    byte[] value = leaves.get(secureKey(secureKey));
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  /** Reconciles path-addressed nodes and returns the canonical root hash. */
  public synchronized byte[] rootHash() {
    if (dirty) {
      commit();
    }
    return Arrays.copyOf(rootHash, rootHash.length);
  }

  public synchronized int size() {
    return leaves.size();
  }

  synchronized List<LeafEntry> leafEntries() {
    List<LeafEntry> entries = new ArrayList<>(leaves.size());
    for (Map.Entry<BytesKey, byte[]> entry : leaves.entrySet()) {
      entries.add(new LeafEntry(entry.getKey().copy(), entry.getValue()));
    }
    return entries;
  }

  /** Restores current leaves and verifies their complete path-node set without repairing it. */
  synchronized void restoreLeaves(Collection<LeafEntry> entries) {
    if (!leaves.isEmpty() || !committedPaths.isEmpty() || dirty) {
      throw new IllegalStateException("path trie is not empty before leaf restoration");
    }
    for (LeafEntry entry : Objects.requireNonNull(entries, "entries")) {
      LeafEntry present = Objects.requireNonNull(entry, "entry");
      if (leaves.put(secureKey(present.secureKey),
          nonEmpty(present.encodedValue, "encodedValue")) != null) {
        throw new IllegalArgumentException("duplicate restored path-state leaf");
      }
    }
    Map<BytesKey, byte[]> expectedNodes = buildCurrentNodes();
    for (Map.Entry<BytesKey, byte[]> entry : expectedNodes.entrySet()) {
      if (!Arrays.equals(nodeStore.get(entry.getKey().bytes), entry.getValue())) {
        throw new IllegalStateException("restored leaves do not match persisted path nodes");
      }
    }
    committedPaths.addAll(expectedNodes.keySet());
    rootHash = rootHash(expectedNodes);
  }

  /** Verifies every path owned by the current committed node set without repairing corruption. */
  public synchronized void verifyNodeStore() {
    if (dirty) {
      throw new IllegalStateException("cannot verify a dirty path trie");
    }
    Map<BytesKey, byte[]> expectedNodes = buildCurrentNodes();
    if (!committedPaths.equals(expectedNodes.keySet())) {
      throw new IllegalStateException("committed path set does not match current leaves");
    }
    byte[] expectedRoot = rootHash(expectedNodes);
    if (!Arrays.equals(rootHash, expectedRoot)) {
      throw new IllegalStateException("committed path root does not match current leaves");
    }
    for (Map.Entry<BytesKey, byte[]> entry : expectedNodes.entrySet()) {
      if (!Arrays.equals(nodeStore.get(entry.getKey().bytes), entry.getValue())) {
        throw new IllegalStateException("missing or corrupt committed path node");
      }
    }
  }

  private void commit() {
    Map<BytesKey, byte[]> nextNodes = buildCurrentNodes();
    rootHash = rootHash(nextNodes);

    Set<BytesKey> stalePaths = new LinkedHashSet<>(committedPaths);
    stalePaths.removeAll(nextNodes.keySet());
    for (BytesKey stalePath : stalePaths) {
      nodeStore.delete(stalePath.copy());
    }
    for (Map.Entry<BytesKey, byte[]> entry : nextNodes.entrySet()) {
      byte[] existing = nodeStore.get(entry.getKey().bytes);
      if (!Arrays.equals(existing, entry.getValue())) {
        nodeStore.put(entry.getKey().copy(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
      }
    }
    committedPaths.clear();
    committedPaths.addAll(nextNodes.keySet());
    dirty = false;
  }

  private Map<BytesKey, byte[]> buildCurrentNodes() {
    Map<BytesKey, byte[]> nodes = new LinkedHashMap<>();
    if (!leaves.isEmpty()) {
      List<Leaf> entries = new ArrayList<>(leaves.size());
      for (Map.Entry<BytesKey, byte[]> entry : leaves.entrySet()) {
        entries.add(new Leaf(toNibbles(entry.getKey().bytes), entry.getValue()));
      }
      build(entries, 0, EMPTY_PATH, nodes);
    }
    return nodes;
  }

  private static byte[] rootHash(Map<BytesKey, byte[]> nodes) {
    if (nodes.isEmpty()) {
      return Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length);
    }
    byte[] root = nodes.get(new BytesKey(EMPTY_PATH));
    if (root == null) {
      throw new IllegalStateException("path node set has no root");
    }
    return Hash.sha3(root);
  }

  private static byte[] build(List<Leaf> entries, int depth, byte[] nodePath,
      Map<BytesKey, byte[]> nodes) {
    if (entries.size() == 1) {
      Leaf leaf = entries.get(0);
      byte[] encoded = rlpList(rlpItem(compactPath(leaf.nibbles, depth, true)),
          rlpItem(leaf.value));
      nodes.put(new BytesKey(nodePath), encoded);
      return encoded;
    }

    int shared = sharedPrefix(entries, depth);
    if (shared > 0) {
      byte[] childPath = append(nodePath, entries.get(0).nibbles, depth, shared);
      byte[] child = build(entries, depth + shared, childPath, nodes);
      byte[] prefix = Arrays.copyOfRange(entries.get(0).nibbles, depth, depth + shared);
      byte[] encoded = rlpList(rlpItem(compactPath(prefix, 0, false)), nodeReference(child));
      nodes.put(new BytesKey(nodePath), encoded);
      return encoded;
    }

    List<byte[]> encodedChildren = new ArrayList<>(Collections.nCopies(17, EMPTY_RLP_ITEM));
    int start = 0;
    while (start < entries.size()) {
      int nibble = entries.get(start).nibbles[depth];
      int end = start + 1;
      while (end < entries.size() && entries.get(end).nibbles[depth] == nibble) {
        end++;
      }
      byte[] childPath = append(nodePath, new byte[]{(byte) nibble}, 0, 1);
      byte[] child = build(entries.subList(start, end), depth + 1, childPath, nodes);
      encodedChildren.set(nibble, nodeReference(child));
      start = end;
    }
    byte[] encoded = rlpList(encodedChildren.toArray(new byte[encodedChildren.size()][]));
    nodes.put(new BytesKey(nodePath), encoded);
    return encoded;
  }

  private static int sharedPrefix(List<Leaf> entries, int depth) {
    int shared = 0;
    int keyLength = entries.get(0).nibbles.length;
    while (depth + shared < keyLength) {
      byte expected = entries.get(0).nibbles[depth + shared];
      for (int i = 1; i < entries.size(); i++) {
        if (entries.get(i).nibbles[depth + shared] != expected) {
          return shared;
        }
      }
      shared++;
    }
    return shared;
  }

  private static byte[] nodeReference(byte[] encodedNode) {
    return encodedNode.length < SECURE_KEY_LENGTH ? encodedNode : rlpItem(Hash.sha3(encodedNode));
  }

  private static byte[] compactPath(byte[] nibbles, int offset, boolean leaf) {
    int length = nibbles.length - offset;
    boolean odd = (length & 1) != 0;
    byte[] compact = new byte[1 + length / 2];
    int flag = leaf ? 2 : 0;
    int source = offset;
    if (odd) {
      compact[0] = (byte) ((flag + 1) << 4 | nibbles[source++]);
    } else {
      compact[0] = (byte) (flag << 4);
    }
    int target = 1;
    while (source < nibbles.length) {
      compact[target++] = (byte) (nibbles[source++] << 4 | nibbles[source++]);
    }
    return compact;
  }

  private static byte[] toNibbles(byte[] key) {
    byte[] nibbles = new byte[key.length * 2];
    for (int i = 0; i < key.length; i++) {
      nibbles[i * 2] = (byte) ((key[i] >>> 4) & 0x0f);
      nibbles[i * 2 + 1] = (byte) (key[i] & 0x0f);
    }
    return nibbles;
  }

  private static byte[] append(byte[] prefix, byte[] suffix, int offset, int length) {
    byte[] result = Arrays.copyOf(prefix, prefix.length + length);
    System.arraycopy(suffix, offset, result, prefix.length, length);
    return result;
  }

  private static BytesKey secureKey(byte[] value) {
    byte[] key = Objects.requireNonNull(value, "secureKey");
    if (key.length != SECURE_KEY_LENGTH) {
      throw new IllegalArgumentException("secureKey must be exactly 32 bytes");
    }
    return new BytesKey(key);
  }

  private static byte[] nonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  private static byte[] rlpItem(byte[] raw) {
    if (raw.length == 1 && (raw[0] & 0xff) < 0x80) {
      return Arrays.copyOf(raw, raw.length);
    }
    byte[] prefix = rlpLength(raw.length, 0x80, 0xb7);
    return concatenate(prefix, raw);
  }

  private static byte[] rlpList(byte[]... encodedItems) {
    int payloadLength = 0;
    for (byte[] item : encodedItems) {
      payloadLength = Math.addExact(payloadLength, item.length);
    }
    byte[] prefix = rlpLength(payloadLength, 0xc0, 0xf7);
    byte[] result = Arrays.copyOf(prefix, Math.addExact(prefix.length, payloadLength));
    int offset = prefix.length;
    for (byte[] item : encodedItems) {
      System.arraycopy(item, 0, result, offset, item.length);
      offset += item.length;
    }
    return result;
  }

  private static byte[] rlpLength(int length, int shortOffset, int longOffset) {
    if (length < 56) {
      return new byte[]{(byte) (shortOffset + length)};
    }
    int lengthOfLength = (Integer.SIZE - Integer.numberOfLeadingZeros(length) + 7) / 8;
    byte[] encoded = new byte[lengthOfLength + 1];
    encoded[0] = (byte) (longOffset + lengthOfLength);
    int remaining = length;
    for (int i = lengthOfLength; i > 0; i--) {
      encoded[i] = (byte) remaining;
      remaining >>>= Byte.SIZE;
    }
    return encoded;
  }

  private static byte[] concatenate(byte[] first, byte[] second) {
    byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  private static final class Leaf {

    private final byte[] nibbles;
    private final byte[] value;

    private Leaf(byte[] nibbles, byte[] value) {
      this.nibbles = nibbles;
      this.value = value;
    }
  }

  static final class LeafEntry {

    private final byte[] secureKey;
    private final byte[] encodedValue;

    LeafEntry(byte[] secureKey, byte[] encodedValue) {
      this.secureKey = Arrays.copyOf(Objects.requireNonNull(secureKey, "secureKey"),
          secureKey.length);
      this.encodedValue = Arrays.copyOf(Objects.requireNonNull(encodedValue, "encodedValue"),
          encodedValue.length);
    }

    byte[] getSecureKey() {
      return Arrays.copyOf(secureKey, secureKey.length);
    }

    byte[] getEncodedValue() {
      return Arrays.copyOf(encodedValue, encodedValue.length);
    }
  }

  private static final class BytesKey {

    private final byte[] bytes;

    private BytesKey(byte[] bytes) {
      this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    private byte[] copy() {
      return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof BytesKey
          && Arrays.equals(bytes, ((BytesKey) other).bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
