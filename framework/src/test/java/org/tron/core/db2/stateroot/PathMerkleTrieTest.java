package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.tron.common.crypto.Hash;
import org.tron.core.trie.TrieImpl;

public class PathMerkleTrieTest {

  @Test
  public void emptyAndSingleLeafMatchIndependentTrieOracle() {
    InMemoryPathNodeStore store = new InMemoryPathNodeStore();
    PathMerkleTrie trie = new PathMerkleTrie(store);
    assertArrayEquals(Hash.EMPTY_TRIE_HASH, trie.rootHash());
    assertTrue(store.nodes.isEmpty());

    byte[] key = filledKey(0x11);
    byte[] value = Hex.decode("c20180");
    trie.put(key, value);

    assertArrayEquals(referenceRoot(new byte[][]{key}, new byte[][]{value}), trie.rootHash());
    assertArrayEquals(value, trie.get(key));
    assertEquals(1, trie.size());
    assertTrue(store.nodes.containsKey(""));
  }

  @Test
  public void mutationOrderProducesSameRootAndPathNodeSet() {
    byte[][] keys = {filledKey(0x11), keyWithTail(0x11, 0x12), filledKey(0x21),
        keyWithTail(0x21, 0x22)};
    byte[][] values = {value("one"), value("two"), value("three"), value("four")};

    InMemoryPathNodeStore forwardStore = new InMemoryPathNodeStore();
    PathMerkleTrie forward = new PathMerkleTrie(forwardStore);
    for (int i = 0; i < keys.length; i++) {
      forward.put(keys[i], values[i]);
    }

    InMemoryPathNodeStore reverseStore = new InMemoryPathNodeStore();
    PathMerkleTrie reverse = new PathMerkleTrie(reverseStore);
    for (int i = keys.length - 1; i >= 0; i--) {
      reverse.put(keys[i], values[i]);
    }

    byte[] expected = referenceRoot(keys, values);
    assertArrayEquals(
        Hex.decode("dc1c7bfcacb455baeca2454d8aedbe4b17da4a66364fbc0baa7e5919cacf2bdc"),
        expected);
    assertArrayEquals(expected, forward.rootHash());
    assertArrayEquals(expected, reverse.rootHash());
    assertNodeMapsEqual(forwardStore.nodes, reverseStore.nodes);
  }

  @Test
  public void updateAndDeleteCompressCanonicalPaths() {
    byte[] first = filledKey(0x33);
    byte[] second = keyWithTail(0x33, 0x34);
    byte[] third = filledKey(0x44);
    InMemoryPathNodeStore store = new InMemoryPathNodeStore();
    PathMerkleTrie trie = new PathMerkleTrie(store);
    trie.put(first, value("first"));
    trie.put(second, value("second"));
    trie.put(third, value("third"));
    trie.rootHash();
    int expandedNodeCount = store.nodes.size();

    trie.put(first, value("updated"));
    trie.delete(second);
    byte[] expected = referenceRoot(new byte[][]{first, third},
        new byte[][]{value("updated"), value("third")});
    assertArrayEquals(expected, trie.rootHash());
    assertNull(trie.get(second));
    assertTrue(store.nodes.size() < expandedNodeCount);

    trie.delete(first);
    trie.delete(third);
    assertArrayEquals(Hash.EMPTY_TRIE_HASH, trie.rootHash());
    assertTrue(store.nodes.isEmpty());
  }

  @Test
  public void rejectsInvalidKeysAndEmptyValues() {
    PathMerkleTrie trie = new PathMerkleTrie(new InMemoryPathNodeStore());
    assertThrows(NullPointerException.class, () -> new PathMerkleTrie(null));
    assertThrows(IllegalArgumentException.class, () -> trie.put(new byte[31], value("x")));
    assertThrows(IllegalArgumentException.class,
        () -> trie.put(new byte[PathMerkleTrie.SECURE_KEY_LENGTH], new byte[0]));
    assertThrows(NullPointerException.class, () -> trie.delete(null));
  }

  @Test
  public void detectsMissingCorruptAndDirtyCommittedNodes() {
    InMemoryPathNodeStore corruptStore = new InMemoryPathNodeStore();
    PathMerkleTrie corruptTrie = new PathMerkleTrie(corruptStore);
    corruptTrie.put(filledKey(0x55), value("value"));
    corruptTrie.rootHash();
    corruptTrie.verifyNodeStore();
    corruptStore.nodes.put("", new byte[]{1});
    assertThrows(IllegalStateException.class, corruptTrie::verifyNodeStore);

    InMemoryPathNodeStore missingStore = new InMemoryPathNodeStore();
    PathMerkleTrie missingTrie = new PathMerkleTrie(missingStore);
    missingTrie.put(filledKey(0x66), value("value"));
    missingTrie.rootHash();
    missingStore.nodes.remove("");
    assertThrows(IllegalStateException.class, missingTrie::verifyNodeStore);

    PathMerkleTrie dirtyTrie = new PathMerkleTrie(new InMemoryPathNodeStore());
    dirtyTrie.put(filledKey(0x77), value("value"));
    assertThrows(IllegalStateException.class, dirtyTrie::verifyNodeStore);
  }

  @Test
  public void singleLeafUpdateRewritesOnlyItsMaterializedPath() {
    int leafCount = 32;
    byte[][] keys = new byte[leafCount][];
    byte[][] values = new byte[leafCount][];
    InMemoryPathNodeStore store = new InMemoryPathNodeStore();
    PathMerkleTrie trie = new PathMerkleTrie(store);
    for (int i = 0; i < leafCount; i++) {
      keys[i] = filledKey(i);
      values[i] = value("value-" + i);
      trie.put(keys[i], values[i]);
    }
    trie.rootHash();
    int fullNodeCount = store.nodes.size();

    values[17] = value("updated");
    trie.put(keys[17], values[17]);

    assertArrayEquals(referenceRoot(keys, values), trie.rootHash());
    assertTrue(fullNodeCount > 3);
    assertEquals(3, trie.getLastNodePuts());
    assertEquals(3, trie.getLastNodeDeletes());
    trie.verifyNodeStore();
  }

  @Test
  public void restoresRootAndLoadsOnlyTheChangedPath() {
    int leafCount = 1_000;
    byte[][] keys = new byte[leafCount][];
    byte[][] values = new byte[leafCount][];
    InMemoryPathNodeStore sourceStore = new InMemoryPathNodeStore();
    PathMerkleTrie source = new PathMerkleTrie(sourceStore);
    for (int index = 0; index < leafCount; index++) {
      keys[index] = indexedKey(index);
      values[index] = value("value-" + index);
      source.put(keys[index], values[index]);
    }
    byte[] originalRoot = source.rootHash();

    InMemoryPathNodeStore lazyStore = new InMemoryPathNodeStore();
    lazyStore.nodes.putAll(sourceStore.nodes);
    PathMerkleTrie restored = new PathMerkleTrie(lazyStore);
    restored.restoreRoot(originalRoot);
    assertEquals(1, lazyStore.gets);
    assertArrayEquals(values[517], restored.get(keys[517]));
    assertTrue(lazyStore.gets <= PathMerkleTrie.SECURE_KEY_LENGTH * 2 + 2);

    values[517] = value("updated-lazily");
    restored.put(keys[517], values[517]);
    assertArrayEquals(referenceRoot(keys, values), restored.rootHash());
    assertTrue(restored.getLastNodePuts() <= PathMerkleTrie.SECURE_KEY_LENGTH * 2 + 1);
  }

  private static byte[] referenceRoot(byte[][] keys, byte[][] values) {
    TrieImpl reference = new TrieImpl();
    reference.setAsync(false);
    for (int i = 0; i < keys.length; i++) {
      reference.put(keys[i], values[i]);
    }
    return reference.getRootHash();
  }

  private static byte[] filledKey(int value) {
    byte[] key = new byte[PathMerkleTrie.SECURE_KEY_LENGTH];
    Arrays.fill(key, (byte) value);
    return key;
  }

  private static byte[] keyWithTail(int prefix, int tail) {
    byte[] key = filledKey(prefix);
    key[key.length - 1] = (byte) tail;
    return key;
  }

  private static byte[] indexedKey(int index) {
    byte[] key = new byte[PathMerkleTrie.SECURE_KEY_LENGTH];
    ByteBuffer.wrap(key, key.length - Integer.BYTES, Integer.BYTES).putInt(index);
    return key;
  }

  private static byte[] value(String value) {
    return PathStateCommitmentCodec.presentLeafValue(value.getBytes(StandardCharsets.UTF_8));
  }

  private static void assertNodeMapsEqual(Map<String, byte[]> expected,
      Map<String, byte[]> actual) {
    assertEquals(expected.keySet(), actual.keySet());
    for (String path : expected.keySet()) {
      assertArrayEquals(expected.get(path), actual.get(path));
    }
  }

  private static final class InMemoryPathNodeStore implements PathNodeStore {

    private final Map<String, byte[]> nodes = new LinkedHashMap<>();
    private int gets;

    @Override
    public byte[] get(byte[] path) {
      gets++;
      byte[] value = nodes.get(Hex.toHexString(path));
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      nodes.put(Hex.toHexString(path), Arrays.copyOf(encodedNode, encodedNode.length));
    }

    @Override
    public void delete(byte[] path) {
      nodes.remove(Hex.toHexString(path));
    }
  }
}
