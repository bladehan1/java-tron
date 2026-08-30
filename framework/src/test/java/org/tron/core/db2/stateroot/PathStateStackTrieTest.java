package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import org.tron.common.crypto.Hash;

public class PathStateStackTrieTest {

  private static final Comparator<Row> UNSIGNED = (left, right) -> {
    for (int index = 0; index < left.key.length; index++) {
      int compared = Integer.compare(left.key[index] & 0xff, right.key[index] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return 0;
  };

  @Test
  public void matchesReferenceRootAndEmitsDurableNodes() {
    List<Row> rows = rows(10_000, 19L);
    Map<String, byte[]> emitted = new LinkedHashMap<>();
    PathStateStackTrie stack = new PathStateStackTrie(
        (path, encoded) -> emitted.put(hex(path), encoded));
    PathMerkleTrie reference = new PathMerkleTrie(new MemoryNodeStore());

    for (Row row : rows) {
      stack.update(row.key, row.value);
      reference.put(row.key, row.value);
    }

    byte[] root = stack.rootHash();
    assertArrayEquals(reference.rootHash(), root);
    assertArrayEquals(root, Hash.sha3(emitted.get("")));
    assertTrue(stack.getEmittedNodes() > 1);
  }

  @Test
  public void retainsOnlyAscendingFrontierForLargeInput() {
    PathStateStackTrie stack = new PathStateStackTrie((path, encoded) -> { });
    for (Row row : rows(100_000, 41L)) {
      stack.update(row.key, row.value);
      assertTrue(stack.retainedNodes() <= 1024);
    }
    stack.rootHash();
    assertTrue(stack.retainedNodes() <= 1);
  }

  @Test
  public void rejectsDuplicateAndDescendingKeys() {
    byte[] low = key(1);
    byte[] high = key(2);
    PathStateStackTrie duplicate = new PathStateStackTrie((path, encoded) -> { });
    duplicate.update(low, new byte[]{1});
    assertThrows(IllegalArgumentException.class,
        () -> duplicate.update(low, new byte[]{2}));

    PathStateStackTrie descending = new PathStateStackTrie((path, encoded) -> { });
    descending.update(high, new byte[]{1});
    assertThrows(IllegalArgumentException.class,
        () -> descending.update(low, new byte[]{2}));
  }

  @Test
  public void returnsCanonicalEmptyRoot() {
    assertArrayEquals(Hash.EMPTY_TRIE_HASH,
        new PathStateStackTrie((path, encoded) -> { }).rootHash());
  }

  private static List<Row> rows(int count, long seed) {
    Random random = new Random(seed);
    List<Row> rows = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      byte[] key = new byte[PathMerkleTrie.SECURE_KEY_LENGTH];
      random.nextBytes(key);
      ByteBuffer.wrap(key, key.length - Integer.BYTES, Integer.BYTES).putInt(index);
      byte[] value = new byte[8 + random.nextInt(96)];
      random.nextBytes(value);
      rows.add(new Row(key, value));
    }
    rows.sort(UNSIGNED);
    return rows;
  }

  private static byte[] key(int suffix) {
    byte[] key = new byte[PathMerkleTrie.SECURE_KEY_LENGTH];
    key[key.length - 1] = (byte) suffix;
    return key;
  }

  private static String hex(byte[] value) {
    StringBuilder result = new StringBuilder(value.length * 2);
    for (byte present : value) {
      result.append(String.format("%02x", present & 0xff));
    }
    return result.toString();
  }

  private static final class Row {

    private final byte[] key;
    private final byte[] value;

    private Row(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }

  private static final class MemoryNodeStore implements PathNodeStore {

    private final Map<String, byte[]> nodes = new LinkedHashMap<>();

    @Override
    public byte[] get(byte[] path) {
      return nodes.get(hex(path));
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
      nodes.put(hex(path), encodedNode);
    }

    @Override
    public void delete(byte[] path) {
      nodes.remove(hex(path));
    }
  }
}
