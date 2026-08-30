package org.tron.core.db2.stateroot;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;
import org.tron.common.crypto.Hash;

/**
 * Ascending-key MPT builder that commits completed subtrees and retains only the right frontier.
 *
 * <p>This follows the memory boundary of Geth's StackTrie: once ascending input moves beyond a
 * subtree, that subtree is encoded, emitted to the node sink, replaced by its reference, and its
 * children are released. Keys must be fixed secure keys in strict unsigned order.
 */
final class PathStateStackTrie {

  private static final int EMPTY = 0;
  private static final int BRANCH = 1;
  private static final int EXTENSION = 2;
  private static final int LEAF = 3;
  private static final int HASHED = 4;
  private static final byte[] EMPTY_RLP_ITEM = new byte[]{(byte) 0x80};

  private final NodeSink sink;
  private final Node root = new Node();
  private byte[] previousKey;
  private boolean finished;
  private long emittedNodes;

  PathStateStackTrie(NodeSink sink) {
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  void update(byte[] secureKey, byte[] encodedValue) {
    requireOpen();
    byte[] key = secureKey(secureKey);
    byte[] value = nonEmpty(encodedValue, "encodedValue");
    if (previousKey != null && compareUnsigned(previousKey, key) >= 0) {
      throw new IllegalArgumentException("path-state stack trie keys must be strictly ascending");
    }
    previousKey = key;
    insert(root, toNibbles(key), value, new byte[0]);
  }

  byte[] rootHash() {
    if (!finished) {
      hash(root, new byte[0]);
      finished = true;
    }
    return root.type == EMPTY
        ? Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length)
        : Arrays.copyOf(root.value, root.value.length);
  }

  long getEmittedNodes() {
    return emittedNodes;
  }

  int retainedNodes() {
    return retainedNodes(root);
  }

  private void requireOpen() {
    if (finished) {
      throw new IllegalStateException("path-state stack trie is already finished");
    }
  }

  private void insert(Node node, byte[] key, byte[] value, byte[] path) {
    switch (node.type) {
      case EMPTY:
        node.becomeLeaf(key, value);
        return;
      case BRANCH:
        int branch = key[0] & 0xff;
        for (int sibling = branch - 1; sibling >= 0; sibling--) {
          if (node.children[sibling] != null) {
            if (node.children[sibling].type != HASHED) {
              hash(node.children[sibling], append(path, (byte) sibling));
            }
            break;
          }
        }
        if (node.children[branch] == null) {
          node.children[branch] = Node.leaf(slice(key, 1), value);
        } else {
          insert(node.children[branch], slice(key, 1), value, append(path, key[0]));
        }
        return;
      case EXTENSION:
        splitExtension(node, key, value, path);
        return;
      case LEAF:
        splitLeaf(node, key, value, path);
        return;
      case HASHED:
        throw new IllegalStateException("path-state stack trie inserted into committed subtree");
      default:
        throw new IllegalStateException("unknown path-state stack trie node");
    }
  }

  private void splitExtension(Node node, byte[] key, byte[] value, byte[] path) {
    int shared = commonPrefix(node.key, key);
    if (shared == node.key.length) {
      insert(node.children[0], slice(key, shared), value,
          concatenate(path, slice(key, 0, shared)));
      return;
    }
    Node oldChild;
    if (shared < node.key.length - 1) {
      oldChild = Node.extension(slice(node.key, shared + 1), node.children[0]);
      hash(oldChild, concatenate(path, slice(node.key, 0, shared + 1)));
    } else {
      oldChild = node.children[0];
      hash(oldChild, concatenate(path, node.key));
    }
    byte oldBranch = node.key[shared];
    byte newBranch = key[shared];
    Node branch = new Node();
    branch.type = BRANCH;
    branch.children = new Node[16];
    branch.children[oldBranch & 0xff] = oldChild;
    branch.children[newBranch & 0xff] = Node.leaf(slice(key, shared + 1), value);
    if (shared == 0) {
      node.copyFrom(branch);
    } else {
      node.type = EXTENSION;
      node.key = slice(node.key, 0, shared);
      node.value = null;
      node.children = new Node[1];
      node.children[0] = branch;
    }
  }

  private void splitLeaf(Node node, byte[] key, byte[] value, byte[] path) {
    int shared = commonPrefix(node.key, key);
    if (shared >= node.key.length || shared >= key.length) {
      throw new IllegalArgumentException("path-state stack trie duplicate or prefixed key");
    }
    Node branch = new Node();
    branch.type = BRANCH;
    branch.children = new Node[16];
    byte oldBranch = node.key[shared];
    byte newBranch = key[shared];
    Node oldLeaf = Node.leaf(slice(node.key, shared + 1), node.value);
    hash(oldLeaf, concatenate(path, slice(node.key, 0, shared + 1)));
    branch.children[oldBranch & 0xff] = oldLeaf;
    branch.children[newBranch & 0xff] = Node.leaf(slice(key, shared + 1), value);
    if (shared == 0) {
      node.copyFrom(branch);
    } else {
      node.type = EXTENSION;
      node.key = slice(node.key, 0, shared);
      node.value = null;
      node.children = new Node[]{branch};
    }
  }

  private void hash(Node node, byte[] path) {
    if (node.type == HASHED) {
      return;
    }
    byte[] encoded;
    switch (node.type) {
      case EMPTY:
        node.type = HASHED;
        node.key = null;
        node.children = null;
        node.value = Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length);
        return;
      case BRANCH:
        byte[][] children = new byte[17][];
        for (int index = 0; index < 16; index++) {
          Node child = node.children[index];
          if (child == null) {
            children[index] = EMPTY_RLP_ITEM;
          } else {
            hash(child, append(path, (byte) index));
            children[index] = nodeReference(child.value);
            node.children[index] = null;
          }
        }
        children[16] = EMPTY_RLP_ITEM;
        encoded = rlpList(children);
        break;
      case EXTENSION:
        hash(node.children[0], concatenate(path, node.key));
        encoded = rlpList(rlpItem(compactPath(node.key, false)),
            nodeReference(node.children[0].value));
        node.children[0] = null;
        break;
      case LEAF:
        encoded = rlpList(rlpItem(compactPath(node.key, true)), rlpItem(node.value));
        break;
      default:
        throw new IllegalStateException("unknown path-state stack trie node");
    }
    node.type = HASHED;
    node.key = null;
    node.children = null;
    node.value = encoded.length < 32 && path.length != 0
        ? encoded : Hash.sha3(encoded);
    // Path-state addresses nodes by trie path, so persist embedded nodes as well as hashed nodes.
    // The parent reference still follows canonical MPT embedding rules.
    sink.put(path, encoded);
    emittedNodes++;
  }

  private static byte[] nodeReference(byte[] value) {
    return value.length < 32 ? Arrays.copyOf(value, value.length) : rlpItem(value);
  }

  private static byte[] compactPath(byte[] nibbles, boolean leaf) {
    int odd = nibbles.length & 1;
    int flags = (leaf ? 2 : 0) + odd;
    byte[] encoded = new byte[1 + nibbles.length / 2];
    int source = 0;
    if (odd == 1) {
      encoded[0] = (byte) ((flags << 4) | nibbles[source++]);
    } else {
      encoded[0] = (byte) (flags << 4);
    }
    int target = 1;
    while (source < nibbles.length) {
      encoded[target++] = (byte) ((nibbles[source++] << 4) | nibbles[source++]);
    }
    return encoded;
  }

  private static byte[] rlpItem(byte[] raw) {
    byte[] value = Objects.requireNonNull(raw, "raw");
    if (value.length == 1 && (value[0] & 0xff) < 0x80) {
      return Arrays.copyOf(value, value.length);
    }
    byte[] prefix = rlpLength(value.length, 0x80, 0xb7);
    return concatenate(prefix, value);
  }

  private static byte[] rlpList(byte[]... items) {
    int length = 0;
    for (byte[] item : items) {
      length = Math.addExact(length, item.length);
    }
    byte[] prefix = rlpLength(length, 0xc0, 0xf7);
    ByteArrayOutputStream output = new ByteArrayOutputStream(prefix.length + length);
    output.write(prefix, 0, prefix.length);
    for (byte[] item : items) {
      output.write(item, 0, item.length);
    }
    return output.toByteArray();
  }

  private static byte[] rlpLength(int length, int shortOffset, int longOffset) {
    if (length <= 55) {
      return new byte[]{(byte) (shortOffset + length)};
    }
    int bytes = Integer.BYTES - Integer.numberOfLeadingZeros(length) / Byte.SIZE;
    byte[] encoded = new byte[bytes + 1];
    encoded[0] = (byte) (longOffset + bytes);
    for (int index = bytes; index > 0; index--) {
      encoded[index] = (byte) length;
      length >>>= Byte.SIZE;
    }
    return encoded;
  }

  private static int retainedNodes(Node node) {
    if (node == null) {
      return 0;
    }
    int count = 1;
    if (node.children != null) {
      for (Node child : node.children) {
        count += retainedNodes(child);
      }
    }
    return count;
  }

  private static int commonPrefix(byte[] left, byte[] right) {
    int length = Math.min(left.length, right.length);
    int index = 0;
    while (index < length && left[index] == right[index]) {
      index++;
    }
    return index;
  }

  private static byte[] toNibbles(byte[] key) {
    byte[] nibbles = new byte[key.length * 2];
    for (int index = 0; index < key.length; index++) {
      nibbles[index * 2] = (byte) ((key[index] >>> 4) & 0x0f);
      nibbles[index * 2 + 1] = (byte) (key[index] & 0x0f);
    }
    return nibbles;
  }

  private static byte[] append(byte[] value, byte suffix) {
    byte[] result = Arrays.copyOf(value, value.length + 1);
    result[value.length] = suffix;
    return result;
  }

  private static byte[] concatenate(byte[] left, byte[] right) {
    byte[] result = Arrays.copyOf(left, left.length + right.length);
    System.arraycopy(right, 0, result, left.length, right.length);
    return result;
  }

  private static byte[] slice(byte[] value, int from) {
    return slice(value, from, value.length);
  }

  private static byte[] slice(byte[] value, int from, int to) {
    return Arrays.copyOfRange(value, from, to);
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    for (int index = 0; index < Math.min(left.length, right.length); index++) {
      int compared = Integer.compare(left[index] & 0xff, right[index] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static byte[] secureKey(byte[] value) {
    byte[] key = Arrays.copyOf(Objects.requireNonNull(value, "secureKey"), value.length);
    if (key.length != PathMerkleTrie.SECURE_KEY_LENGTH) {
      throw new IllegalArgumentException("secureKey must contain exactly 32 bytes");
    }
    return key;
  }

  private static byte[] nonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  @FunctionalInterface
  interface NodeSink {

    void put(byte[] path, byte[] encodedNode);
  }

  private static final class Node {

    private int type;
    private byte[] key;
    private byte[] value;
    private Node[] children;

    private static Node leaf(byte[] key, byte[] value) {
      Node node = new Node();
      node.becomeLeaf(key, value);
      return node;
    }

    private static Node extension(byte[] key, Node child) {
      Node node = new Node();
      node.type = EXTENSION;
      node.key = key;
      node.children = new Node[]{child};
      return node;
    }

    private void becomeLeaf(byte[] nextKey, byte[] nextValue) {
      type = LEAF;
      key = Arrays.copyOf(nextKey, nextKey.length);
      value = Arrays.copyOf(nextValue, nextValue.length);
      children = null;
    }

    private void copyFrom(Node source) {
      type = source.type;
      key = source.key;
      value = source.value;
      children = source.children;
    }
  }
}
