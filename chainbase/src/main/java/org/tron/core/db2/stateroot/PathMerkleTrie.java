package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.common.crypto.Hash;

/** Backend-neutral secure-key MPT with path-local immutable node updates. */
public final class PathMerkleTrie {

  public static final int SECURE_KEY_LENGTH = 32;

  private static final byte[] EMPTY_PATH = new byte[0];
  private static final byte[] EMPTY_RLP_ITEM = new byte[]{(byte) 0x80};
  private static final Comparator<BytesKey> UNSIGNED_KEY_COMPARATOR = (left, right) -> {
    int length = Math.min(left.bytes.length, right.bytes.length);
    for (int i = 0; i < length; i++) {
      int compared = Integer.compare(left.bytes[i] & 0xff, right.bytes[i] & 0xff);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(left.bytes.length, right.bytes.length);
  };

  private final PathNodeStore nodeStore;
  private final Map<BytesKey, byte[]> leaves = new TreeMap<>(UNSIGNED_KEY_COMPARATOR);
  private final IdentityHashMap<Node, BytesKey> materializedNodes = new IdentityHashMap<>();
  private Snapshot inheritedSnapshot;
  private Node rootNode;
  private Node materializedRoot;
  private byte[] rootHash = Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length);
  private int leafCount;
  private boolean dirty;
  private boolean frozen;
  private int lastNodePuts;
  private int lastNodeDeletes;

  public PathMerkleTrie(PathNodeStore nodeStore) {
    this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
  }

  public synchronized void put(byte[] secureKey, byte[] encodedValue) {
    requireMutable();
    BytesKey key = secureKey(secureKey);
    byte[] value = nonEmpty(encodedValue, "encodedValue");
    byte[] previous = leafValue(key);
    if (Arrays.equals(previous, value)) {
      return;
    }
    leaves.put(key, value);
    if (previous == null) {
      leafCount++;
    }
    rootNode = update(rootNode, toNibbles(key.bytes), 0, value);
    dirty = true;
  }

  public synchronized void delete(byte[] secureKey) {
    requireMutable();
    BytesKey key = secureKey(secureKey);
    if (leafValue(key) != null) {
      leaves.put(key, null);
      leafCount--;
      rootNode = update(rootNode, toNibbles(key.bytes), 0, null);
      dirty = true;
    }
  }

  public synchronized byte[] get(byte[] secureKey) {
    byte[] value = leafValue(secureKey(secureKey));
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  /** Reconciles only structurally changed paths and returns the canonical root hash. */
  public synchronized byte[] rootHash() {
    if (dirty) {
      commit();
    }
    return Arrays.copyOf(rootHash, rootHash.length);
  }

  public synchronized int size() {
    return leafCount;
  }

  synchronized int getLastNodePuts() {
    return lastNodePuts;
  }

  synchronized int getLastNodeDeletes() {
    return lastNodeDeletes;
  }

  synchronized List<LeafEntry> leafEntries() {
    Map<BytesKey, byte[]> effective = effectiveLeaves();
    List<LeafEntry> entries = new ArrayList<>(effective.size());
    for (Map.Entry<BytesKey, byte[]> entry : effective.entrySet()) {
      entries.add(new LeafEntry(entry.getKey().copy(), entry.getValue()));
    }
    return entries;
  }

  synchronized Snapshot snapshot() {
    rootHash();
    frozen = true;
    return new Snapshot(inheritedSnapshot, leaves, materializedNodes, rootNode, rootHash,
        leafCount);
  }

  static PathMerkleTrie fromSnapshot(PathNodeStore nodeStore, Snapshot snapshot) {
    Snapshot parent = Objects.requireNonNull(snapshot, "snapshot");
    PathMerkleTrie trie = new PathMerkleTrie(nodeStore);
    trie.inheritedSnapshot = parent;
    trie.rootNode = parent.rootNode;
    trie.materializedRoot = parent.rootNode;
    trie.rootHash = Arrays.copyOf(parent.rootHash, parent.rootHash.length);
    trie.leafCount = parent.leafCount;
    return trie;
  }

  /** Initializes an empty trie from canonical leaves and writes its complete path-node set. */
  synchronized void initializeLeaves(Collection<LeafEntry> entries) {
    importLeaves(entries, "initialized");
    rootNode = buildTree();
    dirty = true;
    rootHash();
  }

  /** Restores current leaves and verifies their complete path-node set without repairing it. */
  synchronized void restoreLeaves(Collection<LeafEntry> entries) {
    importLeaves(entries, "restored");
    rootNode = buildTree();
    Map<BytesKey, byte[]> expectedNodes = collectNodes(rootNode);
    for (Map.Entry<BytesKey, byte[]> entry : expectedNodes.entrySet()) {
      if (!Arrays.equals(nodeStore.get(entry.getKey().bytes), entry.getValue())) {
        throw new IllegalStateException("restored leaves do not match persisted path nodes");
      }
    }
    materializedRoot = rootNode;
    indexNodes(rootNode, EMPTY_PATH, materializedNodes);
    rootHash = hash(rootNode);
  }

  private void importLeaves(Collection<LeafEntry> entries, String operation) {
    if (!leaves.isEmpty() || inheritedSnapshot != null || rootNode != null
        || materializedRoot != null || dirty) {
      throw new IllegalStateException("path trie is not empty before leaf " + operation);
    }
    for (LeafEntry entry : Objects.requireNonNull(entries, "entries")) {
      LeafEntry present = Objects.requireNonNull(entry, "entry");
      if (leaves.put(secureKey(present.secureKey),
          nonEmpty(present.encodedValue, "encodedValue")) != null) {
        throw new IllegalArgumentException("duplicate " + operation + " path-state leaf");
      }
      leafCount++;
    }
  }

  /** Verifies every path owned by the current materialized node set without repairing it. */
  public synchronized void verifyNodeStore() {
    if (dirty) {
      throw new IllegalStateException("cannot verify a dirty path trie");
    }
    Map<BytesKey, byte[]> expectedNodes = collectNodes(rootNode);
    for (Map.Entry<BytesKey, byte[]> entry : expectedNodes.entrySet()) {
      if (!Arrays.equals(nodeStore.get(entry.getKey().bytes), entry.getValue())) {
        throw new IllegalStateException("missing or corrupt materialized path node");
      }
    }
    if (!Arrays.equals(rootHash, hash(rootNode))) {
      throw new IllegalStateException("materialized path root does not match current leaves");
    }
  }

  private void commit() {
    IdentityHashMap<Node, Boolean> retained = new IdentityHashMap<>();
    List<NodePath> additions = new ArrayList<>();
    collectAdditions(rootNode, EMPTY_PATH, retained, additions);
    List<NodePath> removals = new ArrayList<>();
    collectRemovals(materializedRoot, retained, removals);

    for (NodePath removal : removals) {
      nodeStore.delete(removal.path);
      materializedNodes.remove(removal.node);
    }
    for (NodePath addition : additions) {
      nodeStore.put(addition.path, addition.node.encoded);
      materializedNodes.put(addition.node, new BytesKey(addition.path));
    }
    lastNodeDeletes = removals.size();
    lastNodePuts = additions.size();
    materializedRoot = rootNode;
    rootHash = hash(rootNode);
    dirty = false;
  }

  private void collectAdditions(Node node, byte[] path,
      IdentityHashMap<Node, Boolean> retained, List<NodePath> additions) {
    if (node == null) {
      return;
    }
    BytesKey oldPath = materializedPath(node);
    if (oldPath != null) {
      if (!Arrays.equals(oldPath.bytes, path)) {
        throw new IllegalStateException("path-local update moved an unchanged subtree");
      }
      retained.put(node, Boolean.TRUE);
      return;
    }
    additions.add(new NodePath(node, path));
    visitChildren(node, path,
        (child, childPath) -> collectAdditions(child, childPath, retained, additions));
  }

  private void collectRemovals(Node node, IdentityHashMap<Node, Boolean> retained,
      List<NodePath> removals) {
    if (node == null || retained.containsKey(node)) {
      return;
    }
    BytesKey path = materializedPath(node);
    if (path == null) {
      throw new IllegalStateException("path-local update lost a materialized node identity");
    }
    removals.add(new NodePath(node, path.copy()));
    visitChildren(node, path.bytes,
        (child, ignored) -> collectRemovals(child, retained, removals));
  }

  private Node buildTree() {
    Map<BytesKey, byte[]> effective = effectiveLeaves();
    if (effective.isEmpty()) {
      return null;
    }
    List<Leaf> entries = new ArrayList<>(effective.size());
    for (Map.Entry<BytesKey, byte[]> entry : effective.entrySet()) {
      entries.add(new Leaf(toNibbles(entry.getKey().bytes), entry.getValue()));
    }
    return build(entries, 0);
  }

  private byte[] leafValue(BytesKey key) {
    if (leaves.containsKey(key)) {
      return leaves.get(key);
    }
    return inheritedSnapshot == null ? null : inheritedSnapshot.leafValue(key);
  }

  private Map<BytesKey, byte[]> effectiveLeaves() {
    Map<BytesKey, byte[]> effective = new TreeMap<>(UNSIGNED_KEY_COMPARATOR);
    if (inheritedSnapshot != null) {
      inheritedSnapshot.populateLeaves(effective);
    }
    applyLeaves(effective, leaves);
    return effective;
  }

  private BytesKey materializedPath(Node node) {
    BytesKey path = materializedNodes.get(node);
    return path != null || inheritedSnapshot == null
        ? path : inheritedSnapshot.materializedPath(node);
  }

  private void requireMutable() {
    if (frozen) {
      throw new IllegalStateException("path trie is frozen as an immutable parent snapshot");
    }
  }

  private static void applyLeaves(Map<BytesKey, byte[]> target,
      Map<BytesKey, byte[]> changes) {
    for (Map.Entry<BytesKey, byte[]> entry : changes.entrySet()) {
      if (entry.getValue() == null) {
        target.remove(entry.getKey());
      } else {
        target.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private static Node build(List<Leaf> entries, int depth) {
    if (entries.size() == 1) {
      Leaf leaf = entries.get(0);
      return new LeafNode(Arrays.copyOfRange(leaf.nibbles, depth, leaf.nibbles.length),
          leaf.value);
    }
    int shared = sharedPrefix(entries, depth);
    if (shared > 0) {
      return new ExtensionNode(Arrays.copyOfRange(entries.get(0).nibbles, depth,
          depth + shared), build(entries, depth + shared));
    }
    Node[] children = new Node[16];
    int start = 0;
    while (start < entries.size()) {
      int nibble = entries.get(start).nibbles[depth];
      int end = start + 1;
      while (end < entries.size() && entries.get(end).nibbles[depth] == nibble) {
        end++;
      }
      children[nibble] = build(entries.subList(start, end), depth + 1);
      start = end;
    }
    return new BranchNode(children);
  }

  private static Node update(Node node, byte[] key, int offset, byte[] value) {
    if (node == null) {
      return value == null ? null
          : new LeafNode(Arrays.copyOfRange(key, offset, key.length), value);
    }
    if (node instanceof LeafNode) {
      return updateLeaf((LeafNode) node, key, offset, value);
    }
    if (node instanceof ExtensionNode) {
      return updateExtension((ExtensionNode) node, key, offset, value);
    }
    BranchNode branch = (BranchNode) node;
    if (offset >= key.length) {
      throw new IllegalStateException("secure path ended inside a branch");
    }
    int nibble = key[offset];
    Node previous = branch.children[nibble];
    Node changed = update(previous, key, offset + 1, value);
    if (previous == changed) {
      return branch;
    }
    Node[] children = Arrays.copyOf(branch.children, branch.children.length);
    children[nibble] = changed;
    return normalizeBranch(children);
  }

  private static Node updateLeaf(LeafNode leaf, byte[] key, int offset, byte[] value) {
    int remaining = key.length - offset;
    int shared = commonPrefix(leaf.path, 0, key, offset);
    if (shared == leaf.path.length && shared == remaining) {
      if (value == null) {
        return null;
      }
      return Arrays.equals(leaf.value, value) ? leaf : new LeafNode(leaf.path, value);
    }
    if (value == null) {
      return leaf;
    }
    if (shared >= leaf.path.length || shared >= remaining) {
      throw new IllegalStateException("fixed secure keys cannot prefix one another");
    }
    Node[] children = new Node[16];
    int oldNibble = leaf.path[shared];
    children[oldNibble] = new LeafNode(
        Arrays.copyOfRange(leaf.path, shared + 1, leaf.path.length), leaf.value);
    int newNibble = key[offset + shared];
    children[newNibble] = new LeafNode(
        Arrays.copyOfRange(key, offset + shared + 1, key.length), value);
    Node branch = new BranchNode(children);
    return shared == 0 ? branch
        : new ExtensionNode(Arrays.copyOf(leaf.path, shared), branch);
  }

  private static Node updateExtension(ExtensionNode extension, byte[] key, int offset,
      byte[] value) {
    int shared = commonPrefix(extension.path, 0, key, offset);
    if (shared == extension.path.length) {
      Node changed = update(extension.child, key, offset + shared, value);
      if (changed == extension.child) {
        return extension;
      }
      return normalizeExtension(extension.path, changed);
    }
    if (value == null) {
      return extension;
    }
    Node[] children = new Node[16];
    int oldNibble = extension.path[shared];
    byte[] oldSuffix = Arrays.copyOfRange(extension.path, shared + 1, extension.path.length);
    children[oldNibble] = oldSuffix.length == 0 ? extension.child
        : new ExtensionNode(oldSuffix, extension.child);
    int newNibble = key[offset + shared];
    children[newNibble] = new LeafNode(
        Arrays.copyOfRange(key, offset + shared + 1, key.length), value);
    Node branch = new BranchNode(children);
    return shared == 0 ? branch
        : new ExtensionNode(Arrays.copyOf(extension.path, shared), branch);
  }

  private static Node normalizeBranch(Node[] children) {
    int count = 0;
    int only = -1;
    for (int i = 0; i < children.length; i++) {
      if (children[i] != null) {
        count++;
        only = i;
      }
    }
    if (count == 0) {
      return null;
    }
    if (count > 1) {
      return new BranchNode(children);
    }
    Node child = children[only];
    byte[] prefix = new byte[]{(byte) only};
    if (child instanceof LeafNode) {
      LeafNode leaf = (LeafNode) child;
      return new LeafNode(append(prefix, leaf.path), leaf.value);
    }
    if (child instanceof ExtensionNode) {
      ExtensionNode extension = (ExtensionNode) child;
      return new ExtensionNode(append(prefix, extension.path), extension.child);
    }
    return new ExtensionNode(prefix, child);
  }

  private static Node normalizeExtension(byte[] path, Node child) {
    if (child == null) {
      return null;
    }
    if (child instanceof LeafNode) {
      LeafNode leaf = (LeafNode) child;
      return new LeafNode(append(path, leaf.path), leaf.value);
    }
    if (child instanceof ExtensionNode) {
      ExtensionNode extension = (ExtensionNode) child;
      return new ExtensionNode(append(path, extension.path), extension.child);
    }
    return new ExtensionNode(path, child);
  }

  private static Map<BytesKey, byte[]> collectNodes(Node root) {
    Map<BytesKey, byte[]> nodes = new LinkedHashMap<>();
    collectNodes(root, EMPTY_PATH, nodes);
    return nodes;
  }

  private static void collectNodes(Node node, byte[] path, Map<BytesKey, byte[]> nodes) {
    if (node == null) {
      return;
    }
    if (nodes.put(new BytesKey(path), node.encoded) != null) {
      throw new IllegalStateException("duplicate path-state node path");
    }
    visitChildren(node, path, (child, childPath) -> collectNodes(child, childPath, nodes));
  }

  private static void indexNodes(Node node, byte[] path,
      IdentityHashMap<Node, BytesKey> indexed) {
    if (node == null) {
      return;
    }
    indexed.put(node, new BytesKey(path));
    visitChildren(node, path, (child, childPath) -> indexNodes(child, childPath, indexed));
  }

  private static void visitChildren(Node node, byte[] path, NodeVisitor visitor) {
    if (node instanceof ExtensionNode) {
      ExtensionNode extension = (ExtensionNode) node;
      visitor.visit(extension.child, append(path, extension.path));
    } else if (node instanceof BranchNode) {
      BranchNode branch = (BranchNode) node;
      for (int i = 0; i < branch.children.length; i++) {
        if (branch.children[i] != null) {
          visitor.visit(branch.children[i], append(path, new byte[]{(byte) i}));
        }
      }
    }
  }

  private static byte[] hash(Node node) {
    return node == null ? Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length)
        : Hash.sha3(node.encoded);
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

  private static int commonPrefix(byte[] left, int leftOffset, byte[] right, int rightOffset) {
    int length = Math.min(left.length - leftOffset, right.length - rightOffset);
    int shared = 0;
    while (shared < length && left[leftOffset + shared] == right[rightOffset + shared]) {
      shared++;
    }
    return shared;
  }

  private static byte[] nodeReference(byte[] encodedNode) {
    return encodedNode.length < SECURE_KEY_LENGTH ? encodedNode : rlpItem(Hash.sha3(encodedNode));
  }

  private static byte[] compactPath(byte[] nibbles, boolean leaf) {
    int length = nibbles.length;
    boolean odd = (length & 1) != 0;
    byte[] compact = new byte[1 + length / 2];
    int flag = leaf ? 2 : 0;
    int source = 0;
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

  private static byte[] append(byte[] first, byte[] second) {
    byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
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
    return concatenate(rlpLength(raw.length, 0x80, 0xb7), raw);
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

  private abstract static class Node {

    private final byte[] encoded;

    private Node(byte[] encoded) {
      this.encoded = encoded;
    }
  }

  private static final class LeafNode extends Node {

    private final byte[] path;
    private final byte[] value;

    private LeafNode(byte[] path, byte[] value) {
      super(rlpList(rlpItem(compactPath(path, true)), rlpItem(value)));
      this.path = Arrays.copyOf(path, path.length);
      this.value = Arrays.copyOf(value, value.length);
    }
  }

  private static final class ExtensionNode extends Node {

    private final byte[] path;
    private final Node child;

    private ExtensionNode(byte[] path, Node child) {
      super(encode(path, child));
      if (path.length == 0) {
        throw new IllegalArgumentException("extension path must not be empty");
      }
      this.path = Arrays.copyOf(path, path.length);
      this.child = Objects.requireNonNull(child, "child");
    }

    private static byte[] encode(byte[] path, Node child) {
      Node present = Objects.requireNonNull(child, "child");
      return rlpList(rlpItem(compactPath(path, false)), nodeReference(present.encoded));
    }
  }

  private static final class BranchNode extends Node {

    private final Node[] children;

    private BranchNode(Node[] children) {
      super(encode(children));
      this.children = Arrays.copyOf(children, children.length);
    }

    private static byte[] encode(Node[] children) {
      if (children.length != 16) {
        throw new IllegalArgumentException("branch must contain 16 child slots");
      }
      List<byte[]> encodedChildren = new ArrayList<>(Collections.nCopies(17, EMPTY_RLP_ITEM));
      for (int i = 0; i < children.length; i++) {
        if (children[i] != null) {
          encodedChildren.set(i, nodeReference(children[i].encoded));
        }
      }
      return rlpList(encodedChildren.toArray(new byte[encodedChildren.size()][]));
    }
  }

  private static final class Leaf {

    private final byte[] nibbles;
    private final byte[] value;

    private Leaf(byte[] nibbles, byte[] value) {
      this.nibbles = nibbles;
      this.value = value;
    }
  }

  private static final class NodePath {

    private final Node node;
    private final byte[] path;

    private NodePath(Node node, byte[] path) {
      this.node = node;
      this.path = Arrays.copyOf(path, path.length);
    }
  }

  @FunctionalInterface
  private interface NodeVisitor {

    void visit(Node node, byte[] path);
  }

  static final class Snapshot {

    private final Snapshot parent;
    private final Map<BytesKey, byte[]> leaves;
    private final IdentityHashMap<Node, BytesKey> materializedNodes;
    private final Node rootNode;
    private final byte[] rootHash;
    private final int leafCount;

    private Snapshot(Snapshot parent, Map<BytesKey, byte[]> leaves,
        IdentityHashMap<Node, BytesKey> materializedNodes, Node rootNode, byte[] rootHash,
        int leafCount) {
      this.parent = parent;
      this.leaves = leaves;
      this.materializedNodes = materializedNodes;
      this.rootNode = rootNode;
      this.rootHash = Arrays.copyOf(rootHash, rootHash.length);
      this.leafCount = leafCount;
    }

    private byte[] leafValue(BytesKey key) {
      if (leaves.containsKey(key)) {
        return leaves.get(key);
      }
      return parent == null ? null : parent.leafValue(key);
    }

    private void populateLeaves(Map<BytesKey, byte[]> target) {
      if (parent != null) {
        parent.populateLeaves(target);
      }
      applyLeaves(target, leaves);
    }

    private BytesKey materializedPath(Node node) {
      BytesKey path = materializedNodes.get(node);
      return path != null || parent == null ? path : parent.materializedPath(node);
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
