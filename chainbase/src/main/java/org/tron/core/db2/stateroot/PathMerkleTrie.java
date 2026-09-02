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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.tron.common.crypto.Hash;

/** Backend-neutral secure-key MPT with path-local immutable node updates. */
public final class PathMerkleTrie {

  public static final int SECURE_KEY_LENGTH = 32;

  private static final byte[] EMPTY_PATH = new byte[0];
  private static final byte[] EMPTY_RLP_ITEM = new byte[]{(byte) 0x80};
  private static final int PARALLEL_UPDATE_THRESHOLD = 4;
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
  private final AtomicLong nodeDecodeCount = new AtomicLong();
  private final AtomicLong nodeHashVerifyCount = new AtomicLong();

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
    rootNode = update(rootNode, toNibbles(key.bytes), 0, EMPTY_PATH, value);
    dirty = true;
  }

  public synchronized void delete(byte[] secureKey) {
    requireMutable();
    BytesKey key = secureKey(secureKey);
    if (leafValue(key) != null) {
      leaves.put(key, null);
      leafCount--;
      rootNode = update(rootNode, toNibbles(key.bytes), 0, EMPTY_PATH, null);
      dirty = true;
    }
  }

  synchronized void applyBatch(List<BatchMutation> mutations, ExecutorService executor) {
    requireMutable();
    List<BatchMutation> batch = new ArrayList<>(Objects.requireNonNull(mutations, "mutations"));
    Node resolvedRoot = resolve(rootNode, EMPTY_PATH);
    rootNode = resolvedRoot;
    if (batch.size() < PARALLEL_UPDATE_THRESHOLD || !(resolvedRoot instanceof BranchNode)) {
      applySequential(batch);
      return;
    }
    BranchNode root = (BranchNode) resolvedRoot;
    List<List<BatchMutation>> groups = new ArrayList<>(16);
    boolean[] deletes = new boolean[16];
    for (int i = 0; i < 16; i++) {
      groups.add(new ArrayList<>());
    }
    for (BatchMutation mutation : batch) {
      BatchMutation present = Objects.requireNonNull(mutation, "mutation");
      int nibble = (present.secureKey[0] >>> 4) & 0x0f;
      groups.get(nibble).add(present);
      deletes[nibble] |= present.encodedValue == null;
    }
    int guaranteedSurvivors = 0;
    for (int i = 0; i < root.children.length; i++) {
      if (root.children[i] != null && !deletes[i]) {
        guaranteedSurvivors++;
      }
    }
    if (guaranteedSurvivors < 2) {
      applySequential(batch);
      return;
    }
    List<Future<SubtreeResult>> futures = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();
    for (int i = 0; i < groups.size(); i++) {
      if (!groups.get(i).isEmpty()) {
        final int position = i;
        positions.add(position);
        futures.add(Objects.requireNonNull(executor, "executor").submit(
            () -> applySubtree(root.children[position], position, groups.get(position))));
      }
    }
    Node[] children = Arrays.copyOf(root.children, root.children.length);
    Map<BytesKey, byte[]> previous = new LinkedHashMap<>();
    Map<BytesKey, byte[]> changed = new LinkedHashMap<>();
    try {
      for (int i = 0; i < futures.size(); i++) {
        SubtreeResult result = futures.get(i).get();
        children[positions.get(i)] = result.node;
        previous.putAll(result.previous);
        changed.putAll(result.changed);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      cancel(futures);
      throw new IllegalStateException("path trie batch update interrupted", interrupted);
    } catch (ExecutionException failed) {
      cancel(futures);
      Throwable cause = failed.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException("path trie batch update failed", cause);
    }
    if (changed.isEmpty()) {
      return;
    }
    for (BatchMutation mutation : batch) {
      BytesKey key = new BytesKey(mutation.secureKey);
      if (!changed.containsKey(key)) {
        continue;
      }
      byte[] oldValue = previous.get(key);
      leaves.put(key, mutation.encodedValue);
      if (oldValue == null && mutation.encodedValue != null) {
        leafCount++;
      } else if (oldValue != null && mutation.encodedValue == null) {
        leafCount--;
      }
    }
    rootNode = new BranchNode(children);
    dirty = true;
  }

  private void applySequential(List<BatchMutation> mutations) {
    for (BatchMutation mutation : mutations) {
      if (mutation.encodedValue == null) {
        delete(mutation.secureKey);
      } else {
        put(mutation.secureKey, mutation.encodedValue);
      }
    }
  }

  private SubtreeResult applySubtree(Node initial, int nibble,
      List<BatchMutation> mutations) {
    Node node = initial;
    byte[] path = new byte[]{(byte) nibble};
    Map<BytesKey, byte[]> previous = new LinkedHashMap<>();
    Map<BytesKey, byte[]> changed = new LinkedHashMap<>();
    for (BatchMutation mutation : mutations) {
      BytesKey key = new BytesKey(mutation.secureKey);
      byte[] oldValue;
      if (leaves.containsKey(key)) {
        oldValue = leaves.get(key);
      } else if (inheritedSnapshot != null && inheritedSnapshot.containsLeaf(key)) {
        oldValue = inheritedSnapshot.leafValue(key);
      } else {
        byte[] nibbles = toNibbles(key.bytes);
        ValueResult result = resolvedValueAt(node, nibbles, 1, path);
        node = result.node;
        oldValue = result.value;
      }
      previous.put(key, oldValue);
      if (Arrays.equals(oldValue, mutation.encodedValue)) {
        continue;
      }
      node = update(node, toNibbles(key.bytes), 1, path, mutation.encodedValue);
      changed.put(key, mutation.encodedValue);
    }
    return new SubtreeResult(node, previous, changed);
  }

  private static void cancel(List<? extends Future<?>> futures) {
    for (Future<?> future : futures) {
      if (!future.isDone()) {
        future.cancel(true);
      }
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

  synchronized long getNodeDecodeCount() {
    return nodeDecodeCount.get();
  }

  synchronized long getNodeHashVerifyCount() {
    return nodeHashVerifyCount.get();
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

  /** Restores only the durable root node; descendants are decoded from path storage on demand. */
  synchronized void restoreRoot(byte[] expectedRoot) {
    if (!leaves.isEmpty() || inheritedSnapshot != null || rootNode != null
        || materializedRoot != null || dirty) {
      throw new IllegalStateException("path trie is not empty before root restoration");
    }
    byte[] expected = Arrays.copyOf(Objects.requireNonNull(expectedRoot, "expectedRoot"),
        expectedRoot.length);
    if (expected.length != SECURE_KEY_LENGTH) {
      throw new IllegalArgumentException("expectedRoot must contain exactly 32 bytes");
    }
    if (Arrays.equals(expected, Hash.EMPTY_TRIE_HASH)) {
      rootHash = expected;
      return;
    }
    byte[] encoded = nodeStore.get(EMPTY_PATH);
    nodeHashVerifyCount.incrementAndGet();
    if (encoded == null || !Arrays.equals(Hash.sha3(encoded), expected)) {
      throw new IllegalStateException("durable path trie root is missing or corrupt");
    }
    installStoredRoot(encoded, expected);
  }

  synchronized byte[] restoreRoot() {
    byte[] encoded = nodeStore.get(EMPTY_PATH);
    if (encoded == null) {
      rootHash = Arrays.copyOf(Hash.EMPTY_TRIE_HASH, Hash.EMPTY_TRIE_HASH.length);
      return Arrays.copyOf(rootHash, rootHash.length);
    }
    nodeHashVerifyCount.incrementAndGet();
    byte[] expected = Hash.sha3(encoded);
    installStoredRoot(encoded, expected);
    return Arrays.copyOf(expected, expected.length);
  }

  private void installStoredRoot(byte[] encoded, byte[] expected) {
    rootNode = new StoredNode(encoded, EMPTY_PATH);
    materializedRoot = rootNode;
    rememberMaterialized(rootNode, EMPTY_PATH);
    rootHash = Arrays.copyOf(expected, expected.length);
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
    if (inheritedSnapshot != null && inheritedSnapshot.containsLeaf(key)) {
      return inheritedSnapshot.leafValue(key);
    }
    return valueAt(rootNode, toNibbles(key.bytes), 0, EMPTY_PATH);
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
    BytesKey path;
    synchronized (materializedNodes) {
      path = materializedNodes.get(node);
    }
    return path != null || inheritedSnapshot == null
        ? path : inheritedSnapshot.materializedPath(node);
  }

  private void rememberMaterialized(Node node, byte[] path) {
    synchronized (materializedNodes) {
      materializedNodes.put(node, new BytesKey(path));
    }
  }

  private Node retainResolvedReplacement(Node original, Node replacement, byte[] path) {
    BytesKey materialized = materializedPath(original);
    if (materialized != null) {
      if (!Arrays.equals(materialized.bytes, path)) {
        throw new IllegalStateException("resolved path trie node moved from its durable path");
      }
      rememberMaterialized(replacement, path);
    }
    return replacement;
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

  private Node update(Node node, byte[] key, int offset, byte[] path, byte[] value) {
    if (node == null) {
      return value == null ? null
          : new LeafNode(Arrays.copyOfRange(key, offset, key.length), value);
    }
    node = resolve(node, path);
    if (node instanceof LeafNode) {
      return updateLeaf((LeafNode) node, key, offset, value);
    }
    if (node instanceof ExtensionNode) {
      return updateExtension((ExtensionNode) node, key, offset, path, value);
    }
    BranchNode branch = (BranchNode) node;
    if (offset >= key.length) {
      throw new IllegalStateException("secure path ended inside a branch");
    }
    int nibble = key[offset];
    Node previous = branch.children[nibble];
    Node changed = update(previous, key, offset + 1,
        append(path, new byte[]{(byte) nibble}), value);
    if (previous == changed) {
      return branch;
    }
    Node[] children = Arrays.copyOf(branch.children, branch.children.length);
    children[nibble] = changed;
    return normalizeBranch(children, path);
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

  private Node updateExtension(ExtensionNode extension, byte[] key, int offset,
      byte[] path, byte[] value) {
    int shared = commonPrefix(extension.path, 0, key, offset);
    if (shared == extension.path.length) {
      Node changed = update(extension.child, key, offset + shared,
          append(path, extension.path), value);
      if (changed == extension.child) {
        return extension;
      }
      return normalizeExtension(extension.path, changed, path);
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

  private Node normalizeBranch(Node[] children, byte[] path) {
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
    Node child = resolve(children[only], append(path, new byte[]{(byte) only}));
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

  private Node normalizeExtension(byte[] extensionPath, Node child, byte[] parentPath) {
    if (child == null) {
      return null;
    }
    child = resolve(child, append(parentPath, extensionPath));
    if (child instanceof LeafNode) {
      LeafNode leaf = (LeafNode) child;
      return new LeafNode(append(extensionPath, leaf.path), leaf.value);
    }
    if (child instanceof ExtensionNode) {
      ExtensionNode extension = (ExtensionNode) child;
      return new ExtensionNode(append(extensionPath, extension.path), extension.child);
    }
    return new ExtensionNode(extensionPath, child);
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

  private byte[] valueAt(Node node, byte[] key, int offset, byte[] path) {
    ValueResult result = resolvedValueAt(node, key, offset, path);
    if (path.length == 0) {
      rootNode = result.node;
    }
    return result.value;
  }

  private ValueResult resolvedValueAt(Node node, byte[] key, int offset, byte[] path) {
    if (node == null) {
      return new ValueResult(null, null);
    }
    Node present = resolve(node, path);
    if (present instanceof LeafNode) {
      LeafNode leaf = (LeafNode) present;
      int remaining = key.length - offset;
      byte[] value = remaining == leaf.path.length
          && commonPrefix(leaf.path, 0, key, offset) == remaining
          ? Arrays.copyOf(leaf.value, leaf.value.length) : null;
      return new ValueResult(value, present);
    }
    if (present instanceof ExtensionNode) {
      ExtensionNode extension = (ExtensionNode) present;
      int shared = commonPrefix(extension.path, 0, key, offset);
      if (shared != extension.path.length) {
        return new ValueResult(null, present);
      }
      ValueResult child = resolvedValueAt(extension.child, key, offset + shared,
          append(path, extension.path));
      if (child.node == extension.child) {
        return new ValueResult(child.value, present);
      }
      Node replacement = retainResolvedReplacement(present,
          new ExtensionNode(extension.path, child.node), path);
      return new ValueResult(child.value, replacement);
    }
    BranchNode branch = (BranchNode) present;
    if (offset >= key.length) {
      return new ValueResult(null, present);
    }
    int nibble = key[offset];
    ValueResult child = resolvedValueAt(branch.children[nibble], key, offset + 1,
        append(path, new byte[]{(byte) nibble}));
    if (child.node == branch.children[nibble]) {
      return new ValueResult(child.value, present);
    }
    Node[] children = Arrays.copyOf(branch.children, branch.children.length);
    children[nibble] = child.node;
    Node replacement = retainResolvedReplacement(present, new BranchNode(children), path);
    return new ValueResult(child.value, replacement);
  }

  private Node resolve(Node node, byte[] expectedPath) {
    if (!(node instanceof StoredNode)) {
      return node;
    }
    StoredNode stored = (StoredNode) node;
    nodeDecodeCount.incrementAndGet();
    byte[] storedEncoding = node.encoded;
    if (!Arrays.equals(stored.path, expectedPath)) {
      throw new IllegalStateException("stored path trie node moved from its durable path");
    }
    List<RlpElement> elements = decodeList(storedEncoding);
    Node decoded;
    if (elements.size() == 17) {
      if (!elements.get(16).isEmptyString()) {
        throw new IllegalStateException("path-state branch contains a value slot");
      }
      Node[] children = new Node[16];
      for (int index = 0; index < children.length; index++) {
        byte[] childPath = append(expectedPath, new byte[]{(byte) index});
        children[index] = storedChild(elements.get(index), childPath);
      }
      decoded = new BranchNode(children);
    } else if (elements.size() == 2 && !elements.get(0).list) {
      Compact compact = decodeCompact(elements.get(0).payload);
      if (compact.leaf) {
        if (elements.get(1).list) {
          throw new IllegalStateException("path-state leaf value must be an RLP item");
        }
        decoded = new LeafNode(compact.path, elements.get(1).payload);
      } else {
        if (compact.path.length == 0) {
          throw new IllegalStateException("path-state extension path must not be empty");
        }
        decoded = new ExtensionNode(compact.path,
            requiredStoredChild(elements.get(1), append(expectedPath, compact.path)));
      }
    } else {
      throw new IllegalStateException("path-state durable node has invalid arity");
    }
    if (!Arrays.equals(decoded.encoded, storedEncoding)) {
      throw new IllegalStateException("path-state durable node is not canonically encoded");
    }
    return retainResolvedReplacement(stored, decoded, expectedPath);
  }

  private Node requiredStoredChild(RlpElement reference, byte[] path) {
    Node child = storedChild(reference, path);
    if (child == null) {
      throw new IllegalStateException("path-state extension has an empty child");
    }
    return child;
  }

  private Node storedChild(RlpElement reference, byte[] path) {
    if (reference.isEmptyString()) {
      return null;
    }
    if (reference.list) {
      Node stored = new StoredNode(reference.encoded, path);
      rememberMaterialized(stored, path);
      return stored;
    }
    if (reference.payload.length != SECURE_KEY_LENGTH) {
      throw new IllegalStateException("path-state child hash must contain exactly 32 bytes");
    }
    byte[] encoded = nodeStore.get(path);
    nodeHashVerifyCount.incrementAndGet();
    if (encoded == null || !Arrays.equals(Hash.sha3(encoded), reference.payload)) {
      throw new IllegalStateException("path-state durable child is missing or corrupt");
    }
    Node stored = new StoredNode(encoded, path);
    rememberMaterialized(stored, path);
    return stored;
  }

  private static List<RlpElement> decodeList(byte[] encoded) {
    RlpElement root = decodeElement(encoded, 0);
    if (!root.list || root.encoded.length != encoded.length) {
      throw new IllegalStateException("path-state durable node must be one RLP list");
    }
    List<RlpElement> elements = new ArrayList<>();
    int offset = 0;
    while (offset < root.payload.length) {
      RlpElement child = decodeElement(root.payload, offset);
      elements.add(child);
      offset += child.encoded.length;
    }
    return elements;
  }

  private static RlpElement decodeElement(byte[] encoded, int offset) {
    if (offset < 0 || offset >= encoded.length) {
      throw new IllegalStateException("path-state RLP offset is invalid");
    }
    int marker = encoded[offset] & 0xff;
    if (marker < 0x80) {
      return new RlpElement(false, new byte[]{encoded[offset]}, new byte[]{encoded[offset]});
    }
    boolean list = marker >= 0xc0;
    int shortBase = list ? 0xc0 : 0x80;
    int longBase = list ? 0xf7 : 0xb7;
    int payloadOffset;
    int payloadLength;
    if (marker <= longBase) {
      payloadOffset = offset + 1;
      payloadLength = marker - shortBase;
    } else {
      int lengthBytes = marker - longBase;
      if (lengthBytes > Integer.BYTES || offset + 1 + lengthBytes > encoded.length) {
        throw new IllegalStateException("path-state RLP length is invalid");
      }
      payloadOffset = offset + 1 + lengthBytes;
      payloadLength = 0;
      for (int index = offset + 1; index < payloadOffset; index++) {
        payloadLength = Math.addExact(Math.multiplyExact(payloadLength, 256),
            encoded[index] & 0xff);
      }
    }
    int end = Math.addExact(payloadOffset, payloadLength);
    if (end > encoded.length) {
      throw new IllegalStateException("path-state RLP payload exceeds its node");
    }
    return new RlpElement(list, Arrays.copyOfRange(encoded, offset, end),
        Arrays.copyOfRange(encoded, payloadOffset, end));
  }

  private static Compact decodeCompact(byte[] encoded) {
    if (encoded.length == 0) {
      throw new IllegalStateException("path-state compact path must not be empty");
    }
    int flags = (encoded[0] >>> 4) & 0x0f;
    if (flags > 3) {
      throw new IllegalStateException("path-state compact path has invalid flags");
    }
    boolean odd = (flags & 1) != 0;
    byte[] path = new byte[encoded.length * 2 - (odd ? 1 : 2)];
    int target = 0;
    if (odd) {
      path[target++] = (byte) (encoded[0] & 0x0f);
    } else if ((encoded[0] & 0x0f) != 0) {
      throw new IllegalStateException("path-state compact path has non-zero padding");
    }
    for (int index = 1; index < encoded.length; index++) {
      path[target++] = (byte) ((encoded[index] >>> 4) & 0x0f);
      path[target++] = (byte) (encoded[index] & 0x0f);
    }
    return new Compact((flags & 2) != 0, path);
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

  private static final class StoredNode extends Node {

    private final byte[] path;

    private StoredNode(byte[] encoded, byte[] path) {
      super(Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length));
      this.path = Arrays.copyOf(Objects.requireNonNull(path, "path"), path.length);
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

  private static final class ValueResult {

    private final byte[] value;
    private final Node node;

    private ValueResult(byte[] value, Node node) {
      this.value = value;
      this.node = node;
    }
  }

  static final class BatchMutation {

    private final byte[] secureKey;
    private final byte[] encodedValue;

    BatchMutation(byte[] secureKey, byte[] encodedValue) {
      this.secureKey = PathMerkleTrie.secureKey(secureKey).copy();
      this.encodedValue = encodedValue == null ? null
          : nonEmpty(encodedValue, "encodedValue");
    }
  }

  private static final class SubtreeResult {

    private final Node node;
    private final Map<BytesKey, byte[]> previous;
    private final Map<BytesKey, byte[]> changed;

    private SubtreeResult(Node node, Map<BytesKey, byte[]> previous,
        Map<BytesKey, byte[]> changed) {
      this.node = node;
      this.previous = previous;
      this.changed = changed;
    }
  }

  private static final class RlpElement {

    private final boolean list;
    private final byte[] encoded;
    private final byte[] payload;

    private RlpElement(boolean list, byte[] encoded, byte[] payload) {
      this.list = list;
      this.encoded = encoded;
      this.payload = payload;
    }

    private boolean isEmptyString() {
      return !list && payload.length == 0;
    }
  }

  private static final class Compact {

    private final boolean leaf;
    private final byte[] path;

    private Compact(boolean leaf, byte[] path) {
      this.leaf = leaf;
      this.path = path;
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

    private boolean containsLeaf(BytesKey key) {
      return leaves.containsKey(key) || parent != null && parent.containsLeaf(key);
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
