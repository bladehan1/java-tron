package org.tron.core.db2.stateroot;

/**
 * Minimal path-addressed node boundary for the TASK-016 Merkle Patricia trie.
 *
 * <p>Paths contain one byte per nibble, each in the range {@code 0..15}; the empty path identifies
 * the root node. Implementations must copy mutable input and output arrays at their ownership
 * boundary.
 */
public interface PathNodeStore {

  byte[] get(byte[] path);

  void put(byte[] path, byte[] encodedNode);

  void delete(byte[] path);
}
