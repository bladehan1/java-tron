package org.tron.core.db2.stateroot;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.tron.core.db2.common.WrappedByteArray;

/**
 * Immutable, memory-only encoded-node layers above one published checkpoint.
 *
 * <p>Only the retained suffix is indexed here. A new checkpoint starts a new chain from its
 * durable baseline, so this view never keeps an old baseline or a resolved trie graph alive.
 * A null entry is an authoritative deletion, not permission to fall through to older bytes.
 */
final class PathStateNodeOverlay {

  private final PathStateNodeOverlay parent;
  private final Map<Integer, Map<WrappedByteArray, byte[]>> stores = new LinkedHashMap<>();

  PathStateNodeOverlay(PathStateNodeOverlay parent, PathStateSnapshotDelta delta,
      PathStateParticipantScope scope) {
    this.parent = parent;
    for (PathStateSnapshotDelta.StoreDelta store : delta.getStores()) {
      if (scope.require(store.getDbName()).getStoreId() != store.getStoreId()) {
        throw new IllegalArgumentException("rebase node overlay Store identity mismatch");
      }
      stores.put(store.getStoreId(), index(store.getNodeMutations()));
    }
    stores.put(0, index(delta.getSuperNodeMutations()));
  }

  private static Map<WrappedByteArray, byte[]> index(
      List<PathStateSnapshotDelta.Mutation> mutations) {
    Map<WrappedByteArray, byte[]> indexed = new LinkedHashMap<>();
    for (PathStateSnapshotDelta.Mutation mutation : mutations) {
      indexed.put(WrappedByteArray.of(mutation.getKey()),
          mutation.isDelete() ? null : mutation.getValue());
    }
    return indexed;
  }

  PathNodeStore view(int storeId, PathNodeStore baseline) {
    return new PathNodeStore() {
      @Override
      public byte[] get(byte[] path) {
        WrappedByteArray key = WrappedByteArray.of(path);
        for (PathStateNodeOverlay layer = PathStateNodeOverlay.this;
            layer != null; layer = layer.parent) {
          Map<WrappedByteArray, byte[]> nodes = layer.stores.get(storeId);
          if (nodes != null && nodes.containsKey(key)) {
            byte[] value = nodes.get(key);
            return value == null ? null : Arrays.copyOf(value, value.length);
          }
        }
        return baseline.get(path);
      }

      @Override
      public void put(byte[] path, byte[] encodedNode) {
        throw new UnsupportedOperationException("checkpoint suffix node view is read-only");
      }

      @Override
      public void delete(byte[] path) {
        throw new UnsupportedOperationException("checkpoint suffix node view is read-only");
      }
    };
  }
}
