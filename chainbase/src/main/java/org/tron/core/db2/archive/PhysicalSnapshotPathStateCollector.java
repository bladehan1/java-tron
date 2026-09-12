package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.stateroot.PathStateBlockTransition;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateMutation;
import org.tron.core.db2.stateroot.PathStateTransitionCollector;

/** Consumes exact physical mutations after P66 materialization; no projection or scans. */
public final class PhysicalSnapshotPathStateCollector implements PathStateTransitionCollector {
  @Override
  public PathStateBlockTransition collect(BlockChangeView view) {
    // This is the PathState side of the same prepare barrier.  P66 has already been materialized
    // in the Snapshot, so this collector consumes exact physical mutations and records the old
    // physical value needed by PathState's transition/rebase logic.  It does not advance durable
    // CURRENT; the owner publishes that only after both prepare branches have joined.
    boolean enabled = SnapshotOldValueCollector.resolveTargetAssetOptimization(view);
    List<PathStateMutation> mutations = new ArrayList<>();
    for (BlockChangeView.DatabaseChanges database : view.getDatabases()) {
      for (BlockChangeView.Change change : database.getChanges()) {
        byte[] key = change.getKey();
        byte[] oldValue = database.getPrevious(key);
        byte[] value = change.getPostValue().isPresent()
            ? change.getPostValue().getValue() : null;
        if (!Arrays.equals(oldValue, value)) {
          mutations.add((value == null ? PathStateMutation.delete(database.getDbName(), key)
              : PathStateMutation.put(database.getDbName(), key, value))
              .withPreviousPhysicalValue(oldValue));
        }
      }
    }
    BlockSnapshotMeta meta = view.getMeta();
    return new PathStateBlockTransition(meta.getBlockNumber(), meta.getBlockHash(),
        meta.getParentHash(), meta.getTimestamp(), enabled ? P66Phase.P66_ON : P66Phase.P66_OFF,
        mutations);
  }
}
