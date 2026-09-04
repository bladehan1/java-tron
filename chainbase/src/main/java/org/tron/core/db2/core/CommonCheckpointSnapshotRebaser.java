package org.tron.core.db2.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Removes an already-materialized Snapshot prefix without writing its mutations a second time. */
public final class CommonCheckpointSnapshotRebaser {

  /**
   * Reconnects every Store only after the complete target range has been validated everywhere.
   * The caller must hold the common-checkpoint write gate and SnapshotManager monitor.
   */
  public void rebase(List<Chainbase> databases, CommonCheckpointTarget target, int count)
      throws IOException {
    CommonCheckpointTarget admittedTarget = Objects.requireNonNull(target, "target");
    if (count <= 0) {
      throw new IllegalArgumentException("common checkpoint rebase count must be positive");
    }
    List<Plan> plans = new ArrayList<>();
    for (Chainbase database : Objects.requireNonNull(databases, "databases")) {
      plans.add(validate(Objects.requireNonNull(database, "database"), admittedTarget, count));
    }
    if (plans.isEmpty()) {
      throw new IOException("common checkpoint rebase requires registered Stores");
    }
    for (Plan plan : plans) {
      plan.apply();
    }
  }

  private static Plan validate(Chainbase database, CommonCheckpointTarget target, int count)
      throws IOException {
    Snapshot rootSnapshot = database.getHead().getRoot();
    if (!(rootSnapshot instanceof SnapshotRoot)) {
      throw new IOException("common checkpoint rebase Store has no SnapshotRoot: "
          + database.getDbName());
    }
    SnapshotRoot root = (SnapshotRoot) rootSnapshot;
    Snapshot next = root;
    BlockSnapshotMeta previous = null;
    BlockSnapshotMeta first = null;
    for (int index = 0; index < count; index++) {
      next = next.getNext();
      if (!(next instanceof SnapshotImpl)) {
        throw new IOException("common checkpoint rebase Store has too few layers: "
            + database.getDbName());
      }
      BlockSnapshotMeta meta = ((SnapshotImpl) next).getBlockSnapshotMeta();
      if (meta == null || previous != null && !isChild(previous, meta)) {
        throw new IOException("common checkpoint rebase Store block chain differs: "
            + database.getDbName());
      }
      if (first == null) {
        first = meta;
      }
      previous = meta;
    }
    if (!target.getFirstBlock().equals(first) || !target.getLastBlock().equals(previous)) {
      throw new IOException("common checkpoint rebase Store target differs: "
          + database.getDbName());
    }
    Snapshot successor = next.getNext();
    Snapshot head = database.getHead();
    if (head != next && successor == null) {
      throw new IOException("common checkpoint rebase Store chain is disconnected: "
          + database.getDbName());
    }
    return new Plan(database, root, next, successor, head == next);
  }

  private static boolean isChild(BlockSnapshotMeta parent, BlockSnapshotMeta child) {
    return child.getEpoch() == parent.getEpoch() + 1
        && child.getBlockNumber() == parent.getBlockNumber() + 1
        && Arrays.equals(child.getParentHash(), parent.getBlockHash());
  }

  private static final class Plan {

    private final Chainbase database;
    private final SnapshotRoot root;
    private final Snapshot last;
    private final Snapshot successor;
    private final boolean consumesHead;

    private Plan(Chainbase database, SnapshotRoot root, Snapshot last, Snapshot successor,
        boolean consumesHead) {
      this.database = database;
      this.root = root;
      this.last = last;
      this.successor = successor;
      this.consumesHead = consumesHead;
    }

    private void apply() {
      root.resetSolidity();
      if (consumesHead) {
        database.setHead(root);
        root.setNext(null);
      } else {
        successor.setPrevious(root);
        root.setNext(successor);
      }
      last.setNext(null);
    }
  }
}
