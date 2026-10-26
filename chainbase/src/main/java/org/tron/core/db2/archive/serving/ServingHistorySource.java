package org.tron.core.db2.archive.serving;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.format.V5HistoryTail;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/**
 * Read-only committed History view consumed by serving (ch04 §3.3.1 rule 4; the chapter's
 * {@code HistoryCommittedView}). History publishes an immutable {@link Committed} after every
 * READABLE publication; serving reads it with a volatile read and never takes a History lock
 * (L3). Every read is bounded by the passed boundary, so {@code I <= W} holds by construction
 * (S-INV-1).
 */
public interface ServingHistorySource {

  /** The latest published boundary; before the first Common publication {@code W = B0 - 1}. */
  Committed committed();

  /** Five-lane bundle bytes of each block in {@code [first, last]} from blocks.idx only. */
  long[] bundleBytes(Committed view, long first, long last) throws IOException;

  /** Decodes {@code [first, last]} with canonical metadata and frame-digest step digests. */
  List<CommittedBlock> read(Committed view, long first, long last) throws IOException;

  /** Committed block hash at {@code block}; {@code B0 - 1} answers the baseline parent hash. */
  byte[] blockHash(Committed view, long block) throws IOException;

  /** An immutable published History boundary. */
  final class Committed {
    private final long historyStartBlock;
    private final byte[] baselineParentHash;
    private final V5HistoryTail tail;
    private final CommonCheckpointTarget target;

    public Committed(long historyStartBlock, byte[] baselineParentHash, V5HistoryTail tail,
        CommonCheckpointTarget target) {
      this.historyStartBlock = historyStartBlock;
      this.baselineParentHash = Arrays.copyOf(baselineParentHash, baselineParentHash.length);
      this.tail = tail;
      this.target = target;
    }

    public long getHistoryStartBlock() {
      return historyStartBlock;
    }

    public byte[] getBaselineParentHash() {
      return Arrays.copyOf(baselineParentHash, baselineParentHash.length);
    }

    /** {@code W}. */
    public long getCommittedThrough() {
      return target == null ? historyStartBlock - 1 : target.getLastBlock().getBlockNumber();
    }

    /** {@code hash(W)}. */
    public byte[] getCommittedHash() {
      return target == null ? getBaselineParentHash() : target.getLastBlock().getBlockHash();
    }

    public V5HistoryTail getTail() {
      return tail;
    }

    public CommonCheckpointTarget getTarget() {
      return target;
    }
  }

  /** One committed block, decoded, with its constant-size step digest. */
  final class CommittedBlock {
    private final BlockReverseDiff diff;
    private final byte[] stepDigest;
    private final long entries;
    private final long planInputBytes;

    public CommittedBlock(BlockReverseDiff diff, byte[] stepDigest) {
      this.diff = Objects.requireNonNull(diff, "diff");
      this.stepDigest = Arrays.copyOf(stepDigest, stepDigest.length);
      long count = 0;
      long bytes = 0;
      for (BlockReverseDiff.DbGroup group : diff.getGroups()) {
        for (BlockReverseDiff.Entry entry : group.getEntries()) {
          count++;
          bytes += entry.getEstimatedIndexInputBytes();
        }
      }
      this.entries = count;
      this.planInputBytes = bytes;
    }

    public BlockReverseDiff getDiff() {
      return diff;
    }

    public byte[] getStepDigest() {
      return Arrays.copyOf(stepDigest, stepDigest.length);
    }

    public long getEntries() {
      return entries;
    }

    public long getPlanInputBytes() {
      return planInputBytes;
    }
  }
}
