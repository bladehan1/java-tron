package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.bouncycastle.util.encoders.Hex;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Single-owner bulk-to-live coordinator for the append-file v3 exact-27 serving index. */
public final class StateArchiveServingIndexBuildCoordinatorV3 implements AutoCloseable {

  static final String DIRECTORY = "serving-index-v3";
  private final int bulkStartBlocks;
  private final List<BlockReverseDiff> pending = new ArrayList<>();
  private final PersistentServingKeyIndexGeneration.MutableIndex index;
  private CommonCheckpointTarget committedHead;
  private byte[] latestSourceIdentity;
  private long indexedThrough = -1;
  private byte[] indexedHash;
  private long buildSequence;
  private long syncSession;
  private Mode mode = Mode.RECOVERING;
  private boolean closed;

  public StateArchiveServingIndexBuildCoordinatorV3(Path archiveRoot, Engine engine,
      int bulkStartBlocks) throws IOException {
    Objects.requireNonNull(archiveRoot, "archiveRoot");
    Objects.requireNonNull(engine, "engine");
    Path catalogRoot = archiveRoot.resolve(DIRECTORY);
    if (bulkStartBlocks <= 0) {
      throw new IllegalArgumentException("bulkStartBlocks must be positive");
    }
    this.bulkStartBlocks = bulkStartBlocks;
    if (Files.exists(catalogRoot.resolve("current"), LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Legacy serving generations require explicit migration; preserved");
    }
    index = new PersistentServingKeyIndexGeneration.MutableIndex(
        catalogRoot.resolve("single-v1"), engine);
    indexedThrough = index.indexedThrough();
    indexedHash = index.headHash();
    mode = Mode.BULK_CATCH_UP;
  }

  /** Accepts only a Common-published contiguous range; bulk flushes at the configured threshold. */
  public synchronized BuildProgress offerCommittedRange(List<BlockReverseDiff> diffs,
      CommonCheckpointTarget target) throws IOException {
    requireOpen();
    if (mode != Mode.BULK_CATCH_UP) {
      throw new IllegalStateException("Serving bulk input is closed after handoff");
    }
    admit(diffs, target);
    if (pending.size() >= bulkStartBlocks) {
      flushPending();
    }
    return progress();
  }

  synchronized void recoverCommittedRange(List<BlockReverseDiff> supplied,
      CommonCheckpointTarget publishedHead, boolean finalRange) throws IOException {
    requireOpen();
    if (mode != Mode.BULK_CATCH_UP) {
      throw new IllegalStateException("Serving recovery requires bulk mode");
    }
    List<BlockReverseDiff> diffs = new ArrayList<>(Objects.requireNonNull(supplied, "diffs"));
    if (diffs.isEmpty()) {
      if (finalRange) {
        CommonCheckpointTarget target = Objects.requireNonNull(publishedHead, "publishedHead");
        if (indexedThrough != target.getLastBlock().getBlockNumber()
            || !Arrays.equals(indexedHash, target.getLastBlock().getBlockHash())) {
          throw new IOException("Serving recovery zero-action boundary mismatch");
        }
        committedHead = target;
      }
      return;
    }
    BlockSnapshotMeta previous = pending.isEmpty() ? null
        : pending.get(pending.size() - 1).getMeta();
    if (previous == null && indexedThrough >= 0) {
      BlockSnapshotMeta first = diffs.get(0).getMeta();
      if (first.getBlockNumber() != indexedThrough + 1
          || !Arrays.equals(first.getParentHash(), indexedHash)) {
        throw new IOException("Serving recovery suffix does not extend durable I");
      }
    }
    for (BlockReverseDiff diff : diffs) {
      if (previous != null && (diff.getMeta().getBlockNumber()
          != previous.getBlockNumber() + 1
          || !Arrays.equals(diff.getMeta().getParentHash(), previous.getBlockHash()))) {
        throw new IOException("Serving recovery suffix has a gap");
      }
      previous = diff.getMeta();
    }
    pending.addAll(diffs);
    // Intermediate batches use the exact same final-block digest computed by the plan.
    latestSourceIdentity = null;
    if (finalRange) {
      CommonCheckpointTarget target = Objects.requireNonNull(publishedHead, "publishedHead");
      if (!previous.equals(target.getLastBlock())) {
        throw new IOException("Serving recovery suffix differs from published W");
      }
      committedHead = target;
      latestSourceIdentity = target.getPayloadDigest();
    }
    if (pending.size() >= bulkStartBlocks || finalRange) {
      flushPending();
    }
  }

  /** Drains through the exact Common boundary and returns a session-bound live handle. */
  public synchronized LiveServingIndexer completeInitialSync(CommonCheckpointTarget boundary)
      throws IOException {
    requireOpen();
    if (mode != Mode.BULK_CATCH_UP || committedHead == null
        || !committedHead.equals(Objects.requireNonNull(boundary, "boundary"))) {
      throw new IllegalArgumentException("Serving handoff boundary is not the committed head");
    }
    mode = Mode.HANDOFF_DRAINING;
    try {
      flushPending();
      if (indexedThrough != boundary.getLastBlock().getBlockNumber()
          || !Arrays.equals(indexedHash, boundary.getLastBlock().getBlockHash())) {
        throw new IOException("Serving handoff did not reach the exact Common boundary");
      }
      mode = Mode.LIVE_IMMEDIATE;
      syncSession++;
      return new LiveServingIndexer(syncSession, buildSequence,
          index.identity());
    } catch (IOException | RuntimeException failure) {
      mode = Mode.CATCH_UP_REQUIRED;
      throw failure;
    }
  }

  public synchronized BuildProgress status() {
    requireOpen();
    return progress();
  }

  synchronized void flushRecoveryBatch() throws IOException {
    requireOpen();
    flushPending();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      mode = Mode.CLOSED;
      index.close();
    }
  }

  private void admit(List<BlockReverseDiff> supplied, CommonCheckpointTarget target) {
    List<BlockReverseDiff> diffs = new ArrayList<>(Objects.requireNonNull(supplied, "diffs"));
    CommonCheckpointTarget admittedTarget = Objects.requireNonNull(target, "target");
    if (diffs.isEmpty() || diffs.contains(null)
        || !diffs.get(0).getMeta().equals(admittedTarget.getFirstBlock())
        || !diffs.get(diffs.size() - 1).getMeta().equals(admittedTarget.getLastBlock())) {
      throw new IllegalArgumentException("Serving committed range differs from Common target");
    }
    BlockSnapshotMeta expectedParent;
    if (!pending.isEmpty()) {
      expectedParent = pending.get(pending.size() - 1).getMeta();
    } else if (committedHead != null) {
      expectedParent = committedHead.getLastBlock();
    } else if (indexedThrough >= 0) {
      expectedParent = new BlockSnapshotMeta(indexedThrough, indexedThrough, indexedHash,
          new byte[32], 0);
    } else {
      expectedParent = null;
    }
    BlockSnapshotMeta previous = expectedParent;
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      if (previous != null && (meta.getBlockNumber() != previous.getBlockNumber() + 1
          || !Arrays.equals(meta.getParentHash(), previous.getBlockHash()))) {
        throw new IllegalArgumentException("Serving committed ranges are not contiguous");
      }
      previous = meta;
    }
    pending.addAll(diffs);
    committedHead = admittedTarget;
    latestSourceIdentity = admittedTarget.getPayloadDigest();
  }

  private void flushPending() throws IOException {
    if (pending.isEmpty()) {
      return;
    }
    long base = indexedThrough >= 0 ? indexedThrough
        : pending.get(0).getMeta().getBlockNumber() - 1;
    byte[] baseHash = indexedHash == null ? pending.get(0).getMeta().getParentHash() : indexedHash;
    long through = pending.get(pending.size() - 1).getMeta().getBlockNumber();
    try (ServingIndexTiming timing = new ServingIndexTiming(mode.name(), base, through)) {
      long planStarted = System.nanoTime();
      ServingIndexIncrementalPlan plan;
      try {
        plan = ServingIndexIncrementalPlan.planCommittedDiffs(base, baseHash, pending);
      } finally {
        ServingIndexTiming.record(ServingIndexTiming.Stage.PLAN, planStarted);
      }
      List<byte[]> steps = plan.getSourceStepDigests();
      byte[] sourceIdentity = latestSourceIdentity == null
          ? steps.get(steps.size() - 1) : latestSourceIdentity;
      String generationId = generationId(plan.getIndexedThrough(), plan.getHeadHash());
      index.append(generationId, plan, sourceIdentity, () -> { }, () -> { });
      indexedThrough = plan.getIndexedThrough();
      indexedHash = plan.getHeadHash();
      buildSequence++;
      pending.clear();
      timing.succeeded();
    } catch (IOException | RuntimeException failure) {
      mode = Mode.CATCH_UP_REQUIRED;
      throw failure;
    }
  }

  /** Coverage starts at the base preceding the first available reverse diff, not at zero. */
  public synchronized long getIndexedFrom() {
    return index.indexedFrom();
  }

  synchronized PersistentServingKeyIndexGeneration pinIndexed(long baseline) throws IOException {
    requireOpen();
    if (mode != Mode.LIVE_IMMEDIATE || baseline < index.indexedFrom()
        || baseline > indexedThrough) {
      throw new IOException("Serving query baseline is outside ready coverage");
    }
    return index.pin();
  }

  private String generationId(long blockNumber, byte[] hash) {
    return String.format("append-v3-%020d-%s-%08d", blockNumber,
        Hex.toHexString(Arrays.copyOf(hash, 6)), buildSequence + 1);
  }

  private BuildProgress progress() {
    long committed = committedHead == null ? indexedThrough
        : committedHead.getLastBlock().getBlockNumber();
    return new BuildProgress(mode, indexedThrough, committed, pending.size(), buildSequence,
        index.indexedFrom());
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Serving coordinator is closed");
    }
  }

  public enum Mode {
    RECOVERING,
    BULK_CATCH_UP,
    HANDOFF_DRAINING,
    LIVE_IMMEDIATE,
    CATCH_UP_REQUIRED,
    DEGRADED,
    CLOSED
  }

  public static final class BuildProgress {
    private final Mode mode;
    private final long indexedFrom;
    private final long indexedThrough;
    private final long committedThrough;
    private final int pendingBlocks;
    private final long buildSequence;

    BuildProgress(Mode mode, long indexedThrough, long committedThrough,
        int pendingBlocks, long buildSequence) {
      this(mode, indexedThrough, committedThrough, pendingBlocks, buildSequence, -1);
    }

    BuildProgress(Mode mode, long indexedThrough, long committedThrough,
        int pendingBlocks, long buildSequence, long indexedFrom) {
      this.mode = mode;
      this.indexedFrom = indexedFrom;
      this.indexedThrough = indexedThrough;
      this.committedThrough = committedThrough;
      this.pendingBlocks = pendingBlocks;
      this.buildSequence = buildSequence;
    }

    public Mode getMode() {
      return mode;
    }

    public long getIndexedFrom() {
      return indexedFrom;
    }

    public long getIndexedThrough() {
      return indexedThrough;
    }

    public long getCommittedThrough() {
      return committedThrough;
    }

    public int getPendingBlocks() {
      return pendingBlocks;
    }

    public long getBuildSequence() {
      return buildSequence;
    }

  }

  public final class LiveServingIndexer {
    private final long session;
    private long sequence;
    private String generation;
    private boolean valid = true;

    private LiveServingIndexer(long session, long sequence, String generation) {
      this.session = session;
      this.sequence = sequence;
      this.generation = generation;
    }

    /** Commits every block and its progress atomically in the same long-lived database. */
    public BuildProgress indexNow(List<BlockReverseDiff> diffs, CommonCheckpointTarget target)
        throws IOException {
      synchronized (StateArchiveServingIndexBuildCoordinatorV3.this) {
        requireOpen();
        if (!valid || mode != Mode.LIVE_IMMEDIATE || session != syncSession
            || sequence != buildSequence || !generation.equals(index.identity())) {
          throw new IllegalStateException("Serving live handle is stale");
        }
        List<BlockReverseDiff> admitted = new ArrayList<>(Objects.requireNonNull(diffs, "diffs"));
        int published = 0;
        boolean rangeAccepted = false;
        try {
          admit(admitted, target);
          rangeAccepted = true;
          pending.clear();
          for (BlockReverseDiff diff : admitted) {
            pending.add(diff);
            flushPending();
            published++;
          }
          sequence = buildSequence;
          generation = index.identity();
          return progress();
        } catch (IOException | RuntimeException failure) {
          if (rangeAccepted) {
            pending.clear();
            pending.addAll(admitted.subList(published, admitted.size()));
          }
          valid = false;
          mode = Mode.CATCH_UP_REQUIRED;
          throw failure;
        }
      }
    }
  }
}
