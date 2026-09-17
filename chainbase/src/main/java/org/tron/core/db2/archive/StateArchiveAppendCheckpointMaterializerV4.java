package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointMaterializer;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Common participant backed by the catalogless five-lane v4 append files. */
public final class StateArchiveAppendCheckpointMaterializerV4
    implements CommonCheckpointMaterializer, StateArchiveAppendFileRuntime {

  private final Path directory;
  private final byte[] commonFormatIdentity;
  private final Engine bindingEngine;
  private final short compressionId;
  private final StateArchiveFiveLaneBlockCodecV3 codec =
      new StateArchiveFiveLaneBlockCodecV3();
  private final StateArchiveCataloglessWriterV4 writer;
  private final StateArchiveServingWorkerV3 servingWorker;
  private CommonCheckpointTarget preparedTarget;
  private boolean closed;

  public StateArchiveAppendCheckpointMaterializerV4(Path directory,
      byte[] commonFormatIdentity, Engine bindingEngine, byte[] baselineHistoryDigest,
      short compressionId, long rotationTargetBytes) throws IOException {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.commonFormatIdentity = requireDigest(commonFormatIdentity, "Common format identity");
    this.bindingEngine = Objects.requireNonNull(bindingEngine, "bindingEngine");
    this.compressionId = compressionId;
    Optional<CommonCheckpointTarget> readable =
        StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
    StateArchiveServingIndexBuildCoordinatorV3 coordinator =
        new StateArchiveServingIndexBuildCoordinatorV3(directory, bindingEngine, 1_000);
    StateArchiveCataloglessWriterV4 openedWriter = null;
    StateArchiveServingWorkerV3 openedWorker = null;
    try {
      StateArchiveTailV4 tail = readable.isPresent()
          ? coordinator.archiveTail(requireTarget(readable.get())) : null;
      openedWriter = new StateArchiveCataloglessWriterV4(directory, baselineHistoryDigest,
          compressionId, rotationTargetBytes, readable.orElse(null), tail);
      StateArchiveServingIndexBuildCoordinatorV3 admittedCoordinator = coordinator;
      openedWorker = new StateArchiveServingWorkerV3(() -> admittedCoordinator,
          openedWriter, () -> { });
      this.writer = openedWriter;
      this.servingWorker = openedWorker;
    } catch (IOException | RuntimeException failure) {
      if (openedWorker != null) {
        try {
          openedWorker.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      } else {
        try {
          coordinator.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      if (openedWriter != null) {
        try {
          openedWriter.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  @Override
  public Authority authority() {
    return Authority.STATE_ARCHIVE;
  }

  @Override
  public synchronized StateArchiveHotBatchDescriptor planCheckpoint(
      List<BlockReverseDiff> diffs) throws IOException {
    requireOpen();
    List<BlockReverseDiff> admitted = admittedDiffs(diffs);
    BlockSnapshotMeta first = admitted.get(0).getMeta();
    long lastBlock = admitted.get(admitted.size() - 1).getMeta().getBlockNumber();
    BlockSnapshotMeta head = writer.getAppendHead();
    if (head != null && (head.getBlockNumber() < first.getBlockNumber() - 1
        || head.getBlockNumber() > lastBlock)) {
      throw new IOException("Archive V4 checkpoint is not the current successor or retry");
    }
    return StateArchiveHotStore.planCheckpointDescriptor(bindingEngine,
        first.getBlockNumber() - 1, first.getParentHash(),
        new byte[StateArchiveFileFormatV3.HASH_LENGTH], admitted);
  }

  @Override
  public synchronized void appendFinalized(List<BlockReverseDiff> diffs) throws IOException {
    requireOpen();
    if (!StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory).isPresent()) {
      return;
    }
    appendMissing(admittedDiffs(diffs), false);
  }

  @Override
  public synchronized CommonCheckpointTarget prepare(CommonCheckpointCapture capture)
      throws IOException {
    requireOpen();
    CommonCheckpointCapture admitted = Objects.requireNonNull(capture, "capture");
    CommonCheckpointTarget target = requireTarget(
        CommonCheckpointTarget.from(admitted.getPayload()));
    Status status = inspect(target);
    if (status != Status.NEEDS_MATERIALIZATION) {
      return target;
    }
    List<BlockReverseDiff> diffs = admittedDiffs(admitted.getArchiveDiffs());
    if (!admitted.getArchiveBinding().equals(planCheckpoint(diffs))) {
      throw new IOException("Archive V4 checkpoint binding differs");
    }
    appendMissing(diffs, true);
    StateArchiveTailV4 tail = writer.force(target);
    servingWorker.publishArchiveTail(tail);
    writer.publish(tail);
    preparedTarget = target;
    return target;
  }

  @Override
  public synchronized Status inspect(CommonCheckpointTarget target) throws IOException {
    requireOpen();
    CommonCheckpointTarget admitted = requireTarget(target);
    Optional<CommonCheckpointTarget> readable =
        StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
    if (readable.isPresent() && readable.get().equals(admitted)) {
      servingWorker.archiveTail(admitted);
      return Status.PUBLISHED;
    }
    if (readable.isPresent()) {
      requireParent(readable.get(), admitted);
    }
    return admitted.equals(preparedTarget) ? Status.MATERIALIZED
        : Status.NEEDS_MATERIALIZATION;
  }

  @Override
  public synchronized Optional<CommonCheckpointTarget> loadPublishedTargetIfPresent()
      throws IOException {
    requireOpen();
    Optional<CommonCheckpointTarget> target =
        StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
    if (target.isPresent()) {
      servingWorker.archiveTail(requireTarget(target.get()));
    }
    return target;
  }

  @Override
  public synchronized void materialize(CommonCheckpointPayload payload,
      CommonCheckpointTarget target) throws IOException {
    CommonCheckpointPayload admittedPayload = Objects.requireNonNull(payload, "payload");
    CommonCheckpointTarget admittedTarget = requireTarget(target);
    if (admittedPayload.getVersion() != CommonCheckpointPayload.COORDINATION_FORMAT_VERSION
        || !admittedTarget.equals(CommonCheckpointTarget.from(admittedPayload))
        || inspect(admittedTarget) == Status.NEEDS_MATERIALIZATION) {
      throw new IOException("Archive V4 requires its prepared coordination payload");
    }
  }

  @Override
  public synchronized void publish(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    Status status = inspect(admitted);
    if (status == Status.PUBLISHED) {
      return;
    }
    if (status != Status.MATERIALIZED) {
      throw new IOException("Archive V4 target is not materialized");
    }
    StateArchiveCheckpointMaterializer.publishReadableTarget(directory, admitted);
  }

  @Override
  public void afterCommit(CommonCheckpointTarget target) {
    try {
      servingWorker.offer(target);
    } catch (IOException | RuntimeException failure) {
      org.slf4j.LoggerFactory.getLogger("DB").error(
          "Archive V4 serving degraded after Common commit at {}",
          target.getLastBlock().getBlockNumber(), failure);
    }
  }

  @Override
  public synchronized void completeServingInitialSync(CommonCheckpointTarget boundary)
      throws IOException {
    requireOpen();
    servingWorker.completeInitialSync(boundary);
  }

  @Override
  public synchronized CheckpointPointHistory pinHistory(CommonCheckpointTarget target)
      throws IOException {
    requireOpen();
    CommonCheckpointTarget admitted = requireTarget(target);
    return new StateArchiveAppendReadAdapterV3(
        servingWorker.pinIndexed(admitted.getLastBlock().getBlockNumber()), writer, writer,
        admitted);
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      IOException failure = null;
      try {
        servingWorker.close();
      } catch (IOException closeFailure) {
        failure = closeFailure;
      }
      try {
        writer.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private void appendMissing(List<BlockReverseDiff> diffs, boolean allowRotation)
      throws IOException {
    int first = firstMissing(diffs, writer.getAppendHead());
    byte[] previous = writer.getResultHistoryDigest();
    for (int index = first; index < diffs.size(); index++) {
      EncodedBundle bundle = codec.encode(diffs.get(index), previous, compressionId);
      writer.append(bundle, allowRotation);
      previous = bundle.getResultHistoryDigest();
    }
  }

  private CommonCheckpointTarget requireTarget(CommonCheckpointTarget target)
      throws IOException {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    if (!Arrays.equals(commonFormatIdentity, admitted.getFormatIdentity())) {
      throw new IOException("Archive V4 Common format identity differs");
    }
    return admitted;
  }

  private static void requireParent(CommonCheckpointTarget parent,
      CommonCheckpointTarget target) throws IOException {
    if (!Arrays.equals(parent.getFormatIdentity(), target.getFormatIdentity())
        || parent.getLastBlock().getBlockNumber() + 1
            != target.getFirstBlock().getBlockNumber()
        || !Arrays.equals(parent.getLastBlock().getBlockHash(),
            target.getFirstBlock().getParentHash())
        || !Arrays.equals(parent.getStateRoot(), target.getParentStateRoot())) {
      throw new IOException("Archive V4 readable target is not the parent");
    }
  }

  private static List<BlockReverseDiff> admittedDiffs(List<BlockReverseDiff> diffs) {
    List<BlockReverseDiff> admitted = new ArrayList<>(Objects.requireNonNull(diffs, "diffs"));
    if (admitted.isEmpty() || admitted.contains(null)) {
      throw new IllegalArgumentException("Archive V4 checkpoint requires blocks");
    }
    BlockSnapshotMeta previous = null;
    for (BlockReverseDiff diff : admitted) {
      BlockSnapshotMeta current = diff.getMeta();
      if (current.getEpoch() != current.getBlockNumber()
          || previous != null && (current.getBlockNumber() != previous.getBlockNumber() + 1
          || !Arrays.equals(current.getParentHash(), previous.getBlockHash()))) {
        throw new IllegalArgumentException("Archive V4 block chain is not contiguous");
      }
      previous = current;
    }
    return admitted;
  }

  private static int firstMissing(List<BlockReverseDiff> diffs, BlockSnapshotMeta head)
      throws IOException {
    if (head == null || head.getBlockNumber() < diffs.get(0).getMeta().getBlockNumber()) {
      return 0;
    }
    for (int index = 0; index < diffs.size(); index++) {
      BlockSnapshotMeta meta = diffs.get(index).getMeta();
      if (meta.getBlockNumber() == head.getBlockNumber()) {
        if (!meta.equals(head)) {
          throw new IOException("Archive V4 retry prefix identity differs");
        }
        return index + 1;
      }
    }
    throw new IOException("Archive V4 retry head is outside checkpoint range");
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw new IOException("Archive V4 materializer is closed");
    }
  }

  private static byte[] requireDigest(byte[] value, String name) {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (admitted.length != StateArchiveFileFormatV3.HASH_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return admitted;
  }
}
