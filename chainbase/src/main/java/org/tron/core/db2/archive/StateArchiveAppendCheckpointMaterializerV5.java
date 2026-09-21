package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.ExactWriteFaultHook;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.ReadObserver;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ChannelOpener;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ProcessFdAdmission;
import org.tron.core.db2.archive.StateArchiveFiveLaneWriterV5.ForceStage;
import org.tron.core.db2.archive.StateArchiveReaderGenerationHolderV5.PinnedReader;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Default-off fresh-root V5 participant through the tail/Common publication barriers. */
public final class StateArchiveAppendCheckpointMaterializerV5
    implements StateArchiveAppendFileRuntime {

  public static byte[] baselineHistoryDigest(long firstBlockNumber, byte[] parentHash) {
    return StateArchiveGethFormatV5.baselineHistoryDigest(firstBlockNumber, parentHash);
  }

  private final Path directory;
  private final byte[] commonFormatIdentity;
  private final Engine bindingEngine;
  private final long firstBlockNumber;
  private final StateArchiveFiveLaneWriterV5 writer;
  private final StateArchiveServingSourceV5 servingSource;
  private final StateArchiveServingWorkerV3 servingWorker;
  private final ProcessFdAdmission admission;
  private final ChannelOpener channelOpener;
  private final ReadObserver readObserver;
  private final PublicationObserver observer;
  private final ExactWriteFaultHook beforeTailSync;
  private StateArchiveReaderGenerationHolderV5 readerHolder;
  private CommonCheckpointTarget preparedTarget;
  private StateArchiveTailV5 preparedTail;
  private boolean closed;

  StateArchiveAppendCheckpointMaterializerV5(Path directory, byte[] commonFormatIdentity,
      Engine bindingEngine, long firstBlockNumber, byte[] baselineHistoryDigest,
      long rotationTargetBytes, ProcessFdAdmission admission) throws IOException {
    this(directory, commonFormatIdentity, bindingEngine, firstBlockNumber,
        baselineHistoryDigest, rotationTargetBytes, admission, missingCanonical(), null,
        (path, position, length) -> { }, (stage, laneId) -> { }, () -> { });
  }

  /** Opens the production V5 runtime with canonical Block Store and process-FD admission. */
  public StateArchiveAppendCheckpointMaterializerV5(Path directory,
      byte[] commonFormatIdentity, Engine bindingEngine, long firstBlockNumber,
      byte[] baselineHistoryDigest, long rotationTargetBytes,
      StateArchiveCanonicalBlockMetaSource canonical) throws IOException {
    this(directory, commonFormatIdentity, bindingEngine, firstBlockNumber,
        baselineHistoryDigest, rotationTargetBytes, StateArchiveProcessFdAdmissionV5.INSTANCE,
        canonical, null, (path, position, length) -> { }, (stage, laneId) -> { }, () -> { });
  }

  StateArchiveAppendCheckpointMaterializerV5(Path directory, byte[] commonFormatIdentity,
      Engine bindingEngine, long firstBlockNumber, byte[] baselineHistoryDigest,
      long rotationTargetBytes, ProcessFdAdmission admission, ChannelOpener channelOpener,
      ReadObserver readObserver, PublicationObserver observer,
      ExactWriteFaultHook beforeTailSync) throws IOException {
    this(directory, commonFormatIdentity, bindingEngine, firstBlockNumber,
        baselineHistoryDigest, rotationTargetBytes, admission, missingCanonical(), channelOpener,
        readObserver, observer, beforeTailSync);
  }

  StateArchiveAppendCheckpointMaterializerV5(Path directory, byte[] commonFormatIdentity,
      Engine bindingEngine, long firstBlockNumber, byte[] baselineHistoryDigest,
      long rotationTargetBytes, ProcessFdAdmission admission,
      StateArchiveCanonicalBlockMetaSource canonical, ChannelOpener channelOpener,
      ReadObserver readObserver, PublicationObserver observer,
      ExactWriteFaultHook beforeTailSync) throws IOException {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.commonFormatIdentity = requireDigest(commonFormatIdentity, "Common format identity");
    this.bindingEngine = Objects.requireNonNull(bindingEngine, "bindingEngine");
    if (firstBlockNumber < 0) {
      throw new IllegalArgumentException("Archive V5 first block is negative");
    }
    this.firstBlockNumber = firstBlockNumber;
    byte[] admittedBaseline = requireDigest(baselineHistoryDigest,
        "baseline history digest");
    this.admission = Objects.requireNonNull(admission, "admission");
    this.channelOpener = channelOpener;
    this.readObserver = Objects.requireNonNull(readObserver, "readObserver");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.beforeTailSync = Objects.requireNonNull(beforeTailSync, "beforeTailSync");
    StateArchiveServingIndexBuildCoordinatorV3 openedCoordinator = null;
    StateArchiveFiveLaneWriterV5 openedWriter = null;
    StateArchiveReaderGenerationV5 openedGeneration = null;
    StateArchiveServingWorkerV3 openedWorker = null;
    StateArchiveServingSourceV5 openedSource = new StateArchiveServingSourceV5(
        directory, firstBlockNumber, admittedBaseline,
        Objects.requireNonNull(canonical, "canonical"));
    try {
      requireAdmittedRoot(directory);
      openedCoordinator = new StateArchiveServingIndexBuildCoordinatorV3(
          directory, bindingEngine, 1_000);
      Optional<CommonCheckpointTarget> readable =
          StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
      if (readable.isPresent()) {
        CommonCheckpointTarget target = requireTarget(readable.get());
        StateArchiveTailV5 tail = openedCoordinator.archiveTailV5(target);
        if (tail.getFirstBlockNumber() != firstBlockNumber) {
          throw new IOException("Archive V5 configured first block differs from tail");
        }
        openedWriter = StateArchiveFiveLaneWriterV5.reopen(directory, tail, target,
            admittedBaseline, rotationTargetBytes, this.readObserver);
        openedGeneration = openGeneration(tail, target);
        openedSource.updatePublished(tail, target);
      } else {
        openedWriter = new StateArchiveFiveLaneWriterV5(directory, firstBlockNumber,
            admittedBaseline, rotationTargetBytes);
      }
      StateArchiveServingIndexBuildCoordinatorV3 admittedCoordinator = openedCoordinator;
      openedWorker = new StateArchiveServingWorkerV3(() -> admittedCoordinator,
          openedSource, () -> { });
    } catch (IOException | RuntimeException failure) {
      if (openedWorker != null) {
        try {
          openedWorker.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      if (openedGeneration != null) {
        try {
          openedGeneration.close();
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
      if (openedWorker == null && openedCoordinator != null) {
        try {
          openedCoordinator.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
    writer = openedWriter;
    servingSource = openedSource;
    servingWorker = openedWorker;
    if (openedGeneration != null) {
      readerHolder = new StateArchiveReaderGenerationHolderV5(openedGeneration);
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
    if ((head == null && first.getBlockNumber() != firstBlockNumber)
        || (head != null && (head.getBlockNumber() < first.getBlockNumber() - 1
        || head.getBlockNumber() > lastBlock))) {
      throw new IOException("Archive V5 checkpoint is not the current successor or retry");
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
    List<BlockReverseDiff> admitted = admittedDiffs(diffs);
    int firstMissing = firstMissing(admitted, writer.getAppendHead());
    for (int index = firstMissing; index < admitted.size(); index++) {
      writer.append(admitted.get(index));
    }
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
      throw new IOException("Archive V5 checkpoint binding differs");
    }
    if (preparedTarget != null && !preparedTarget.equals(target)) {
      throw new IOException("Archive V5 already has a different prepared target");
    }
    int firstMissing = firstMissing(diffs, writer.getAppendHead());
    for (int index = firstMissing; index < diffs.size(); index++) {
      writer.append(diffs.get(index));
    }
    StateArchiveTailV5 tail = writer.forceTailReady(target, (stage, laneId) -> {
      if (stage == ForceStage.DATA) {
        observer.completed(PublicationStage.DATA_FORCED, laneId);
      } else if (stage == ForceStage.LANE_INDEX) {
        observer.completed(PublicationStage.LANE_INDEX_FORCED, laneId);
      }
    });
    servingWorker.publishArchiveTailV5(tail, beforeTailSync);
    servingWorker.archiveTailV5(target);
    observer.completed(PublicationStage.TAIL_SYNCED, -1);
    preparedTarget = target;
    preparedTail = tail;
    return target;
  }

  @Override
  public synchronized Status inspect(CommonCheckpointTarget target) throws IOException {
    requireOpen();
    CommonCheckpointTarget admitted = requireTarget(target);
    Optional<CommonCheckpointTarget> readable =
        StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
    if (readable.isPresent() && readable.get().equals(admitted)) {
      servingWorker.archiveTailV5(admitted);
      return Status.PUBLISHED;
    }
    if (readable.isPresent()) {
      requireParent(readable.get(), admitted);
    }
    if (admitted.equals(preparedTarget)) {
      servingWorker.archiveTailV5(admitted);
      return Status.MATERIALIZED;
    }
    return Status.NEEDS_MATERIALIZATION;
  }

  @Override
  public synchronized void materialize(CommonCheckpointPayload payload,
      CommonCheckpointTarget target) throws IOException {
    CommonCheckpointPayload admittedPayload = Objects.requireNonNull(payload, "payload");
    CommonCheckpointTarget admittedTarget = requireTarget(target);
    if (admittedPayload.getVersion() != CommonCheckpointPayload.COORDINATION_FORMAT_VERSION
        || !admittedTarget.equals(CommonCheckpointTarget.from(admittedPayload))
        || inspect(admittedTarget) == Status.NEEDS_MATERIALIZATION) {
      throw new IOException("Archive V5 requires its prepared coordination payload");
    }
  }

  @Override
  public synchronized void publish(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    Status status = inspect(admitted);
    if (status == Status.PUBLISHED) {
      installReaderGeneration(admitted);
      installServingSource(admitted);
      return;
    }
    if (status != Status.MATERIALIZED) {
      throw new IOException("Archive V5 target is not materialized");
    }
    StateArchiveCheckpointMaterializer.publishReadableTarget(directory, admitted);
    installReaderGeneration(admitted);
    installServingSource(admitted);
    observer.completed(PublicationStage.COMMON_PUBLISHED, -1);
    preparedTarget = null;
    preparedTail = null;
  }

  @Override
  public synchronized Optional<CommonCheckpointTarget> loadPublishedTargetIfPresent()
      throws IOException {
    requireOpen();
    Optional<CommonCheckpointTarget> readable =
        StateArchiveCheckpointMaterializer.loadReadableTargetIfPresent(directory);
    if (readable.isPresent()) {
      CommonCheckpointTarget target = requireTarget(readable.get());
      StateArchiveTailV5 tail = servingWorker.archiveTailV5(target);
      servingSource.updatePublished(tail, target);
    }
    return readable;
  }

  @Override
  public void afterCommit(CommonCheckpointTarget target) {
    try {
      servingWorker.offer(target);
    } catch (IOException | RuntimeException failure) {
      org.slf4j.LoggerFactory.getLogger("DB").error(
          "Archive V5 serving degraded after Common commit at {}",
          target.getLastBlock().getBlockNumber(), failure);
    }
  }

  @Override
  public synchronized void completeServingInitialSync(CommonCheckpointTarget boundary)
      throws IOException {
    requireOpen();
    servingWorker.completeInitialSync(requireTarget(boundary));
  }

  synchronized StateArchiveServingIndexBuildCoordinatorV3.BuildProgress servingIndexStatus() {
    return servingWorker.status();
  }

  @Override
  public synchronized CheckpointPointHistory pinHistory(CommonCheckpointTarget target)
      throws IOException {
    requireOpen();
    CommonCheckpointTarget admitted = requireTarget(target);
    PinnedReader point = pinReader();
    try {
      PersistentServingKeyIndexGeneration index = servingWorker.pinIndexed(
          admitted.getLastBlock().getBlockNumber());
      try {
        return new OwnedPointHistory(new StateArchiveAppendReadAdapterV3(
            index, servingSource, point, admitted), point);
      } catch (IOException | RuntimeException failure) {
        point.close();
        throw failure;
      }
    } catch (IOException | RuntimeException failure) {
      try {
        point.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  synchronized PinnedReader pinReader() throws IOException {
    requireOpen();
    if (readerHolder == null) {
      throw new IOException("Archive V5 reader generation is not published");
    }
    return readerHolder.pin();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      servingWorker.close();
    } catch (IOException closeFailure) {
      failure = closeFailure;
    }
    if (readerHolder != null) {
      try {
        readerHolder.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
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

  private void installReaderGeneration(CommonCheckpointTarget target) throws IOException {
    long blockNumber = target.getLastBlock().getBlockNumber();
    if (readerHolder != null && readerHolder.getCommittedBlockNumber() == blockNumber) {
      return;
    }
    StateArchiveTailV5 tail = preparedTail == null
        ? servingWorker.archiveTailV5(target) : preparedTail;
    StateArchiveReaderGenerationV5 generation = openGeneration(tail, target);
    if (readerHolder == null) {
      readerHolder = new StateArchiveReaderGenerationHolderV5(generation);
    } else {
      readerHolder.publish(generation);
    }
  }

  private void installServingSource(CommonCheckpointTarget target) throws IOException {
    StateArchiveTailV5 tail = preparedTail == null
        ? servingWorker.archiveTailV5(target) : preparedTail;
    servingSource.updatePublished(tail, target);
  }

  private StateArchiveReaderGenerationV5 openGeneration(StateArchiveTailV5 tail,
      CommonCheckpointTarget target) throws IOException {
    return StateArchiveReaderGenerationV5.open(directory, tail, target,
        this::admitRuntimeHandles,
        channelOpener, readObserver);
  }

  private void admitRuntimeHandles(long requiredDataHandles, long reserveHandles)
      throws IOException {
    long total;
    try {
      total = StrictMathWrapper.addExact(requiredDataHandles,
          StateArchiveServingSourceV5.MAX_REPLAY_DATA_HANDLES);
    } catch (ArithmeticException failure) {
      throw new IOException("Archive V5 runtime data handle requirement overflow", failure);
    }
    StateArchiveDataHandlePoolV5.requireApplicationCap(total);
    admission.require(total, reserveHandles);
  }

  private CommonCheckpointTarget requireTarget(CommonCheckpointTarget target)
      throws IOException {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    if (!Arrays.equals(commonFormatIdentity, admitted.getFormatIdentity())) {
      throw new IOException("Archive V5 Common format identity differs");
    }
    return admitted;
  }

  private static List<BlockReverseDiff> admittedDiffs(List<BlockReverseDiff> supplied) {
    List<BlockReverseDiff> diffs = new ArrayList<>(Objects.requireNonNull(supplied, "diffs"));
    if (diffs.isEmpty() || diffs.contains(null)) {
      throw new IllegalArgumentException("Archive V5 checkpoint requires blocks");
    }
    BlockSnapshotMeta previous = null;
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta current = diff.getMeta();
      if (current.getEpoch() != current.getBlockNumber()
          || previous != null && (current.getBlockNumber() != previous.getBlockNumber() + 1
          || !Arrays.equals(current.getParentHash(), previous.getBlockHash()))) {
        throw new IllegalArgumentException("Archive V5 block chain is not contiguous");
      }
      previous = current;
    }
    return diffs;
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
          throw new IOException("Archive V5 retry prefix identity differs");
        }
        return index + 1;
      }
    }
    throw new IOException("Archive V5 retry head is outside checkpoint range");
  }

  private static void requireParent(CommonCheckpointTarget parent,
      CommonCheckpointTarget target) throws IOException {
    if (!Arrays.equals(parent.getFormatIdentity(), target.getFormatIdentity())
        || parent.getLastBlock().getBlockNumber() + 1
            != target.getFirstBlock().getBlockNumber()
        || !Arrays.equals(parent.getLastBlock().getBlockHash(),
            target.getFirstBlock().getParentHash())
        || !Arrays.equals(parent.getStateRoot(), target.getParentStateRoot())) {
      throw new IOException("Archive V5 readable target is not the parent");
    }
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw new IOException("Archive V5 materializer is closed");
    }
  }

  private static byte[] requireDigest(byte[] value, String name) {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (admitted.length != StateArchiveFileFormatV3.HASH_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return admitted;
  }

  private static StateArchiveCanonicalBlockMetaSource missingCanonical() {
    return blockNumber -> {
      throw new IOException("Archive V5 canonical metadata source is not configured");
    };
  }

  private static void requireAdmittedRoot(Path root) throws IOException {
    if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Archive V5 root is not a directory");
    }
    if (Files.exists(root.resolve(StateArchiveCheckpointMaterializer.READABLE_FILE),
        LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
      if (entries.iterator().hasNext()) {
        throw new IOException(
            "Archive V5 non-fresh root lacks a committed Common publication");
      }
    }
  }

  private static final class OwnedPointHistory implements CheckpointPointHistory {
    private final CheckpointPointHistory delegate;
    private final PinnedReader point;

    private OwnedPointHistory(CheckpointPointHistory delegate, PinnedReader point) {
      this.delegate = delegate;
      this.point = point;
    }

    @Override
    public Optional<OldValue> findOldValueAfter(String dbName, byte[] rawKey,
        long targetBlock) throws IOException {
      return delegate.findOldValueAfter(dbName, rawKey, targetBlock);
    }

    @Override
    public long getIndexedFrom() {
      return delegate.getIndexedFrom();
    }

    @Override
    public long getIndexedThrough() {
      return delegate.getIndexedThrough();
    }

    @Override
    public byte[] getHeadHash() {
      return delegate.getHeadHash();
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      try {
        delegate.close();
      } catch (IOException closeFailure) {
        failure = closeFailure;
      }
      try {
        point.close();
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

  enum PublicationStage {
    DATA_FORCED,
    LANE_INDEX_FORCED,
    TAIL_SYNCED,
    COMMON_PUBLISHED
  }

  interface PublicationObserver {
    void completed(PublicationStage stage, int laneId) throws IOException;
  }
}
