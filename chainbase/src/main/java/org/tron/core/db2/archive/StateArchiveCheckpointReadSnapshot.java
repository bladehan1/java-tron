package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.core.CommonCheckpointRuntimeOwner;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Request-owned, point-only view whose keys independently use the current published head. */
public final class StateArchiveCheckpointReadSnapshot implements ArchivePointSnapshot {

  private final long targetBlock;
  private final long admissionBlock;
  private final byte[] admissionHash;
  private final HistoryFactory historyFactory;
  private final PinnedLatestStateFactory latestFactory;
  private boolean closed;

  private StateArchiveCheckpointReadSnapshot(long targetBlock,
      CheckpointPointHistory admission, HistoryFactory historyFactory,
      PinnedLatestStateFactory latestFactory) {
    this.targetBlock = targetBlock;
    this.admissionBlock = admission.getIndexedThrough();
    this.admissionHash = admission.getHeadHash();
    this.historyFactory = Objects.requireNonNull(historyFactory, "historyFactory");
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
  }

  /** Opens a lock-free accessor over the current published filesystem checkpoint. */
  public static StateArchiveCheckpointReadSnapshot pin(long targetBlock,
      CommonCheckpointRuntimeOwner owner, Path archiveDirectory, byte[] expectedFormatIdentity,
      PinnedLatestStateFactory latestFactory) throws IOException {
    Objects.requireNonNull(owner, "owner");
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    byte[] formatIdentity = Arrays.copyOf(
        Objects.requireNonNull(expectedFormatIdentity, "expectedFormatIdentity"),
        expectedFormatIdentity.length);
    return open(targetBlock,
        () -> StateArchiveCheckpointReadAdapter.open(directory, formatIdentity), latestFactory);
  }

  /** Opens a lock-free accessor over a fixed trusted checkpoint identity. */
  public static StateArchiveCheckpointReadSnapshot pin(long targetBlock,
      CommonCheckpointRuntimeOwner owner, Path archiveDirectory,
      CommonCheckpointTarget publishedTarget, Engine engine,
      PinnedLatestStateFactory latestFactory) throws IOException {
    Objects.requireNonNull(owner, "owner");
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    CommonCheckpointTarget target = Objects.requireNonNull(publishedTarget, "publishedTarget");
    Engine admittedEngine = Objects.requireNonNull(engine, "engine");
    return open(targetBlock,
        () -> StateArchiveCheckpointReadAdapter.openTrusted(directory, target, admittedEngine),
        latestFactory);
  }

  /** Opens append history independently for every key without a Common publication lease. */
  public static StateArchiveCheckpointReadSnapshot pinAppend(long targetBlock,
      CommonCheckpointRuntimeOwner owner, StateArchiveAppendFileRuntime materializer,
      CommonCheckpointTarget publishedTarget, PinnedLatestStateFactory latestFactory)
      throws IOException {
    Objects.requireNonNull(owner, "owner");
    StateArchiveAppendFileRuntime admittedMaterializer =
        Objects.requireNonNull(materializer, "materializer");
    CommonCheckpointTarget target = Objects.requireNonNull(publishedTarget, "publishedTarget");
    return open(targetBlock, () -> admittedMaterializer.pinHistory(target), latestFactory);
  }

  public static StateArchiveCheckpointReadSnapshot pinAppend(long targetBlock,
      StateArchiveAppendFileRuntime materializer,
      PublishedTargetSupplier targetSupplier, PinnedLatestStateFactory latestFactory)
      throws IOException {
    StateArchiveAppendFileRuntime admittedMaterializer =
        Objects.requireNonNull(materializer, "materializer");
    PublishedTargetSupplier admittedTargets = Objects.requireNonNull(targetSupplier,
        "targetSupplier");
    return open(targetBlock, () -> admittedMaterializer.pinHistory(
        requirePublishedTarget(admittedTargets.get())), latestFactory);
  }

  public static StateArchiveCheckpointReadSnapshot pin(long targetBlock, Path archiveDirectory,
      PublishedTargetSupplier targetSupplier, Engine engine,
      PinnedLatestStateFactory latestFactory) throws IOException {
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    PublishedTargetSupplier admittedTargets = Objects.requireNonNull(targetSupplier,
        "targetSupplier");
    Engine admittedEngine = Objects.requireNonNull(engine, "engine");
    return open(targetBlock, () -> StateArchiveCheckpointReadAdapter.openTrusted(directory,
        requirePublishedTarget(admittedTargets.get()), admittedEngine), latestFactory);
  }

  /** Returns this key's reverse-diff old value or a latest value pinned only for this access. */
  public synchronized OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
    ensureOpen();
    String admittedDbName = Objects.requireNonNull(dbName, "dbName");
    byte[] admittedKey = Objects.requireNonNull(physicalRawKey, "physicalRawKey");
    long accessBlock;
    byte[] accessHash;
    try (CheckpointPointHistory history = historyFactory.open()) {
      requireCoverage(history);
      Optional<OldValue> historical = history.findOldValueAfter(admittedDbName, admittedKey,
          targetBlock);
      if (historical.isPresent()) {
        return historical.get();
      }
      accessBlock = history.getIndexedThrough();
      accessHash = history.getHeadHash();
    }
    OldValue value = latestFactory.get(accessBlock, accessHash, admittedDbName, admittedKey);
    if (value == null) {
      throw new IllegalStateException("Latest state access returned null");
    }
    return value;
  }

  public long getTargetBlock() {
    return targetBlock;
  }

  public long getPinnedBlock() {
    return admissionBlock;
  }

  public byte[] getPinnedHash() {
    return Arrays.copyOf(admissionHash, admissionHash.length);
  }

  /** Revalidates current coverage without freezing it for the request lifetime. */
  public synchronized void requirePinnedIdentity() {
    ensureOpen();
    try (CheckpointPointHistory history = historyFactory.open()) {
      requireCoverage(history);
    } catch (IOException failure) {
      throw new IllegalStateException("checkpoint history access validation failed", failure);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
  }

  private void requireCoverage(CheckpointPointHistory history) {
    if (targetBlock < history.getIndexedFrom() || targetBlock > history.getIndexedThrough()) {
      throw new IllegalArgumentException("checkpoint target block is outside indexed coverage");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("checkpoint Archive read snapshot is closed");
    }
  }

  private static StateArchiveCheckpointReadSnapshot open(long targetBlock,
      HistoryFactory historyFactory, PinnedLatestStateFactory latestFactory) throws IOException {
    HistoryFactory admittedHistory = Objects.requireNonNull(historyFactory, "historyFactory");
    PinnedLatestStateFactory admittedLatest = Objects.requireNonNull(latestFactory,
        "latestFactory");
    try (CheckpointPointHistory admission = admittedHistory.open()) {
      StateArchiveCheckpointReadSnapshot snapshot = new StateArchiveCheckpointReadSnapshot(
          targetBlock, admission, admittedHistory, admittedLatest);
      snapshot.requireCoverage(admission);
      return snapshot;
    }
  }

  private static CommonCheckpointTarget requirePublishedTarget(CommonCheckpointTarget target)
      throws IOException {
    if (target == null) {
      throw new IOException("State Archive has no published common-checkpoint target");
    }
    return target;
  }

  @FunctionalInterface
  public interface PinnedLatestStateFactory {
    PinnedLatestState pin(long blockNumber, byte[] blockHash) throws IOException;

    default OldValue get(long blockNumber, byte[] blockHash, String dbName,
        byte[] physicalRawKey) throws IOException {
      try (PinnedLatestState latest = Objects.requireNonNull(
          pin(blockNumber, blockHash), "pinned latest state")) {
        if (latest.getBlockNumber() != blockNumber
            || !Arrays.equals(blockHash, latest.getBlockHash())) {
          throw new IllegalArgumentException("checkpoint latest access identity mismatch");
        }
        return latest.get(dbName, physicalRawKey);
      }
    }
  }

  @FunctionalInterface
  interface HistoryFactory {
    CheckpointPointHistory open() throws IOException;
  }

  @FunctionalInterface
  public interface PublishedTargetSupplier {
    CommonCheckpointTarget get();
  }
}
