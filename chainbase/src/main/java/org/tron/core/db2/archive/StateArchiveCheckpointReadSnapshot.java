package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.core.CommonCheckpointRuntimeOwner;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Request-owned, point-only view over one published next-format checkpoint head. */
public final class StateArchiveCheckpointReadSnapshot implements ArchivePointSnapshot {

  private final long targetBlock;
  private final long pinnedBlock;
  private final byte[] pinnedHash;
  private final CommonCheckpointRuntimeOwner.ReadLease lease;
  private final StateArchiveCheckpointReadAdapter archive;
  private final PinnedLatestState latest;
  private boolean closed;

  private StateArchiveCheckpointReadSnapshot(long targetBlock,
      CommonCheckpointRuntimeOwner.ReadLease lease,
      StateArchiveCheckpointReadAdapter archive, PinnedLatestState latest) {
    this.targetBlock = targetBlock;
    this.pinnedBlock = archive.getIndexedThrough();
    this.pinnedHash = archive.getHeadHash();
    this.lease = lease;
    this.archive = archive;
    this.latest = latest;
    validateIdentity();
  }

  /** Pins the publication gate, Archive index, and latest engine head as one request unit. */
  public static StateArchiveCheckpointReadSnapshot pin(long targetBlock,
      CommonCheckpointRuntimeOwner owner, Path archiveDirectory, byte[] expectedFormatIdentity,
      PinnedLatestStateFactory latestFactory) throws IOException {
    CommonCheckpointRuntimeOwner admittedOwner = Objects.requireNonNull(owner, "owner");
    CommonCheckpointRuntimeOwner.ReadLease lease = admittedOwner.acquireReadLease();
    StateArchiveCheckpointReadAdapter archive = null;
    PinnedLatestState latest = null;
    try {
      archive = StateArchiveCheckpointReadAdapter.open(archiveDirectory,
          expectedFormatIdentity);
      if (targetBlock < archive.getIndexedFrom() || targetBlock > archive.getIndexedThrough()) {
        throw new IllegalArgumentException("checkpoint target block is outside indexed coverage");
      }
      latest = Objects.requireNonNull(latestFactory, "latestFactory").pin(
          archive.getIndexedThrough(), archive.getHeadHash());
      return new StateArchiveCheckpointReadSnapshot(targetBlock, lease, archive,
          Objects.requireNonNull(latest, "pinned latest state"));
    } catch (IOException | RuntimeException failure) {
      closeAfterFailedPin(lease, archive, latest, failure);
      throw failure;
    }
  }

  /** Pins a target already validated and bound by its owning common-checkpoint runtime. */
  public static StateArchiveCheckpointReadSnapshot pin(long targetBlock,
      CommonCheckpointRuntimeOwner owner, Path archiveDirectory,
      CommonCheckpointTarget publishedTarget, Engine engine,
      PinnedLatestStateFactory latestFactory) throws IOException {
    CommonCheckpointRuntimeOwner admittedOwner = Objects.requireNonNull(owner, "owner");
    CommonCheckpointRuntimeOwner.ReadLease lease = admittedOwner.acquireReadLease();
    StateArchiveCheckpointReadAdapter archive = null;
    PinnedLatestState latest = null;
    try {
      archive = StateArchiveCheckpointReadAdapter.openTrusted(archiveDirectory,
          publishedTarget, engine);
      if (targetBlock < archive.getIndexedFrom() || targetBlock > archive.getIndexedThrough()) {
        throw new IllegalArgumentException("checkpoint target block is outside indexed coverage");
      }
      latest = Objects.requireNonNull(latestFactory, "latestFactory").pin(
          archive.getIndexedThrough(), archive.getHeadHash());
      return new StateArchiveCheckpointReadSnapshot(targetBlock, lease, archive,
          Objects.requireNonNull(latest, "pinned latest state"));
    } catch (IOException | RuntimeException failure) {
      closeAfterFailedPin(lease, archive, latest, failure);
      throw failure;
    }
  }

  /** Returns the first reverse-diff old value, or the same-request pinned latest value. */
  public synchronized OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
    ensureOpen();
    Optional<OldValue> historical = archive.findOldValueAfter(dbName, physicalRawKey,
        targetBlock);
    OldValue value = historical.isPresent()
        ? historical.get() : latest.get(dbName, physicalRawKey);
    if (value == null) {
      throw new IllegalStateException("Pinned latest state returned null");
    }
    return value;
  }

  public long getTargetBlock() {
    return targetBlock;
  }

  public long getPinnedBlock() {
    return pinnedBlock;
  }

  public byte[] getPinnedHash() {
    return Arrays.copyOf(pinnedHash, pinnedHash.length);
  }

  /** Revalidates the request-owned history and latest head identity. */
  public synchronized void requirePinnedIdentity() {
    ensureOpen();
    validateIdentity();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      latest.close();
    } catch (IOException e) {
      failure = e;
    }
    try {
      archive.close();
    } catch (RuntimeException e) {
      if (failure == null) {
        failure = new IOException("Failed to close checkpoint Archive reader", e);
      } else {
        failure.addSuppressed(e);
      }
    } finally {
      lease.close();
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void validateIdentity() {
    if (pinnedBlock != archive.getIndexedThrough()
        || !Arrays.equals(pinnedHash, archive.getHeadHash())
        || latest.getBlockNumber() != pinnedBlock
        || !Arrays.equals(pinnedHash, latest.getBlockHash())) {
      throw new IllegalArgumentException("checkpoint Archive read snapshot identity mismatch");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("checkpoint Archive read snapshot is closed");
    }
  }

  private static void closeAfterFailedPin(CommonCheckpointRuntimeOwner.ReadLease lease,
      StateArchiveCheckpointReadAdapter archive, PinnedLatestState latest,
      Exception failure) {
    IOException closeFailure = closeResources(lease, archive, latest);
    if (closeFailure != null) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static IOException closeResources(CommonCheckpointRuntimeOwner.ReadLease lease,
      StateArchiveCheckpointReadAdapter archive, PinnedLatestState latest) {
    IOException failure = null;
    if (latest != null) {
      try {
        latest.close();
      } catch (IOException e) {
        failure = e;
      }
    }
    if (archive != null) {
      try {
        archive.close();
      } catch (RuntimeException e) {
        failure = append(failure, new IOException(
            "Failed to close checkpoint Archive reader", e));
      }
    }
    try {
      lease.close();
    } catch (RuntimeException e) {
      failure = append(failure, new IOException(
          "Failed to release common checkpoint read lease", e));
    }
    return failure;
  }

  private static IOException append(IOException failure, IOException addition) {
    if (failure == null) {
      return addition;
    }
    failure.addSuppressed(addition);
    return failure;
  }

  @FunctionalInterface
  public interface PinnedLatestStateFactory {
    PinnedLatestState pin(long blockNumber, byte[] blockHash) throws IOException;
  }
}
