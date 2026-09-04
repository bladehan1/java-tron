package org.tron.core.db2.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.StateArchiveCheckpointReadSnapshot;

/** Isolated composition boundary for the next-format common-checkpoint runtime. */
public final class CommonCheckpointRuntime implements AutoCloseable {

  private final CommonCheckpointRuntimeOwner owner;
  private final List<Chainbase> databases;
  private final Path archiveDirectory;
  private final byte[] formatIdentity;
  private final StateArchiveCheckpointReadSnapshot.PinnedLatestStateFactory latestFactory;
  private final CommonCheckpointPayloadFactory payloadFactory = new CommonCheckpointPayloadFactory();
  private final CommonCheckpointSnapshotRebaser rebaser = new CommonCheckpointSnapshotRebaser();

  public CommonCheckpointRuntime(CommonCheckpointRuntimeOwner owner, List<Chainbase> databases,
      Path archiveDirectory, byte[] formatIdentity,
      StateArchiveCheckpointReadSnapshot.PinnedLatestStateFactory latestFactory) {
    this.owner = Objects.requireNonNull(owner, "owner");
    this.databases = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    if (this.databases.isEmpty() || this.databases.contains(null)) {
      throw new IllegalArgumentException("common checkpoint runtime requires registered Stores");
    }
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    this.formatIdentity = requireDigest(formatIdentity);
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
  }

  /** Completes durable redo before this runtime admits checkpoint reads or new flushes. */
  public CommonCheckpointRedoCoordinator.RecoveryAction recoverBeforeServing()
      throws IOException {
    return owner.recoverBeforeServing();
  }

  /**
   * Captures and applies the immutable Snapshot prefix, then rebases it without a second Store
   * write. The caller must hold the SnapshotManager monitor for the whole call.
   */
  public CommonCheckpointTarget checkpointAndRebase(int flushCount) throws IOException {
    CommonCheckpointPayload payload = payloadFactory.capture(formatIdentity, databases,
        flushCount);
    CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
    owner.apply(payload, () -> rebaser.rebase(databases, target, flushCount));
    return target;
  }

  /** Pins one point-only historical request under the same publication gate. */
  public StateArchiveCheckpointReadSnapshot pinPoint(long targetBlock) throws IOException {
    return StateArchiveCheckpointReadSnapshot.pin(targetBlock, owner, archiveDirectory,
        formatIdentity, latestFactory);
  }

  public CommonCheckpointRuntimeOwner.State getState() {
    return owner.getState();
  }

  @Override
  public void close() {
    owner.close();
  }

  private static byte[] requireDigest(byte[] value) {
    byte[] admitted = Arrays.copyOf(Objects.requireNonNull(value, "formatIdentity"),
        value.length);
    if (admitted.length != 32) {
      throw new IllegalArgumentException("formatIdentity must contain exactly 32 bytes");
    }
    return admitted;
  }
}
