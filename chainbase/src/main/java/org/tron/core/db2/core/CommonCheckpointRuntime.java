package org.tron.core.db2.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.StateArchiveCheckpointMaterializer;
import org.tron.core.db2.archive.StateArchiveCheckpointReadSnapshot;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Isolated composition boundary for the next-format common-checkpoint runtime. */
public final class CommonCheckpointRuntime implements AutoCloseable {

  private final CommonCheckpointRuntimeOwner owner;
  private final List<Chainbase> databases;
  private final Path archiveDirectory;
  private final byte[] formatIdentity;
  private final Engine engine;
  private final StateArchiveCheckpointReadSnapshot.PinnedLatestStateFactory latestFactory;
  private final CommonCheckpointMemoryRebaser memoryRebaser;
  private final CommonCheckpointPayloadFactory payloadFactory = new CommonCheckpointPayloadFactory();
  private final CommonCheckpointSnapshotRebaser rebaser = new CommonCheckpointSnapshotRebaser();
  private CommonCheckpointTarget publishedTarget;

  public CommonCheckpointRuntime(CommonCheckpointRuntimeOwner owner, List<Chainbase> databases,
      Path archiveDirectory, byte[] formatIdentity, Engine engine,
      StateArchiveCheckpointReadSnapshot.PinnedLatestStateFactory latestFactory,
      CommonCheckpointMemoryRebaser memoryRebaser) {
    this.owner = Objects.requireNonNull(owner, "owner");
    this.databases = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    if (this.databases.isEmpty() || this.databases.contains(null)) {
      throw new IllegalArgumentException("common checkpoint runtime requires registered Stores");
    }
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    this.formatIdentity = requireDigest(formatIdentity);
    this.engine = Objects.requireNonNull(engine, "engine");
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
    this.memoryRebaser = Objects.requireNonNull(memoryRebaser, "memoryRebaser");
  }

  /** Completes durable redo before this runtime admits checkpoint reads or new flushes. */
  public synchronized CommonCheckpointRedoCoordinator.RecoveryAction recoverBeforeServing()
      throws IOException {
    CommonCheckpointRedoCoordinator.RecoveryAction action = owner.recoverBeforeServing();
    publishedTarget = StateArchiveCheckpointMaterializer.loadPublishedTargetIfPresent(
        archiveDirectory, formatIdentity, engine).orElse(null);
    return action;
  }

  /**
   * Captures and applies the immutable Snapshot prefix, then rebases it without a second Store
   * write. The caller must hold the SnapshotManager monitor for the whole call.
   */
  public synchronized CommonCheckpointTarget checkpointAndRebase(int flushCount)
      throws IOException {
    CommonCheckpointPayload payload = payloadFactory.capture(formatIdentity, databases,
        flushCount);
    CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
    owner.apply(payload, () -> {
      CommonCheckpointSnapshotRebaser.Plan chainbasePlan =
          rebaser.prepare(databases, target, flushCount);
      CommonCheckpointMemoryRebaser.RebasePlan pathStatePlan = memoryRebaser.prepare(target);
      chainbasePlan.apply();
      pathStatePlan.apply();
    });
    publishedTarget = target;
    return target;
  }

  /** Pins one point-only historical request under the same publication gate. */
  public synchronized StateArchiveCheckpointReadSnapshot pinPoint(long targetBlock)
      throws IOException {
    CommonCheckpointTarget target = publishedTarget;
    if (target == null) {
      throw new IOException("State Archive has no published common-checkpoint target");
    }
    return StateArchiveCheckpointReadSnapshot.pin(targetBlock, owner, archiveDirectory,
        target, engine, latestFactory);
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
