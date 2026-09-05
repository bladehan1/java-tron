package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Request-owned exact-key reader for one published next-format checkpoint target. */
public final class StateArchiveCheckpointReadAdapter implements AutoCloseable {

  private final CommonCheckpointTarget target;
  private final StateArchiveCheckpointServingIndex.Reader reader;
  private boolean closed;

  private StateArchiveCheckpointReadAdapter(CommonCheckpointTarget target,
      StateArchiveCheckpointServingIndex.Reader reader) {
    this.target = target;
    this.reader = reader;
  }

  /** Opens only an exact target whose Archive READABLE and serving-index markers are published. */
  public static StateArchiveCheckpointReadAdapter open(Path archiveDirectory,
      CommonCheckpointTarget target) throws IOException {
    return open(archiveDirectory, target, StateArchiveCheckpointServingIndex.configuredEngine());
  }

  public static StateArchiveCheckpointReadAdapter open(Path archiveDirectory,
      CommonCheckpointTarget target, Engine engine) throws IOException {
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    StateArchiveCheckpointMaterializer materializer =
        new StateArchiveCheckpointMaterializer(directory, admitted.getFormatIdentity(), null,
            engine);
    if (materializer.inspect(admitted) != Status.PUBLISHED) {
      throw new IOException("State Archive checkpoint target is not published for reading");
    }
    return new StateArchiveCheckpointReadAdapter(admitted,
        StateArchiveCheckpointServingIndex.openReader(directory, admitted, engine));
  }

  /** Opens a target already validated and pinned by the common-checkpoint runtime. */
  public static StateArchiveCheckpointReadAdapter openTrusted(Path archiveDirectory,
      CommonCheckpointTarget target, Engine engine) throws IOException {
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    return new StateArchiveCheckpointReadAdapter(admitted,
        StateArchiveCheckpointServingIndex.openTrustedReader(directory, admitted, engine));
  }

  /** Reconstructs the published target from disk before opening the exact-point reader. */
  public static StateArchiveCheckpointReadAdapter open(Path archiveDirectory,
      byte[] expectedFormatIdentity) throws IOException {
    return open(archiveDirectory, expectedFormatIdentity,
        StateArchiveCheckpointServingIndex.configuredEngine());
  }

  public static StateArchiveCheckpointReadAdapter open(Path archiveDirectory,
      byte[] expectedFormatIdentity, Engine engine) throws IOException {
    Path directory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    return open(directory, StateArchiveCheckpointMaterializer.loadPublishedTarget(directory,
        expectedFormatIdentity, engine), engine);
  }

  /**
   * Returns the old value from the first change in {@code (targetBlock, publishedHead]}, or empty
   * when the caller must use its pinned latest-state value.
   */
  public synchronized Optional<OldValue> findOldValueAfter(String dbName, byte[] rawKey,
      long targetBlock) throws IOException {
    ensureOpen();
    OptionalLong first = reader.firstChangeAfter(dbName, rawKey, targetBlock,
        target.getLastBlock().getBlockNumber());
    return first.isPresent()
        ? Optional.of(reader.readOldValue(dbName, rawKey, first.getAsLong()))
        : Optional.empty();
  }

  public long getIndexedFrom() {
    return reader.getIndexedFrom();
  }

  public long getIndexedThrough() {
    return reader.getIndexedThrough();
  }

  public byte[] getHeadHash() {
    return reader.getHeadHash();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      reader.close();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("State Archive checkpoint read adapter is closed");
    }
  }
}
