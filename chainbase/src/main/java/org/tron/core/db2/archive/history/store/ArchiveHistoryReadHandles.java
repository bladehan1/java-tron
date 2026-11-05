package org.tron.core.db2.archive.history.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.history.codec.BlockHistoryCodec;
import org.tron.core.db2.archive.history.codec.HistoryCommitMarkerCodec;
import org.tron.core.db2.archive.history.codec.HistoryIndexCodec;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;
import org.tron.core.db2.archive.history.model.HistoryIndexLocation;
import org.tron.core.db2.archive.history.model.HistoryIndexRecord;
import org.tron.core.db2.archive.history.model.HistoryLocation;

/**
 * Owns the package-private body/index/commit handles used by one persistent history reader.
 * The reader serializes access and owns normal-close idempotence.
 */
public final class ArchiveHistoryReadHandles implements Closeable {

  private final HistoryBodyStore bodies;
  private final HistoryIndexStore index;
  private final HistoryCommitStore commits;

  ArchiveHistoryReadHandles(HistoryBodyStore bodies, HistoryIndexStore index,
      HistoryCommitStore commits) {
    this.bodies = bodies;
    this.index = index;
    this.commits = commits;
  }

  public static ArchiveHistoryReadHandles open(Path archiveDirectory, long maxSegmentSize)
      throws IOException {
    ArchiveHistoryScanAnchor checkpoint = ArchiveHistoryScanAnchor.load(archiveDirectory,
        new HistoryCommitMarkerCodec());
    if (checkpoint == null) {
      throw new ArchivePersistenceException("Archive history scan anchor is missing");
    }
    HistoryBodyStore openedBodies = null;
    HistoryIndexStore openedIndex = null;
    HistoryCommitStore openedCommits = null;
    try {
      openedBodies = new PartitionedHistoryBodyStore(archiveDirectory, new BlockHistoryCodec(),
          maxSegmentSize, checkpoint);
      openedIndex = new HistoryIndexStore(archiveDirectory, new HistoryIndexCodec(), checkpoint);
      openedCommits = new HistoryCommitStore(archiveDirectory, new HistoryCommitMarkerCodec(),
          checkpoint);
    } catch (IOException | RuntimeException failure) {
      closeAfterFailedConstruction(openedBodies, openedIndex, openedCommits, failure);
      throw failure;
    }
    return new ArchiveHistoryReadHandles(openedBodies, openedIndex, openedCommits);
  }

  public HistoryCommitMarker get(long epoch) {
    return commits.get(epoch);
  }

  public HistoryIndexRecord readIndex(HistoryIndexLocation location) throws IOException {
    return index.read(location);
  }

  public BlockReverseDiff readBody(HistoryLocation location) throws IOException {
    return bodies.read(location);
  }

  /** Close every opened handle, suppressing each IOException on the original failure. */
  public void closeAfterFailedConstruction(Exception failure) {
    closeAfterFailedConstruction(bodies, index, commits, failure);
  }

  private static void closeAfterFailedConstruction(HistoryBodyStore bodies,
      HistoryIndexStore index, HistoryCommitStore commits, Exception failure) {
    close(index, failure);
    close(bodies, failure);
    close(commits, failure);
  }

  @Override
  public void close() throws IOException {
    IOException failure = null;
    try {
      index.close();
    } catch (IOException closeFailure) {
      failure = closeFailure;
    }
    try {
      bodies.close();
    } catch (IOException closeFailure) {
      failure = add(failure, closeFailure);
    }
    try {
      commits.close();
    } catch (IOException closeFailure) {
      failure = add(failure, closeFailure);
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void close(java.io.Closeable resource, Exception failure) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static IOException add(IOException current, IOException addition) {
    if (current == null) {
      return addition;
    }
    current.addSuppressed(addition);
    return current;
  }
}
