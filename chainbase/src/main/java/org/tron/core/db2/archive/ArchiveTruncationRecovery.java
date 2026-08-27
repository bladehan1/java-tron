package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;

/** Completes an atomic truncation intent before normal checkpoint-based startup. */
public final class ArchiveTruncationRecovery {

  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final HistoryCommitMarkerCodec markerCodec = new HistoryCommitMarkerCodec();
  private final FaultHook faultHook;

  public ArchiveTruncationRecovery(Path archiveDirectory, long maxSegmentSize) {
    this(archiveDirectory, maxSegmentSize, stage -> { });
  }

  ArchiveTruncationRecovery(Path archiveDirectory, long maxSegmentSize, FaultHook faultHook) {
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.maxSegmentSize = maxSegmentSize;
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public boolean recover() throws IOException {
    ArchiveTruncationIntent intent = ArchiveTruncationIntent.load(archiveDirectory, markerCodec);
    if (intent == null) {
      return false;
    }
    shrinkCommitLog(intent);
    faultHook.afterDurableStage(Stage.COMMIT_SHRUNK);
    ArchiveHistoryScanAnchor checkpoint = intent.persistCheckpoint(archiveDirectory, markerCodec);
    faultHook.afterDurableStage(Stage.CHECKPOINT_PUBLISHED);

    try (HistoryIndexStore index = new HistoryIndexStore(
        archiveDirectory, new HistoryIndexCodec(), checkpoint)) {
      index.truncateAfter(intent.getMarker().getIndexLocation(), intent.getRecordCount());
    }
    faultHook.afterDurableStage(Stage.INDEX_TRUNCATED);
    try (HistoryBodyStore bodies = new PartitionedHistoryBodyStore(archiveDirectory,
        new BlockHistoryCodec(), maxSegmentSize, checkpoint)) {
      bodies.truncateAfter(intent.getMarker().getHistoryLocation(), intent.getRecordCount());
    }
    faultHook.afterDurableStage(Stage.BODY_TRUNCATED);
    intent.clear(archiveDirectory);
    return true;
  }

  private void shrinkCommitLog(ArchiveTruncationIntent intent) throws IOException {
    Path path = archiveDirectory.resolve("commits/commit.log");
    if (!Files.exists(path)) {
      throw new ArchivePersistenceException("Committed history log is missing during truncation");
    }
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      if (channel.size() < intent.commitEndOffset()) {
        throw new ArchivePersistenceException(
            "Committed history log is shorter than truncation intent");
      }
      ByteBuffer marker = ByteBuffer.allocate(intent.getEncodedMarker().length);
      channel.position(intent.markerOffset());
      while (marker.hasRemaining()) {
        if (channel.read(marker) < 0) {
          throw new ArchivePersistenceException(
              "Committed history target is truncated during recovery");
        }
      }
      if (!Arrays.equals(marker.array(), intent.getEncodedMarker())) {
        throw new ArchivePersistenceException(
            "Committed history target does not match truncation intent");
      }
      channel.truncate(intent.commitEndOffset());
      channel.force(true);
    }
  }

  public enum Stage {
    COMMIT_SHRUNK,
    CHECKPOINT_PUBLISHED,
    INDEX_TRUNCATED,
    BODY_TRUNCATED
  }

  @FunctionalInterface
  interface FaultHook {
    void afterDurableStage(Stage stage) throws IOException;
  }
}
