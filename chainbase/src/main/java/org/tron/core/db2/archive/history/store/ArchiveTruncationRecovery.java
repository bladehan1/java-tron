package org.tron.core.db2.archive.history.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.archive.history.codec.BlockHistoryCodec;
import org.tron.core.db2.archive.history.codec.HistoryCommitMarkerCodec;
import org.tron.core.db2.archive.history.codec.HistoryIndexCodec;
import org.tron.core.db2.archive.recovery.ArchiveCommitLogRecovery;

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
    ArchiveCommitLogRecovery.shrink(archiveDirectory, intent.commitEndOffset(),
        intent.markerOffset(), intent.getEncodedMarker());
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
