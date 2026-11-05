package org.tron.core.db2.archive.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.tron.core.db2.archive.ArchivePersistenceException;

/** Validates the target marker before durably shrinking a truncation intent's commit log. */
public final class ArchiveCommitLogRecovery {

  private ArchiveCommitLogRecovery() {
  }

  /** Called only after the owning recovery has loaded and validated its durable intent. */
  public static void shrink(Path archiveDirectory, long commitEndOffset, long markerOffset,
      byte[] encodedMarker) throws IOException {
    Path path = archiveDirectory.resolve("commits/commit.log");
    if (!Files.exists(path)) {
      throw new ArchivePersistenceException("Committed history log is missing during truncation");
    }
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      if (channel.size() < commitEndOffset) {
        throw new ArchivePersistenceException(
            "Committed history log is shorter than truncation intent");
      }
      ByteBuffer marker = ByteBuffer.allocate(encodedMarker.length);
      channel.position(markerOffset);
      while (marker.hasRemaining()) {
        if (channel.read(marker) < 0) {
          throw new ArchivePersistenceException(
              "Committed history target is truncated during recovery");
        }
      }
      if (!Arrays.equals(marker.array(), encodedMarker)) {
        throw new ArchivePersistenceException(
            "Committed history target does not match truncation intent");
      }
      channel.truncate(commitEndOffset);
      channel.force(true);
    }
  }
}
