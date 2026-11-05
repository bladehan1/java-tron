package org.tron.core.db2.archive.history.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.history.io.ArchiveFileDurability;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;

/** Owns staging and atomic publication while the host supplies recovery lifecycle callbacks. */
public final class ArchiveHistoryBootstrap {

  private ArchiveHistoryBootstrap() {
  }

  public interface RecoveredHistory extends Closeable {
    BlockSnapshotMeta getRecoveredHead();

    int getStartupRecoveryActionCount();
  }

  public interface Recovery<T> {
    RecoveredHistory recoverForValidation(Path directory, long maxSegmentSize) throws IOException;

    T recoverPublished(Path directory, long maxSegmentSize) throws IOException;
  }

  public static <T> T bootstrapAndRecover(Path archiveDirectory, long maxSegmentSize,
      BlockSnapshotMeta baseHead, Recovery<T> recovery) throws IOException {
    Objects.requireNonNull(recovery, "recovery");
    Path root = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    BlockSnapshotMeta head = Objects.requireNonNull(baseHead, "baseHead");
    Path parent = Objects.requireNonNull(root.getParent(), "archive parent directory");
    requireEmptyBootstrapTarget(root);
    Files.createDirectories(parent);
    Path staging = parent.resolve("." + root.getFileName() + ".bootstrap-" + UUID.randomUUID());
    List<String> names = storeNames();
    try {
      HistoryCommitMarker marker;
      try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(staging, maxSegmentSize,
          new java.util.LinkedHashSet<>(names))) {
        writer.accept(new BlockReverseDiff(head, Collections.emptyList()));
        marker = Objects.requireNonNull(writer.committedHead(), "bootstrap history head");
      }

      ArchiveBootstrapAnchor.store(staging, marker, names);

      try (RecoveredHistory verified = recovery.recoverForValidation(staging, maxSegmentSize)) {
        if (!head.equals(verified.getRecoveredHead())
            || verified.getStartupRecoveryActionCount() != 0) {
          throw new ArchivePersistenceException(
              "Fresh State Archive baseline did not recover at a zero-action fixed point");
        }
      }
      if (Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
        Files.delete(root);
      }
      try {
        Files.move(staging, root, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException failure) {
        throw new ArchivePersistenceException(
            "State Archive bootstrap requires atomic directory publication", failure);
      }
      ArchiveFileDurability.syncDirectory(parent);
      return recovery.recoverPublished(root, maxSegmentSize);
    } catch (IOException | RuntimeException failure) {
      throw failure;
    }
  }

  private static void requireEmptyBootstrapTarget(Path root) throws IOException {
    if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    if (Files.isSymbolicLink(root)
        || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
      throw new ArchivePersistenceException("State Archive bootstrap target is not a directory");
    }
    try (java.util.stream.Stream<Path> entries = Files.list(root)) {
      if (entries.findAny().isPresent()) {
        throw new ArchivePersistenceException("State Archive bootstrap target is not empty");
      }
    }
  }

  private static List<String> storeNames() {
    List<String> names = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(names);
    return names;
  }
}
