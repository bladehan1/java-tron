package org.tron.core.db2.archive.query.latest;

import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveProgressReader;
import org.tron.core.db2.core.SnapshotManager;

/** Builds a latest-state coordinator from one frozen SnapshotManager Store registry. */
public final class LatestStateGenerationCoordinatorFactory {

  private LatestStateGenerationCoordinatorFactory() {
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      Path readerVisiblePath) throws ArchivePersistenceException {
    Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    ArchiveProgressReader readerVisible = new ArchiveProgressReader(readerVisiblePath);
    return create(manager, readerVisible::read);
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      LatestStateGenerationCoordinator.AuthorityReader authorityReader)
      throws ArchivePersistenceException {
    return LatestStateGenerationRegistry.create(manager, authorityReader);
  }

  /** Builds a direct request-pinning adapter for the common-checkpoint read gate. */
  public static LatestStateGenerationAdapter createAdapter(SnapshotManager manager)
      throws ArchivePersistenceException {
    return LatestStateGenerationRegistry.createAdapter(manager);
  }
}
