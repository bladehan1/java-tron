package org.tron.core.db2.archive;

import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.archive.query.latest.LatestStateGenerationAdapter;
import org.tron.core.db2.archive.query.latest.LatestStateGenerationCoordinator;
import org.tron.core.db2.archive.query.latest.LatestStateGenerationRegistry;
import org.tron.core.db2.core.SnapshotManager;

/** Builds a latest-state coordinator from one frozen SnapshotManager Store registry. */
public final class LatestStateGenerationCoordinatorFactory {

  private LatestStateGenerationCoordinatorFactory() {
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      Path readerVisiblePath) throws ArchivePersistenceException {
    Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    ArchiveProgressFile readerVisible = new ArchiveProgressFile(readerVisiblePath,
        new ArchiveProgressEnvelopeCodec());
    return create(manager, readerVisible::load);
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
