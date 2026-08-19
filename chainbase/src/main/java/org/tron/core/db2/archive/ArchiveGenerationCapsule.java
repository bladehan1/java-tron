package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/** Acquires one reader-visible serving/history/latest resource capsule from durable R. */
public final class ArchiveGenerationCapsule {

  private final PersistentServingKeyIndexCatalog catalog;
  private final Path readerVisiblePath;
  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory;

  public ArchiveGenerationCapsule(PersistentServingKeyIndexCatalog catalog,
      Path readerVisiblePath, Path archiveDirectory, long maxSegmentSize,
      ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.readerVisiblePath = Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.maxSegmentSize = maxSegmentSize;
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
  }

  public ArchiveReadSnapshot pin(long targetBlock) throws IOException {
    ArchiveProgressEnvelope readerVisible = new ArchiveProgressFile(readerVisiblePath,
        new ArchiveProgressEnvelopeCodec()).load();
    return ArchiveReadSnapshot.pin(targetBlock, catalog, readerVisible, archiveDirectory,
        maxSegmentSize, serving -> pinLatest(serving));
  }

  private ArchiveReadSnapshot.PinnedLatestState pinLatest(
      PersistentServingKeyIndexGeneration serving) throws IOException {
    if (!serving.isLatestSourceIdentityBound()) {
      throw new ArchivePersistenceException(
          "Serving generation is not bound to latest engine source identities");
    }
    ArchiveReadSnapshot.PinnedLatestState latest = latestFactory.pin(serving);
    if (!java.util.Arrays.equals(serving.getLatestSourceIdentityDigest(),
        latest.getSourceIdentityDigest())) {
      try {
        latest.close();
      } catch (IOException closeFailure) {
        ArchivePersistenceException mismatch = new ArchivePersistenceException(
            "Latest engine source identity digest mismatch");
        mismatch.addSuppressed(closeFailure);
        throw mismatch;
      }
      throw new ArchivePersistenceException("Latest engine source identity digest mismatch");
    }
    return latest;
  }
}
