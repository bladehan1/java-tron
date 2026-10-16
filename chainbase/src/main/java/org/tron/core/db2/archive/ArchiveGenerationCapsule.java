package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/** Acquires one reader-visible serving/history/latest resource capsule from durable R. */
public final class ArchiveGenerationCapsule {

  private final PersistentServingKeyIndexCatalog catalog;
  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory;
  private final ReaderAuthority readerAuthority;

  public ArchiveGenerationCapsule(PersistentServingKeyIndexCatalog catalog,
      Path readerVisiblePath, Path archiveDirectory, long maxSegmentSize,
      ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory) {
    this(catalog, archiveDirectory, maxSegmentSize, latestFactory,
        new ArchiveProgressFile(Objects.requireNonNull(readerVisiblePath, "readerVisiblePath"),
            new ArchiveProgressEnvelopeCodec())::load);
  }

  public ArchiveGenerationCapsule(PersistentServingKeyIndexCatalog catalog,
      Path archiveDirectory, long maxSegmentSize,
      ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory,
      ReaderAuthority readerAuthority) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.maxSegmentSize = maxSegmentSize;
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
    this.readerAuthority = Objects.requireNonNull(readerAuthority, "readerAuthority");
  }

  public ArchiveReadSnapshot pin(long targetBlock) throws IOException {
    ArchiveProgressEnvelope readerVisible = Objects.requireNonNull(readerAuthority.read(),
        "reader-visible authority");
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

  @FunctionalInterface
  public interface ReaderAuthority {
    ArchiveProgressEnvelope read() throws IOException;
  }
}
