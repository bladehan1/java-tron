package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.HistorySource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.LatestSource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.ProgressSource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.ServingSource;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestStateFactory;

/** Read-only collector adapters over already-opened archive authority handles. */
public final class ArchiveAuthorityHandleSources
    implements HistorySource, ProgressSource, ServingSource, LatestSource {

  private final CommittedHistoryAuthority history;
  private final ArchiveTargetMutationPlanFile planFile;
  private final ArchiveProgressFile checkpointFile;
  private final ArchiveProgressFile readerFile;
  private final Map<String, ArchiveParticipantProgressSource> participantSources;
  private final PersistentServingKeyIndexCatalog catalog;
  private final PinnedLatestStateFactory latestFactory;

  public ArchiveAuthorityHandleSources(CommittedHistoryAuthority history, Path checkpointPath,
      Map<String, ? extends ArchiveParticipantProgressSource> participantSources,
      Path readerVisiblePath, PersistentServingKeyIndexCatalog catalog,
      PinnedLatestStateFactory latestFactory) {
    this.history = Objects.requireNonNull(history, "history");
    this.planFile = new ArchiveTargetMutationPlanFile(
        Objects.requireNonNull(checkpointPath, "checkpointPath"));
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    this.checkpointFile = new ArchiveProgressFile(checkpointPath, codec);
    this.readerFile = new ArchiveProgressFile(
        Objects.requireNonNull(readerVisiblePath, "readerVisiblePath"), codec);
    this.participantSources = exactParticipantSources(participantSources);
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.latestFactory = Objects.requireNonNull(latestFactory, "latestFactory");
  }

  @Override
  public HistoryCoverage coverage() {
    return history.coverage();
  }

  @Override
  public HistoryCommitMarker first() {
    long firstEpoch = history.firstEpoch();
    return firstEpoch < 0 ? null : history.get(firstEpoch);
  }

  @Override
  public HistoryCommitMarker head() {
    return history.head();
  }

  @Override
  public boolean mutationPlanPresent() throws IOException {
    return planFile.loadIfPresent() != null;
  }

  @Override
  public ArchiveProgressEnvelope applyCheckpoint() throws IOException {
    return checkpointFile.load();
  }

  @Override
  public Map<String, ArchiveProgressEnvelope> participantProgress() throws IOException {
    Map<String, ArchiveProgressEnvelope> loaded = new LinkedHashMap<>();
    for (Map.Entry<String, ArchiveParticipantProgressSource> entry
        : participantSources.entrySet()) {
      loaded.put(entry.getKey(), entry.getValue().loadProgress());
    }
    return loaded;
  }

  @Override
  public ArchiveProgressEnvelope readerVisible() throws IOException {
    return readerFile.load();
  }

  @Override
  public ArchiveAuthoritySourceBundle.ServingGenerationSnapshot current() throws IOException {
    ArchiveProgressEnvelope reader = readerFile.load();
    try (PersistentServingKeyIndexGeneration generation = catalog.pin(reader)) {
      return snapshot(generation);
    }
  }

  @Override
  public byte[] sourceIdentityDigest() throws IOException {
    ArchiveProgressEnvelope reader = readerFile.load();
    try (PersistentServingKeyIndexGeneration generation = catalog.pin(reader);
        PinnedLatestState latest = latestFactory.pin(generation)) {
      if (latest.getBlockNumber() != generation.getIndexedThrough()
          || !Arrays.equals(latest.getBlockHash(), generation.getHeadHash())) {
        throw new ArchivePersistenceException(
            "Pinned latest source does not match serving generation head");
      }
      byte[] digest = latest.getSourceIdentityDigest();
      return digest == null ? null : Arrays.copyOf(digest, digest.length);
    }
  }

  private static ArchiveAuthoritySourceBundle.ServingGenerationSnapshot snapshot(
      PersistentServingKeyIndexGeneration generation) {
    return new ArchiveAuthoritySourceBundle.ServingGenerationSnapshot(
        generation.getScopeIdentity(), generation.getParticipatingDatabases(),
        generation.getIndexedFrom(), generation.getIndexedThrough(), generation.getHeadHash(),
        generation.getAuthoritativePrefixDigest(), generation.getLatestSourceIdentityDigest());
  }

  private static Map<String, ArchiveParticipantProgressSource> exactParticipantSources(
      Map<String, ? extends ArchiveParticipantProgressSource> actual) {
    TreeMap<String, ArchiveParticipantProgressSource> sorted = new TreeMap<>();
    Objects.requireNonNull(actual, "participantSources").forEach(sorted::put);
    List<String> participants = ArchiveParticipantDescriptor.current().getParticipants();
    if (!new ArrayList<>(sorted.keySet()).equals(participants) || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant source set is not exact-27");
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
  }
}
