package org.tron.core.db2.archive.checkpoint.authority;

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
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.checkpoint.authority.ArchiveAuthoritySnapshotCollector.HistorySource;
import org.tron.core.db2.archive.checkpoint.authority.ArchiveAuthoritySnapshotCollector.LatestSource;
import org.tron.core.db2.archive.checkpoint.authority.ArchiveAuthoritySnapshotCollector.ProgressSource;
import org.tron.core.db2.archive.checkpoint.authority.ArchiveAuthoritySnapshotCollector.ServingSource;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveParticipantProgressSource;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveProgressEnvelope;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveProgressEnvelopeCodec;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveProgressReader;
import org.tron.core.db2.archive.history.model.CommittedHistoryAuthority;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;
import org.tron.core.db2.archive.history.model.HistoryCoverage;
import org.tron.core.db2.archive.query.snapshot.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.query.snapshot.ArchiveReadSnapshot.PinnedLatestStateFactory;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexCatalog;
import org.tron.core.db2.archive.serving.PersistentServingKeyIndexGeneration;

/** Read-only collector adapters over already-opened archive authority handles. */
public final class ArchiveAuthorityHandleSources
    implements HistorySource, ProgressSource, ServingSource, LatestSource {

  private final CommittedHistoryAuthority history;
  private final ArchiveTargetMutationPlanFile planFile;
  private final ArchiveProgressReader checkpointFile;
  private final ArchiveProgressReader readerFile;
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
    this.checkpointFile = new ArchiveProgressReader(checkpointPath, codec);
    this.readerFile = new ArchiveProgressReader(
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
    return checkpointFile.read();
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
    return readerFile.read();
  }

  @Override
  public ArchiveAuthoritySourceBundle.ServingGenerationSnapshot current() throws IOException {
    ArchiveProgressEnvelope reader = readerFile.read();
    try (PersistentServingKeyIndexGeneration generation = catalog.pin(reader)) {
      return snapshot(generation);
    }
  }

  @Override
  public byte[] sourceIdentityDigest() throws IOException {
    ArchiveProgressEnvelope reader = readerFile.read();
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
    List<String> participants = ArchiveStoreScope.getParticipants();
    if (!new ArrayList<>(sorted.keySet()).equals(participants) || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Archive participant source set is not exact-27");
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
  }
}
