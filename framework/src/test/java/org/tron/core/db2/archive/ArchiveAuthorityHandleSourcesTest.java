package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;

public class ArchiveAuthorityHandleSourcesTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void persistentAndReadOnlyHandlesProduceReadyAndReleaseEveryPin() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("ready").toPath(), false)) {
      ArchiveAuthoritySnapshotCollector collector = new ArchiveAuthoritySnapshotCollector(
          fixture.sources, fixture.sources, fixture.sources, fixture.sources);

      ArchiveFormatAdmissionValidator.Result result = ArchiveFormatAdmissionValidator.inspect(
          fixture.archive, collector.collect());

      assertEquals(ArchiveFormatAdmissionValidator.Status.CURRENT_READY, result.getStatus());
      assertEquals(2, fixture.latestOpened.get());
      assertEquals(2, fixture.latestClosed.get());
      assertEquals(0, fixture.catalog.getReferenceCount(
          fixture.catalog.getCurrentGenerationId()));
    }
  }

  @Test
  public void latestIdentityFailureReleasesLatestAndCatalogPins() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("bad-latest").toPath(), true)) {
      ArchiveAuthoritySnapshotCollector collector = new ArchiveAuthoritySnapshotCollector(
          fixture.sources, fixture.sources, fixture.sources, fixture.sources);

      assertThrows(ArchivePersistenceException.class, collector::collect);

      assertEquals(1, fixture.latestOpened.get());
      assertEquals(1, fixture.latestClosed.get());
      assertEquals(0, fixture.catalog.getReferenceCount(
          fixture.catalog.getCurrentGenerationId()));
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Path archive;
    private final HistorySegmentStore bodies;
    private final HistoryIndexStore index;
    private final HistoryCommitStore history;
    private final PersistentServingKeyIndexCatalog catalog;
    private final AtomicInteger latestOpened = new AtomicInteger();
    private final AtomicInteger latestClosed = new AtomicInteger();
    private final ArchiveAuthorityHandleSources sources;

    private Fixture(Path root, boolean wrongLatestHead) throws Exception {
      archive = root.resolve("archive");
      List<String> participants = ArchiveParticipantDescriptor.current().getParticipants();
      ArchiveBaseManifest manifest = new ArchiveBaseManifest(archive, participants);
      manifest.ensureBase(meta(1));
      bodies = new HistorySegmentStore(archive, new BlockHistoryCodec(), 4096);
      index = new HistoryIndexStore(archive, new HistoryIndexCodec());
      history = new HistoryCommitStore(archive, new HistoryCommitMarkerCodec());
      BlockReverseDiff diff = new BlockReverseDiff(meta(1), Collections.emptyList());
      HistoryLocation body = bodies.append(diff);
      HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
      HistoryCommitMarker marker = new HistoryCommitMarker(diff.getMeta(), 0, body,
          indexLocation, digest16(41), participants);
      bodies.sync();
      index.sync();
      history.commit(marker);

      ArchiveProgressEnvelope checkpoint = progress(
          ArchiveProgressEnvelope.Kind.APPLY_CHECKPOINT, null, marker);
      ArchiveProgressEnvelope reader = progress(
          ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, marker);
      Path checkpointPath = archive.resolve("progress/checkpoint.progress");
      Path readerPath = archive.resolve("progress/reader.progress");
      ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
      new ArchiveProgressFile(checkpointPath, progressCodec).store(checkpoint);
      new ArchiveProgressFile(readerPath, progressCodec).store(reader);

      Map<String, ArchiveParticipantProgressSource> participantSources = new LinkedHashMap<>();
      for (String participant : participants) {
        ArchiveProgressEnvelope participantProgress = progress(
            ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, participant, marker);
        participantSources.put(participant, () -> participantProgress);
      }

      Path shadow = root.resolve("serving-shadow");
      try (PersistentServingKeyIndexGeneration generation =
          PersistentServingKeyIndexGeneration.build(shadow, "generation-1", 0, hash(0),
              Collections.singletonList(marker), index::read, participants, digest(90))) {
        assertEquals(1, generation.getIndexedThrough());
      }
      catalog = PersistentServingKeyIndexCatalog.create(root.resolve("catalog"), shadow, reader);
      ArchiveReadSnapshot.PinnedLatestStateFactory latestFactory = serving -> {
        latestOpened.incrementAndGet();
        return new TestLatestPin(serving, wrongLatestHead, latestClosed);
      };
      sources = new ArchiveAuthorityHandleSources(history, checkpointPath, participantSources,
          readerPath, catalog, latestFactory);
    }

    @Override
    public void close() throws Exception {
      IOException failure = null;
      try {
        catalog.close();
      } catch (IOException closeFailure) {
        failure = closeFailure;
      }
      history.close();
      index.close();
      bodies.close();
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static final class TestLatestPin implements PinnedLatestState {
    private final long block;
    private final byte[] hash;
    private final byte[] sourceDigest;
    private final AtomicInteger closed;
    private boolean released;

    private TestLatestPin(PersistentServingKeyIndexGeneration serving, boolean wrongHead,
        AtomicInteger closed) {
      block = serving.getIndexedThrough();
      hash = wrongHead ? hash(2) : serving.getHeadHash();
      sourceDigest = serving.getLatestSourceIdentityDigest();
      this.closed = closed;
    }

    @Override
    public long getBlockNumber() {
      return block;
    }

    @Override
    public byte[] getBlockHash() {
      return Arrays.copyOf(hash, hash.length);
    }

    @Override
    public byte[] getSourceIdentityDigest() {
      return Arrays.copyOf(sourceDigest, sourceDigest.length);
    }

    @Override
    public OldValue get(String dbName, byte[] physicalRawKey) {
      throw new AssertionError("Admission must not read latest business data");
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive) {
      throw new AssertionError("Admission must not scan latest business data");
    }

    @Override
    public void close() {
      assertTrue(!released);
      released = true;
      closed.incrementAndGet();
    }
  }

  private static ArchiveProgressEnvelope progress(ArchiveProgressEnvelope.Kind kind,
      String participant, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(),
        ArchiveParticipantDescriptor.current().getParticipants());
  }

  private static BlockSnapshotMeta meta(long epoch) {
    return new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L);
  }

  private static byte[] hash(long suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }

  private static byte[] digest(int value) {
    byte[] digest = new byte[32];
    Arrays.fill(digest, (byte) value);
    return digest;
  }

  private static byte[] digest16(int value) {
    byte[] digest = new byte[16];
    Arrays.fill(digest, (byte) value);
    return digest;
  }
}
