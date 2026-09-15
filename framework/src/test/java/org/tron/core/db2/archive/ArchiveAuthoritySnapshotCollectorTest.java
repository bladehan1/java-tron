package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.HistorySource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.LatestSource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.ProgressSource;
import org.tron.core.db2.archive.ArchiveAuthoritySnapshotCollector.ServingSource;

public class ArchiveAuthoritySnapshotCollectorTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void stableSourcesProduceOneReadyBundle() throws Exception {
    Path archive = temporaryFolder.newFolder("stable").toPath();
    ArchiveBaseManifest manifest = new ArchiveBaseManifest(archive,
        ArchiveParticipantDescriptor.current().getParticipants());
    manifest.ensureBase(meta(1));
    FakeSources sources = new FakeSources(Drift.NONE);
    ArchiveAuthoritySnapshotCollector collector = collector(sources);

    ArchiveFormatAdmissionValidator.Result result = ArchiveFormatAdmissionValidator.inspect(
        archive, collector.collect());

    assertEquals(ArchiveFormatAdmissionValidator.Status.CURRENT_READY, result.getStatus());
    assertEquals(2, sources.headReads);
    assertEquals(2, sources.planReads);
    assertEquals(2, sources.readerReads);
    assertEquals(2, sources.servingReads);
    assertEquals(2, sources.coverageReads);
    assertEquals(2, sources.latestReads);
  }

  @Test
  public void missingAuthorityAndSourceFailureFailClosed() throws Exception {
    FakeSources missing = new FakeSources(Drift.NONE);
    missing.checkpoint = null;
    assertThrows(ArchivePersistenceException.class, () -> collector(missing).collect());

    FakeSources failing = new FakeSources(Drift.NONE);
    failing.failHead = true;
    assertThrows(IOException.class, () -> collector(failing).collect());
  }

  @Test
  public void everyMutableBoundaryReplacementRejectsTheWholeSnapshot() {
    for (Drift drift : Arrays.asList(Drift.PLAN, Drift.HEAD, Drift.READER, Drift.SERVING,
        Drift.COVERAGE, Drift.LATEST)) {
      FakeSources sources = new FakeSources(drift);
      assertThrows(ArchivePersistenceException.class, () -> collector(sources).collect());
    }
  }

  private static ArchiveAuthoritySnapshotCollector collector(FakeSources sources) {
    return new ArchiveAuthoritySnapshotCollector(sources, sources, sources, sources);
  }

  private enum Drift {
    NONE,
    PLAN,
    HEAD,
    READER,
    SERVING,
    COVERAGE,
    LATEST
  }

  private static final class FakeSources
      implements HistorySource, ProgressSource, ServingSource, LatestSource {
    private final Drift drift;
    private final HistoryCommitMarker first = marker(1);
    private final HistoryCommitMarker head = marker(2);
    private ArchiveProgressEnvelope checkpoint;
    private final Map<String, ArchiveProgressEnvelope> participants = new LinkedHashMap<>();
    private final ArchiveProgressEnvelope reader;
    private boolean failHead;
    private int headReads;
    private int planReads;
    private int readerReads;
    private int servingReads;
    private int coverageReads;
    private int latestReads;

    private FakeSources(Drift drift) {
      this.drift = drift;
      checkpoint = progress(ArchiveProgressEnvelope.Kind.APPLY_CHECKPOINT, null, head);
      for (String participant : ArchiveParticipantDescriptor.current().getParticipants()) {
        participants.put(participant, progress(
            ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, participant, head));
      }
      reader = progress(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, head);
    }

    @Override
    public HistoryCoverage coverage() {
      coverageReads++;
      return drift == Drift.COVERAGE && coverageReads == 2
          ? new HistoryCoverage(1, 3, 3, hash(3))
          : new HistoryCoverage(1, 2, 2, hash(2));
    }

    @Override
    public HistoryCommitMarker first() {
      return first;
    }

    @Override
    public HistoryCommitMarker head() throws IOException {
      headReads++;
      if (failHead) {
        throw new IOException("history source unavailable");
      }
      return drift == Drift.HEAD && headReads == 2 ? marker(3) : head;
    }

    @Override
    public boolean mutationPlanPresent() {
      planReads++;
      return drift == Drift.PLAN && planReads == 2;
    }

    @Override
    public ArchiveProgressEnvelope applyCheckpoint() {
      return checkpoint;
    }

    @Override
    public Map<String, ArchiveProgressEnvelope> participantProgress() {
      return participants;
    }

    @Override
    public ArchiveProgressEnvelope readerVisible() {
      readerReads++;
      return drift == Drift.READER && readerReads == 2
          ? progress(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, first) : reader;
    }

    @Override
    public ArchiveAuthoritySourceBundle.ServingGenerationSnapshot current() {
      servingReads++;
      return serving(drift == Drift.SERVING && servingReads == 2 ? digest(81) : digest(80),
          digest(90));
    }

    @Override
    public byte[] sourceIdentityDigest() {
      latestReads++;
      return digest(drift == Drift.LATEST && latestReads == 2 ? 91 : 90);
    }
  }

  private static ArchiveAuthoritySourceBundle.ServingGenerationSnapshot serving(
      byte[] prefixDigest, byte[] latestDigest) {
    return new ArchiveAuthoritySourceBundle.ServingGenerationSnapshot(
        ArchiveParticipantDescriptor.FORMAT_ID,
        ArchiveParticipantDescriptor.current().getParticipants(), 0, 2, hash(2), prefixDigest,
        latestDigest);
  }

  private static ArchiveProgressEnvelope progress(ArchiveProgressEnvelope.Kind kind,
      String participant, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(),
        ArchiveParticipantDescriptor.current().getParticipants());
  }

  private static HistoryCommitMarker marker(long epoch) {
    return new HistoryCommitMarker(
        new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L),
        epoch - 1, new HistoryLocation(0, epoch * 100, 100, (int) epoch,
            digest(20 + (int) epoch)),
        new HistoryIndexLocation(epoch * 50, 50, digest(30 + (int) epoch)),
        digest16(40 + (int) epoch), ArchiveParticipantDescriptor.current().getParticipants());
  }

  private static BlockSnapshotMeta meta(long epoch) {
    return new BlockSnapshotMeta(epoch, epoch, hash(epoch), new byte[32], epoch * 1_000L);
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
