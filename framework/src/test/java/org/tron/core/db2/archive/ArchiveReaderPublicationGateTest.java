package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveReaderPublicationGate.ProgressSource;
import org.tron.core.db2.core.SnapshotManager;

public class ArchiveReaderPublicationGateTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void publishesExactAuthoritiesWhileMergeAndFlushAreBlocked() throws Exception {
    try (Fixture fixture = fixture()) {
      SnapshotManager manager = new SnapshotManager("");
      manager.enable();
      ISession parent = manager.buildSession();
      ISession child = manager.buildSession();
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicBoolean paused = new AtomicBoolean();
      Map<String, ProgressSource> sources = fixture.sources();
      String first = fixture.participants.get(0);
      ProgressSource original = sources.get(first);
      sources.put(first, () -> {
        ArchiveProgressEnvelope loaded = original.load();
        if (paused.compareAndSet(false, true)) {
          entered.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IOException("Timed out waiting to release publication gate");
            }
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted inside publication gate", interrupted);
          }
        }
        return loaded;
      });
      ArchiveReaderPublicationGate gate = new ArchiveReaderPublicationGate(fixture.history,
          fixture.checkpointSource(), sources, fixture.readerPath, fixture.participants,
          manager::withArchiveStateBarrier);
      ExecutorService executor = Executors.newFixedThreadPool(3);
      try {
        Future<?> publication = executor.submit(() -> {
          gate.publish(1);
          return null;
        });
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Future<?> merge = executor.submit(child::merge);
        Future<?> flush = executor.submit(manager::flush);
        assertThrows(TimeoutException.class,
            () -> merge.get(100, TimeUnit.MILLISECONDS));
        assertThrows(TimeoutException.class,
            () -> flush.get(100, TimeUnit.MILLISECONDS));

        release.countDown();
        publication.get(5, TimeUnit.SECONDS);
        merge.get(5, TimeUnit.SECONDS);
        flush.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.reader().getEpoch());
      } finally {
        release.countDown();
        child.close();
        parent.close();
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void missingMismatchedOrNullParticipantNeverAdvancesReader() throws Exception {
    try (Fixture missing = fixture()) {
      Files.delete(missing.participantPaths.get(missing.participants.get(0)));
      assertThrows(ArchivePersistenceException.class,
          () -> missing.fileGate().publish(1));
      assertEquals(0, missing.reader().getEpoch());
    }

    try (Fixture mismatch = fixture()) {
      String participant = mismatch.participants.get(0);
      mismatch.store(mismatch.participantPaths.get(participant),
          mismatch.envelope(Kind.PARTICIPANT_PROGRESS, participant, 0));
      assertThrows(ArchivePersistenceException.class,
          () -> mismatch.fileGate().publish(1));
      assertEquals(0, mismatch.reader().getEpoch());
    }

    try (Fixture absentLevelDb = fixture()) {
      Map<String, ProgressSource> sources = absentLevelDb.sources();
      sources.put("account", () -> null);
      ArchiveReaderPublicationGate gate = new ArchiveReaderPublicationGate(absentLevelDb.history,
          absentLevelDb.checkpointSource(), sources, absentLevelDb.readerPath,
          absentLevelDb.participants, action -> action.run());
      assertThrows(ArchivePersistenceException.class, () -> gate.publish(1));
      assertEquals(0, absentLevelDb.reader().getEpoch());
    }
  }

  @Test
  public void secondScanDriftAndRegressionPreserveCurrentReader() throws Exception {
    try (Fixture drift = fixture()) {
      Map<String, ProgressSource> sources = drift.sources();
      String participant = drift.participants.get(0);
      ProgressSource stable = sources.get(participant);
      AtomicInteger reads = new AtomicInteger();
      sources.put(participant, () -> reads.getAndIncrement() == 0
          ? stable.load() : drift.envelope(Kind.PARTICIPANT_PROGRESS, participant, 0));
      ArchiveReaderPublicationGate gate = new ArchiveReaderPublicationGate(drift.history,
          drift.checkpointSource(), sources, drift.readerPath, drift.participants,
          action -> action.run());
      assertThrows(ArchivePersistenceException.class, () -> gate.publish(1));
      assertEquals(0, drift.reader().getEpoch());
    }

    try (Fixture regression = fixture()) {
      regression.fileGate().publish(1);
      regression.writeAuthorities(0);
      assertThrows(ArchivePersistenceException.class,
          () -> regression.fileGate().publish(0));
      assertEquals(1, regression.reader().getEpoch());
    }
  }

  @Test
  public void publicationFaultKeepsOldReaderAndRetryPublishesOnce() throws Exception {
    try (Fixture fixture = fixture()) {
      ArchiveReaderPublicationGate failing = new ArchiveReaderPublicationGate(fixture.history,
          fixture.checkpointSource(), fixture.sources(), fixture.readerPath,
          fixture.participants, action -> action.run(), temporary -> {
        throw new IOException("injected after reader temporary force");
      });
      assertThrows(IOException.class, () -> failing.publish(1));
      assertEquals(0, fixture.reader().getEpoch());

      fixture.fileGate().publish(1);
      assertEquals(1, fixture.reader().getEpoch());
    }
  }

  private Fixture fixture() throws Exception {
    return new Fixture(temporaryFolder.newFolder().toPath());
  }

  private static final class Fixture implements AutoCloseable {
    private final List<String> participants;
    private final HistoryCommitStore history;
    private final Path checkpointPath;
    private final Map<String, Path> participantPaths = new LinkedHashMap<>();
    private final Path readerPath;
    private final ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();

    private Fixture(Path directory) throws Exception {
      participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
      java.util.Collections.sort(participants);
      history = new HistoryCommitStore(directory, new HistoryCommitMarkerCodec());
      history.commitAll(Arrays.asList(marker(0), marker(1)));
      checkpointPath = directory.resolve("progress/checkpoint.progress");
      for (String participant : participants) {
        participantPaths.put(participant,
            directory.resolve("progress/participants/" + participant + ".progress"));
      }
      readerPath = directory.resolve("progress/reader.progress");
      writeAuthorities(1);
      store(readerPath, envelope(Kind.READER_VISIBLE, null, 0));
    }

    private ArchiveReaderPublicationGate fileGate() {
      return ArchiveReaderPublicationGate.forFiles(history, checkpointPath, participantPaths,
          readerPath, participants, action -> action.run());
    }

    private ProgressSource checkpointSource() {
      return () -> new ArchiveProgressFile(checkpointPath, codec).load();
    }

    private Map<String, ProgressSource> sources() {
      Map<String, ProgressSource> sources = new TreeMap<>();
      participantPaths.forEach((participant, path) -> sources.put(participant,
          () -> new ArchiveProgressFile(path, codec).load()));
      return sources;
    }

    private void writeAuthorities(int epoch) throws IOException {
      store(checkpointPath, envelope(Kind.APPLY_CHECKPOINT, null, epoch));
      for (Map.Entry<String, Path> entry : participantPaths.entrySet()) {
        store(entry.getValue(),
            envelope(Kind.PARTICIPANT_PROGRESS, entry.getKey(), epoch));
      }
    }

    private ArchiveProgressEnvelope reader() throws IOException {
      return new ArchiveProgressFile(readerPath, codec).load();
    }

    private ArchiveProgressEnvelope envelope(Kind kind, String participant, int epoch) {
      HistoryCommitMarker marker = history.get(epoch);
      return new ArchiveProgressEnvelope(kind, participant, epoch,
          marker.getMeta().getBlockHash(), marker.getBatchId(),
          marker.getHistoryLocation().getBodyDigest(), participants);
    }

    private void store(Path path, ArchiveProgressEnvelope envelope) throws IOException {
      new ArchiveProgressFile(path, codec).store(envelope);
    }

    private HistoryCommitMarker marker(int epoch) {
      BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1),
          epoch * 1_000L);
      HistoryLocation body = new HistoryLocation(0, epoch * 100L, 100, epoch,
          bytes(32, epoch + 20));
      HistoryIndexLocation index = new HistoryIndexLocation(epoch * 50L, 50,
          bytes(32, epoch + 30));
      return new HistoryCommitMarker(meta, epoch - 1L, body, index,
          bytes(16, epoch + 40), participants);
    }

    @Override
    public void close() throws IOException {
      history.close();
    }
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
