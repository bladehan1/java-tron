package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;

public class ArchiveMixedEngineProgressSourceTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void mixedEnginesDriveFreshRecoveryAndReaderPublication() throws Exception {
    try (Fixture fixture = fixture()) {
      fixture.apply(fixture.marker(1));
      RecoverySnapshot snapshot = fixture.scanner(fixture.sources()).scan();
      assertEquals(1, snapshot.getHistoryHead());
      assertEquals(1, snapshot.getCheckpointHead());
      assertEquals(Long.valueOf(1), snapshot.getParticipantHeads().get("account"));
      assertEquals(Long.valueOf(1), snapshot.getParticipantHeads().get("account-asset"));
      assertEquals(0, snapshot.getReaderVisibleHead());

      fixture.gate(fixture.sources()).publish(1);
      assertEquals(1, fixture.reader().getEpoch());
    }
  }

  @Test
  public void sourceSetMismatchFailsBeforeReadingEitherEngine() throws Exception {
    try (Fixture fixture = fixture()) {
      AtomicInteger reads = new AtomicInteger();
      Map<String, ArchiveParticipantProgressSource> missing = new LinkedHashMap<>();
      missing.put("account", () -> {
        reads.incrementAndGet();
        return fixture.progress("account", fixture.marker(1));
      });

      assertThrows(IllegalArgumentException.class, () -> fixture.scanner(missing));
      assertThrows(IllegalArgumentException.class, () -> fixture.gate(missing));
      assertEquals(0, reads.get());
      assertEquals(0, fixture.reader().getEpoch());
    }
  }

  @Test
  public void identityAndPartialReadFailureNeverAdvanceReader() throws Exception {
    try (Fixture fixture = fixture()) {
      HistoryCommitMarker target = fixture.marker(1);
      ArchiveProgressEnvelope wrongHash = new ArchiveProgressEnvelope(
          Kind.PARTICIPANT_PROGRESS, "account", 1, bytes(32, 99), target.getBatchId(),
          target.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
      fixture.level.apply(Collections.emptyList(), wrongHash);
      fixture.rocks.apply(Collections.emptyList(), fixture.progress("account-asset", target));
      assertThrows(ArchivePersistenceException.class,
          () -> fixture.scanner(fixture.sources()).scan());
      assertThrows(ArchivePersistenceException.class,
          () -> fixture.gate(fixture.sources()).publish(1));
      assertEquals(0, fixture.reader().getEpoch());

      fixture.apply(target);
      Map<String, ArchiveParticipantProgressSource> partial = fixture.sources();
      partial.put("account-asset", () -> {
        throw new IOException("injected mixed-engine progress read failure");
      });
      assertThrows(IOException.class, () -> fixture.scanner(partial).scan());
      assertThrows(IOException.class, () -> fixture.gate(partial).publish(1));
      assertEquals(0, fixture.reader().getEpoch());
    }
  }

  private Fixture fixture() throws Exception {
    return new Fixture(temporaryFolder.newFolder().toPath());
  }

  private static final class Fixture implements AutoCloseable {
    private final HistoryCommitStore history;
    private final Path checkpointPath;
    private final Path readerPath;
    private final LevelDbArchiveParticipant level;
    private final RocksDbArchiveParticipant rocks;
    private final ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();

    private Fixture(Path directory) throws Exception {
      history = new HistoryCommitStore(directory, new HistoryCommitMarkerCodec());
      history.commitAll(Arrays.asList(marker(0), marker(1)));
      checkpointPath = directory.resolve("progress/checkpoint.progress");
      readerPath = directory.resolve("progress/reader.progress");
      new ArchiveProgressFile(checkpointPath, codec).store(
          progress(Kind.APPLY_CHECKPOINT, null, marker(1)));
      new ArchiveProgressFile(readerPath, codec).store(
          progress(Kind.READER_VISIBLE, null, marker(0)));
      level = new LevelDbArchiveParticipant(
          directory.resolve("account-level"), "account", PARTICIPANTS);
      rocks = new RocksDbArchiveParticipant(
          directory.resolve("asset-rocks"), "account-asset", PARTICIPANTS);
    }

    private void apply(HistoryCommitMarker marker) throws IOException {
      level.apply(Collections.emptyList(), progress("account", marker));
      rocks.apply(Collections.emptyList(), progress("account-asset", marker));
    }

    private Map<String, ArchiveParticipantProgressSource> sources() {
      Map<String, ArchiveParticipantProgressSource> sources = new LinkedHashMap<>();
      sources.put("account", level);
      sources.put("account-asset", rocks);
      return sources;
    }

    private ArchiveRecoveryAuthorityScanner scanner(
        Map<String, ? extends ArchiveParticipantProgressSource> sources) {
      return ArchiveRecoveryAuthorityScanner.forParticipants(history, checkpointPath, sources,
          readerPath, PARTICIPANTS);
    }

    private ArchiveReaderPublicationGate gate(
        Map<String, ? extends ArchiveParticipantProgressSource> sources) {
      return new ArchiveReaderPublicationGate(history,
          () -> new ArchiveProgressFile(checkpointPath, codec).load(), sources,
          readerPath, PARTICIPANTS, action -> action.run());
    }

    private ArchiveProgressEnvelope reader() throws IOException {
      return new ArchiveProgressFile(readerPath, codec).load();
    }

    private ArchiveProgressEnvelope progress(String participant, HistoryCommitMarker marker) {
      return progress(Kind.PARTICIPANT_PROGRESS, participant, marker);
    }

    private ArchiveProgressEnvelope progress(Kind kind, String participant,
        HistoryCommitMarker marker) {
      return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
          marker.getMeta().getBlockHash(), marker.getBatchId(),
          marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
    }

    private HistoryCommitMarker marker(long epoch) {
      BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, bytes(32, (int) epoch),
          bytes(32, (int) epoch - 1), epoch * 1_000);
      HistoryLocation body = new HistoryLocation(0, epoch * 100, 100, (int) epoch,
          bytes(32, (int) epoch + 20));
      HistoryIndexLocation index = new HistoryIndexLocation(epoch * 50, 50,
          bytes(32, (int) epoch + 30));
      return new HistoryCommitMarker(meta, epoch - 1, body, index,
          bytes(16, (int) epoch + 40), new ArrayList<>(PARTICIPANTS));
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      try {
        rocks.close();
      } catch (RuntimeException closeFailure) {
        failure = new IOException("Failed to close RocksDB participant", closeFailure);
      }
      try {
        level.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
      history.close();
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
