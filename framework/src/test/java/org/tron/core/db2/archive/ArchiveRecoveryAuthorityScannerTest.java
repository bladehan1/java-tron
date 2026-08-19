package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoveryStorage;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;

public class ArchiveRecoveryAuthorityScannerTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void composesFreshFileAuthoritiesWithExecutorScan() throws Exception {
    try (Fixture fixture = fixture()) {
      RecordingStorage storage = new RecordingStorage(fixture.scanner);
      RecoveryPlan plan = new ArchiveRecoveryExecutor(storage).recover();

      assertEquals(Arrays.asList("truncate:10", "replay:account-asset:9-10", "publish:10"),
          storage.actions);
      assertEquals(3, plan.getActions().size());
      assertEquals(8, plan.getSafeHeadBeforeRecovery());
    }
  }

  @Test
  public void corruptEnvelopeFailsBeforeFirstRecoveryAction() throws Exception {
    try (Fixture fixture = fixture()) {
      byte[] corrupt = Files.readAllBytes(fixture.participantPaths.get("account-asset"));
      corrupt[corrupt.length - 1] ^= 1;
      Files.write(fixture.participantPaths.get("account-asset"), corrupt);
      RecordingStorage storage = new RecordingStorage(fixture.scanner);

      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(storage).recover());
      assertEquals(0, storage.actions.size());
    }
  }

  @Test
  public void missingOrMismatchedIdentityFailsBeforeFirstRecoveryAction() throws Exception {
    try (Fixture missing = fixture()) {
      Files.delete(missing.checkpointPath);
      RecordingStorage storage = new RecordingStorage(missing.scanner);
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(storage).recover());
      assertEquals(0, storage.actions.size());
    }

    try (Fixture mismatch = fixture()) {
      HistoryCommitMarker marker = mismatch.history.get(8);
      new ArchiveProgressFile(mismatch.participantPaths.get("account-asset"),
          new ArchiveProgressEnvelopeCodec()).store(new ArchiveProgressEnvelope(
              Kind.PARTICIPANT_PROGRESS, "account-asset", 8,
              marker.getMeta().getBlockHash(), bytes(16, 99),
              marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS));
      RecordingStorage storage = new RecordingStorage(mismatch.scanner);
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(storage).recover());
      assertEquals(0, storage.actions.size());
    }
  }

  @Test
  public void readerPublishCrashPreservesOldAuthorityAndSecondRecoveryResumes()
      throws Exception {
    try (Fixture fixture = fixture()) {
      ArchiveReaderHeadPublisher failingPublisher = new ArchiveReaderHeadPublisher(
          fixture.history, fixture.readerVisiblePath, PARTICIPANTS, temporary -> {
        throw new IOException("injected after reader temporary force");
      });
      DurableStorage first = new DurableStorage(fixture, failingPublisher);
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(first).recover());
      assertEquals(Arrays.asList("truncate:10", "replay:account-asset:9-10"), first.actions);
      assertEquals(8, new ArchiveProgressFile(fixture.readerVisiblePath,
          new ArchiveProgressEnvelopeCodec()).load().getEpoch());

      RecoverySnapshot afterCrash = fixture.scanner.scan();
      assertEquals(10, afterCrash.getHistoryHead());
      assertEquals(Long.valueOf(10),
          afterCrash.getParticipantHeads().get("account-asset"));
      assertEquals(8, afterCrash.getReaderVisibleHead());

      ArchiveReaderHeadPublisher publisher = new ArchiveReaderHeadPublisher(
          fixture.history, fixture.readerVisiblePath, PARTICIPANTS);
      DurableStorage second = new DurableStorage(fixture, publisher);
      new ArchiveRecoveryExecutor(second).recover();
      assertEquals(Arrays.asList("publish:10"), second.actions);
      assertEquals(10, fixture.scanner.scan().getReaderVisibleHead());

      DurableStorage third = new DurableStorage(fixture, publisher);
      assertEquals(0, new ArchiveRecoveryExecutor(third).recover().getActions().size());
      assertEquals(0, third.actions.size());
    }
  }

  private Fixture fixture() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    HistoryCommitStore history = new HistoryCommitStore(directory,
        new HistoryCommitMarkerCodec());
    List<HistoryCommitMarker> markers = new ArrayList<>();
    for (long epoch = 8; epoch <= 12; epoch++) {
      markers.add(marker(epoch));
    }
    history.commitAll(markers);

    Path checkpointPath = directory.resolve("progress/checkpoint.progress");
    Map<String, Path> participantPaths = new LinkedHashMap<>();
    participantPaths.put("account", directory.resolve("progress/account.progress"));
    participantPaths.put("account-asset",
        directory.resolve("progress/account-asset.progress"));
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(checkpointPath, codec).store(
        envelope(Kind.APPLY_CHECKPOINT, null, history.get(10)));
    new ArchiveProgressFile(participantPaths.get("account"), codec).store(
        envelope(Kind.PARTICIPANT_PROGRESS, "account", history.get(10)));
    new ArchiveProgressFile(participantPaths.get("account-asset"), codec).store(
        envelope(Kind.PARTICIPANT_PROGRESS, "account-asset", history.get(8)));
    Path readerVisiblePath = directory.resolve("progress/reader-visible.progress");
    new ArchiveProgressFile(readerVisiblePath, codec).store(
        envelope(Kind.READER_VISIBLE, null, history.get(8)));
    ArchiveRecoveryAuthorityScanner scanner = new ArchiveRecoveryAuthorityScanner(history,
        checkpointPath, participantPaths, readerVisiblePath, PARTICIPANTS);
    return new Fixture(history, scanner, checkpointPath, participantPaths, readerVisiblePath);
  }

  private static ArchiveProgressEnvelope envelope(Kind kind, String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), marker.getDatabases());
  }

  private static HistoryCommitMarker marker(long epoch) {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, bytes(32, (int) epoch),
        bytes(32, (int) epoch - 1), epoch * 1_000);
    HistoryLocation body = new HistoryLocation(0, epoch * 100, 100, (int) epoch,
        bytes(32, (int) epoch + 20));
    HistoryIndexLocation index = new HistoryIndexLocation(epoch * 50, 50,
        bytes(32, (int) epoch + 30));
    return new HistoryCommitMarker(meta, epoch - 1, body, index,
        bytes(16, (int) epoch + 40), PARTICIPANTS);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static final class RecordingStorage implements RecoveryStorage {
    private final ArchiveRecoveryAuthorityScanner scanner;
    private final List<String> actions = new ArrayList<>();

    private RecordingStorage(ArchiveRecoveryAuthorityScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public RecoverySnapshot scan() throws IOException {
      return scanner.scan();
    }

    @Override
    public void truncateHistoryAndSync(long historyHead) {
      actions.add("truncate:" + historyHead);
    }

    @Override
    public void replayParticipantAndSyncProgress(String participant, long firstEpoch,
        long lastEpoch) {
      actions.add("replay:" + participant + ":" + firstEpoch + "-" + lastEpoch);
    }

    @Override
    public void publishReaderHeadAndSync(long readerVisibleHead) {
      actions.add("publish:" + readerVisibleHead);
    }
  }

  private static final class DurableStorage implements RecoveryStorage {
    private final Fixture fixture;
    private final ArchiveReaderHeadPublisher publisher;
    private final List<String> actions = new ArrayList<>();

    private DurableStorage(Fixture fixture, ArchiveReaderHeadPublisher publisher) {
      this.fixture = fixture;
      this.publisher = publisher;
    }

    @Override
    public RecoverySnapshot scan() throws IOException {
      return fixture.scanner.scan();
    }

    @Override
    public void truncateHistoryAndSync(long historyHead) throws IOException {
      HistoryCommitMarker head = fixture.history.head();
      while (head != null && head.getMeta().getEpoch() > historyHead) {
        fixture.history.removeHead(head.getMeta());
        head = fixture.history.head();
      }
      actions.add("truncate:" + historyHead);
    }

    @Override
    public void replayParticipantAndSyncProgress(String participant, long firstEpoch,
        long lastEpoch) throws IOException {
      new ArchiveProgressFile(fixture.participantPaths.get(participant),
          new ArchiveProgressEnvelopeCodec()).store(envelope(
              Kind.PARTICIPANT_PROGRESS, participant, fixture.history.get(lastEpoch)));
      actions.add("replay:" + participant + ":" + firstEpoch + "-" + lastEpoch);
    }

    @Override
    public void publishReaderHeadAndSync(long readerVisibleHead) throws IOException {
      publisher.publish(readerVisibleHead);
      actions.add("publish:" + readerVisibleHead);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final HistoryCommitStore history;
    private final ArchiveRecoveryAuthorityScanner scanner;
    private final Path checkpointPath;
    private final Map<String, Path> participantPaths;
    private final Path readerVisiblePath;

    private Fixture(HistoryCommitStore history, ArchiveRecoveryAuthorityScanner scanner,
        Path checkpointPath, Map<String, Path> participantPaths, Path readerVisiblePath) {
      this.history = history;
      this.scanner = scanner;
      this.checkpointPath = checkpointPath;
      this.participantPaths = participantPaths;
      this.readerVisiblePath = readerVisiblePath;
    }

    @Override
    public void close() throws IOException {
      history.close();
    }
  }
}
