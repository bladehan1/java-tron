package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
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
import org.tron.core.db2.archive.ArchiveParticipantBatchFile.Snapshot;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoveryStorage;

public class ArchiveParticipantBatchFileTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void crashKeepsBusinessPayloadAndProgressOnTheSameOldVersion() throws Exception {
    Path path = temporaryFolder.newFolder().toPath().resolve("account-asset.batch");
    ArchiveParticipantBatchFile normal = new ArchiveParticipantBatchFile(path,
        "account-asset", PARTICIPANTS);
    normal.store(bytes(12, 8), envelope("account-asset", marker(8)));

    ArchiveParticipantBatchFile failing = new ArchiveParticipantBatchFile(path,
        "account-asset", PARTICIPANTS, temporary -> {
      throw new IOException("injected after participant temporary force");
    });
    assertThrows(IOException.class,
        () -> failing.store(bytes(12, 10), envelope("account-asset", marker(10))));
    Snapshot old = normal.load();
    assertArrayEquals(bytes(12, 8), old.getBusinessPayload());
    assertEquals(8, old.getProgress().getEpoch());

    normal.store(bytes(12, 10), envelope("account-asset", marker(10)));
    Snapshot current = normal.load();
    assertArrayEquals(bytes(12, 10), current.getBusinessPayload());
    assertEquals(10, current.getProgress().getEpoch());

    byte[] corrupt = Files.readAllBytes(path);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(path, corrupt);
    assertThrows(ArchivePersistenceException.class, normal::load);
  }

  @Test
  public void replayCrashLeavesOldBatchAndSecondRestartReplaysOnce() throws Exception {
    try (Fixture fixture = fixture()) {
      ArchiveParticipantBatchFile failingAsset = new ArchiveParticipantBatchFile(
          fixture.assetPath, "account-asset", PARTICIPANTS, temporary -> {
        throw new IOException("injected replay batch crash");
      });
      DurableBatchStorage first = new DurableBatchStorage(fixture, failingAsset);
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveRecoveryExecutor(first).recover());
      assertEquals(Arrays.asList("truncate:10"), first.actions);
      Snapshot afterCrash = fixture.batches.get("account-asset").load();
      assertArrayEquals(bytes(12, 8), afterCrash.getBusinessPayload());
      assertEquals(8, afterCrash.getProgress().getEpoch());

      RecoverySnapshot restart = fixture.scanner.scan();
      assertEquals(10, restart.getHistoryHead());
      assertEquals(Long.valueOf(8), restart.getParticipantHeads().get("account-asset"));
      assertEquals(8, restart.getReaderVisibleHead());

      DurableBatchStorage second = new DurableBatchStorage(fixture,
          fixture.batches.get("account-asset"));
      new ArchiveRecoveryExecutor(second).recover();
      assertEquals(Arrays.asList("replay:account-asset:9-10", "publish:10"), second.actions);
      Snapshot recovered = fixture.batches.get("account-asset").load();
      assertArrayEquals(bytes(12, 10), recovered.getBusinessPayload());
      assertEquals(10, recovered.getProgress().getEpoch());
      assertEquals(10, fixture.scanner.scan().getReaderVisibleHead());

      DurableBatchStorage third = new DurableBatchStorage(fixture,
          fixture.batches.get("account-asset"));
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

    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    Path checkpointPath = directory.resolve("progress/checkpoint.progress");
    Path readerPath = directory.resolve("progress/reader.progress");
    new ArchiveProgressFile(checkpointPath, codec).store(globalEnvelope(
        Kind.APPLY_CHECKPOINT, history.get(10)));
    new ArchiveProgressFile(readerPath, codec).store(globalEnvelope(
        Kind.READER_VISIBLE, history.get(8)));

    Path accountPath = directory.resolve("participants/account.batch");
    Path assetPath = directory.resolve("participants/account-asset.batch");
    Map<String, ArchiveParticipantBatchFile> batches = new LinkedHashMap<>();
    batches.put("account", new ArchiveParticipantBatchFile(accountPath,
        "account", PARTICIPANTS));
    batches.put("account-asset", new ArchiveParticipantBatchFile(assetPath,
        "account-asset", PARTICIPANTS));
    batches.get("account").store(bytes(12, 10), envelope("account", history.get(10)));
    batches.get("account-asset").store(bytes(12, 8),
        envelope("account-asset", history.get(8)));

    ArchiveRecoveryAuthorityScanner scanner =
        ArchiveRecoveryAuthorityScanner.forParticipantBatches(history, checkpointPath,
            batches, readerPath, PARTICIPANTS);
    return new Fixture(history, scanner, batches, assetPath, readerPath);
  }

  private static ArchiveProgressEnvelope envelope(String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, participant,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), marker.getDatabases());
  }

  private static ArchiveProgressEnvelope globalEnvelope(Kind kind,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
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

  private static final class DurableBatchStorage implements RecoveryStorage {
    private final Fixture fixture;
    private final ArchiveParticipantBatchFile assetReplayBatch;
    private final List<String> actions = new ArrayList<>();

    private DurableBatchStorage(Fixture fixture,
        ArchiveParticipantBatchFile assetReplayBatch) {
      this.fixture = fixture;
      this.assetReplayBatch = assetReplayBatch;
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
      ArchiveParticipantBatchFile batch = "account-asset".equals(participant)
          ? assetReplayBatch : fixture.batches.get(participant);
      batch.store(bytes(12, (int) lastEpoch),
          envelope(participant, fixture.history.get(lastEpoch)));
      actions.add("replay:" + participant + ":" + firstEpoch + "-" + lastEpoch);
    }

    @Override
    public void publishReaderHeadAndSync(long readerVisibleHead) throws IOException {
      new ArchiveReaderHeadPublisher(fixture.history, fixture.readerPath, PARTICIPANTS)
          .publish(readerVisibleHead);
      actions.add("publish:" + readerVisibleHead);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final HistoryCommitStore history;
    private final ArchiveRecoveryAuthorityScanner scanner;
    private final Map<String, ArchiveParticipantBatchFile> batches;
    private final Path assetPath;
    private final Path readerPath;

    private Fixture(HistoryCommitStore history, ArchiveRecoveryAuthorityScanner scanner,
        Map<String, ArchiveParticipantBatchFile> batches, Path assetPath, Path readerPath) {
      this.history = history;
      this.scanner = scanner;
      this.batches = batches;
      this.assetPath = assetPath;
      this.readerPath = readerPath;
    }

    @Override
    public void close() throws IOException {
      history.close();
    }
  }
}
