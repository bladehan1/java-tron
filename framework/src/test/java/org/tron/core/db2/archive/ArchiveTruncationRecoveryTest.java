package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveTruncationRecovery.Stage;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class ArchiveTruncationRecoveryTest {

  private static final List<String> PARTICIPANTS = Collections.singletonList("account");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void preparedIntentRecoversBeforeCommitShrinkWithoutEpochZeroScan() throws Exception {
    Path archive = temporaryFolder.newFolder("before-commit").toPath();
    initialize(archive);
    prepare(archive);
    assertHeads(archive, 12, 12, 12, 12);

    assertTrue(new ArchiveTruncationRecovery(archive, 4096).recover());
    assertRecovered(archive);
    assertFalse(new ArchiveTruncationRecovery(archive, 4096).recover());
  }

  @Test
  public void everyPostIntentCrashUsesIntentAndConvergesToTargetCheckpoint() throws Exception {
    for (Stage failedStage : Stage.values()) {
      Path archive = temporaryFolder.newFolder("intent-" + failedStage).toPath();
      initialize(archive);
      prepare(archive);
      ArchiveTruncationRecovery recovery = new ArchiveTruncationRecovery(archive, 4096,
          stage -> {
            if (stage == failedStage) {
              throw new IOException("injected after " + stage);
            }
          });
      assertThrows(IOException.class, recovery::recover);

      ArchiveHistoryScanAnchor checkpoint = ArchiveHistoryScanAnchor.load(archive,
          new HistoryCommitMarkerCodec());
      long expectedCheckpoint = failedStage == Stage.COMMIT_SHRUNK ? 12 : 10;
      assertEquals(expectedCheckpoint, checkpoint.getMarker().getMeta().getEpoch());

      assertTrue(new ArchiveTruncationRecovery(archive, 4096).recover());
      assertRecovered(archive);
    }
  }

  @Test
  public void intentPreReplaceCrashNeverShrinksCommittedAuthority() throws Exception {
    Path archive = temporaryFolder.newFolder("intent-pre-replace").toPath();
    initialize(archive);
    ArchiveHistoryScanAnchor checkpoint = ArchiveHistoryScanAnchor.load(archive,
        new HistoryCommitMarkerCodec());
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096, checkpoint);
        HistoryIndexStore index = new HistoryIndexStore(
            archive, new HistoryIndexCodec(), checkpoint);
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec(), checkpoint)) {
      assertThrows(IOException.class, () -> ArchiveTruncationIntent.prepare(archive,
          commits, index, bodies, 10, new HistoryCommitMarkerCodec(), temporary -> {
          throw new IOException("injected before intent replace");
        }));
    }
    assertNull(ArchiveTruncationIntent.load(archive, new HistoryCommitMarkerCodec()));
    assertFalse(new ArchiveTruncationRecovery(archive, 4096).recover());
    assertHeads(archive, 12, 12, 12, 12);
  }

  @Test
  public void corruptIntentFailsBeforeCommitShrink() throws Exception {
    Path archive = temporaryFolder.newFolder("corrupt-intent").toPath();
    initialize(archive);
    prepare(archive);
    Path intentPath = archive.resolve("truncation.intent");
    byte[] corrupt = Files.readAllBytes(intentPath);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(intentPath, corrupt);

    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveTruncationRecovery(archive, 4096).recover());
    assertHeads(archive, 12, 12, 12, 12);
  }

  private static void initialize(Path archive) throws Exception {
    HistoryCommitMarker head;
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      List<HistoryCommitMarker> markers = new ArrayList<>();
      for (long epoch = 8; epoch <= 12; epoch++) {
        BlockReverseDiff diff = diff(epoch);
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(diff.getMeta(), epoch - 1, body, indexLocation,
            bytes(16, (int) epoch + 40), PARTICIPANTS));
      }
      bodies.sync();
      index.sync();
      commits.commitAll(markers);
      head = commits.head();
      ArchiveHistoryScanAnchor.persist(archive, commits.firstEpoch(), commits.size(),
          commits.getRecordLength(), head, new HistoryCommitMarkerCodec());
    }
  }

  private static void prepare(Path archive) throws Exception {
    ArchiveHistoryScanAnchor checkpoint = ArchiveHistoryScanAnchor.load(archive,
        new HistoryCommitMarkerCodec());
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096, checkpoint);
        HistoryIndexStore index = new HistoryIndexStore(
            archive, new HistoryIndexCodec(), checkpoint);
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec(), checkpoint)) {
      ArchiveTruncationIntent.prepare(archive, commits, index, bodies, 10,
          new HistoryCommitMarkerCodec());
    }
  }

  private static void assertRecovered(Path archive) throws Exception {
    assertNull(ArchiveTruncationIntent.load(archive, new HistoryCommitMarkerCodec()));
    assertHeads(archive, 10, 10, 10, 10);
  }

  private static void assertHeads(Path archive, long checkpointEpoch, long commitEpoch,
      long indexEpoch, long bodyEpoch) throws Exception {
    ArchiveHistoryScanAnchor checkpoint = ArchiveHistoryScanAnchor.load(archive,
        new HistoryCommitMarkerCodec());
    assertEquals(checkpointEpoch, checkpoint.getMarker().getMeta().getEpoch());
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096, checkpoint);
        HistoryIndexStore index = new HistoryIndexStore(
            archive, new HistoryIndexCodec(), checkpoint);
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec(), checkpoint)) {
      assertEquals(commitEpoch, commits.head().getMeta().getEpoch());
      assertEquals(indexEpoch, index.getScanResult().getHead().getRecord().getMeta().getEpoch());
      assertEquals(bodyEpoch, bodies.getScanResult().getHead().getDiff().getMeta().getEpoch());
      if (checkpointEpoch == 10 && commitEpoch == 10 && indexEpoch == 10 && bodyEpoch == 10) {
        assertEquals(1, commits.getStartupScannedRecords());
        assertEquals(1, index.getStartupScannedRecords());
        assertEquals(1, bodies.getStartupScannedRecords());
      }
    }
  }

  private static BlockReverseDiff diff(long epoch) {
    return new BlockReverseDiff(new BlockSnapshotMeta(epoch, epoch,
        bytes(32, (int) epoch), bytes(32, (int) epoch - 1), epoch * 1_000),
        Collections.singletonList(new DbGroup("account", Collections.singletonList(
            new Entry(bytes(8, (int) epoch), OldValue.present(bytes(12, (int) epoch)))))));
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
