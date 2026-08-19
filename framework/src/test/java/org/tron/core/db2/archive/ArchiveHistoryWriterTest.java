package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveHistoryWriter.Stage;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.protos.Protocol.Account;

public class ArchiveHistoryWriterTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void makesHistoryVisibleOnlyAfterOrderedDurabilityStages() throws Exception {
    Path archive = temporaryFolder.newFolder("writer").toPath();
    List<Stage> stages = new ArrayList<>();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
        databases(), (stage, meta) -> stages.add(stage))) {
      writer.accept(diff(1));
      assertEquals(Arrays.asList(Stage.APPEND_BODY, Stage.APPEND_INDEX, Stage.SYNC_BODY,
          Stage.SYNC_INDEX, Stage.COMMIT_MARKER), stages);
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
      assertEquals(0, writer.committedHead().getPreviousEpoch());
      assertEquals(Arrays.asList("account", "properties"),
          writer.committedHead().getDatabases());
      assertEquals(16, writer.committedHead().getBatchId().length);
      assertEquals(diff(1).getMeta(), writer.readCommitted(1).getMeta());

      writer.accept(diff(2));
      assertEquals(2, writer.committedHead().getMeta().getEpoch());
      writer.revert(diff(2).getMeta());
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
      assertThrows(IllegalArgumentException.class, () -> writer.readCommitted(2));
    }

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(
        archive, 4096, databases())) {
      assertEquals(1, reopened.committedHead().getMeta().getEpoch());
      assertEquals(diff(1).getMeta(), reopened.readCommitted(1).getMeta());
    }
  }

  @Test
  public void rollsBackPreparedSuffixAtEveryPreCommitFailure() throws Exception {
    for (Stage failedStage : Stage.values()) {
      Path archive = temporaryFolder.newFolder("failure-" + failedStage).toPath();
      ArchiveHistoryWriter.DurabilityHook hook = (stage, meta) -> {
        if (stage == failedStage) {
          throw new java.io.IOException("injected " + stage);
        }
      };
      try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
          databases(), hook)) {
        assertThrows(ArchivePersistenceException.class, () -> writer.accept(diff(1)));
        assertNull(writer.committedHead());
      }
      try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(
          archive, 4096, databases())) {
        assertNull(reopened.committedHead());
        assertThrows(IllegalArgumentException.class, () -> reopened.readCommitted(1));
      }
    }
  }

  @Test
  public void truncatesCrashLeftPreparedBodyAndIndexOnOpen() throws Exception {
    Path archive = temporaryFolder.newFolder("prepared").toPath();
    BlockReverseDiff diff = diff(1);
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec())) {
      HistoryLocation body = bodies.append(diff);
      index.append(HistoryIndexRecord.from(diff, body));
      bodies.sync();
      index.sync();
    }

    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertNull(writer.committedHead());
      writer.accept(diff);
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void rejectsNonContiguousCanonicalInput() throws Exception {
    Path archive = temporaryFolder.newFolder("continuity").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.accept(diff(1));
      BlockReverseDiff gap = new BlockReverseDiff(new BlockSnapshotMeta(
          3, 3, hash(3), hash(2), 3L), Collections.emptyList());
      assertThrows(ArchivePersistenceException.class, () -> writer.accept(gap));
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void persistsAccountSeekIndexAndRecoversItAcrossRestart() throws Exception {
    Path archive = temporaryFolder.newFolder("account-index").toPath();
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = 7;
    BlockReverseDiff created = accountDiff(1, address, OldValue.absent());
    BlockReverseDiff changed = accountDiff(2, address,
        OldValue.present(account(address, 10)));
    BlockReverseDiff unchanged = new BlockReverseDiff(new BlockSnapshotMeta(
        3, 3, hash(3), hash(2), 9_000), Collections.emptyList());
    byte[] committedAccount = account(address, 20);

    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.acceptAll(Arrays.asList(created, changed, unchanged));
      assertFalse(writer.readAccountAt(0, address, committedAccount).isPresent());
      assertEquals(10, Account.parseFrom(
          writer.readAccountAt(1, address, committedAccount).getValue()).getBalance());
      assertEquals(20, Account.parseFrom(
          writer.readAccountAt(2, address, committedAccount).getValue()).getBalance());
    }

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(20, Account.parseFrom(
          reopened.readAccountAt(2, address, committedAccount).getValue()).getBalance());
      try (java.util.stream.Stream<Path> files = Files.list(archive.resolve("commits"))) {
        assertEquals(1, files.count());
      }
    }
  }

  @Test
  public void truncatesIncompleteCommitLogTailOnRestart() throws Exception {
    Path archive = temporaryFolder.newFolder("temporary-marker").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.accept(diff(1));
    }
    Files.write(archive.resolve("commits").resolve("commit.log"), new byte[]{1, 2, 3},
        java.nio.file.StandardOpenOption.APPEND);
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(1, writer.committedHead().getMeta().getEpoch());
      assertEquals(diff(1).getMeta(), writer.readCommitted(1).getMeta());
    }
  }

  @Test
  public void persistsBatchedPrefixWithoutPerBlockFilesAndResumes() throws Exception {
    Path archive = temporaryFolder.newFolder("batched-prefix").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      for (int start = 1; start <= 1_000; start += 100) {
        List<BlockReverseDiff> batch = new ArrayList<>(100);
        for (int number = start; number < start + 100; number++) {
          batch.add(diff(number));
        }
        writer.acceptAll(batch);
      }
      assertEquals(1_000, writer.committedHead().getMeta().getEpoch());
    }

    try (java.util.stream.Stream<Path> commits = Files.list(archive.resolve("commits"));
        java.util.stream.Stream<Path> segments = Files.list(archive.resolve("history"))) {
      assertEquals(1, commits.count());
      assertTrue(segments.count() < 1_000);
    }
    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(1_000, reopened.committedHead().getMeta().getEpoch());
      assertEquals(3, reopened.getStartupScannedRecords());
      assertEquals(diff(500).getMeta(), reopened.readCommitted(500).getMeta());
      reopened.accept(diff(1_001));
      assertEquals(1_001, reopened.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void scansOnlyTailAfterAStaleRestartCheckpoint() throws Exception {
    Path archive = temporaryFolder.newFolder("stale-checkpoint").toPath();
    byte[] checkpointAtOne;
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.accept(diff(1));
      checkpointAtOne = Files.readAllBytes(archive.resolve("restart.checkpoint"));
      writer.accept(diff(2));
    }
    Files.write(archive.resolve("restart.checkpoint"), checkpointAtOne);

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(2, reopened.committedHead().getMeta().getEpoch());
      assertEquals(6, reopened.getStartupScannedRecords());
      assertEquals(diff(2).getMeta(), reopened.readCommitted(2).getMeta());
    }
  }

  @Test
  public void truncatesInvalidBodyAndIndexTailWithoutRescanningPrefix() throws Exception {
    Path archive = temporaryFolder.newFolder("invalid-data-tail").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      List<BlockReverseDiff> batch = new ArrayList<>(1_000);
      for (int number = 1; number <= 1_000; number++) {
        batch.add(diff(number));
      }
      writer.acceptAll(batch);
    }
    Path lastSegment;
    try (java.util.stream.Stream<Path> segments = Files.list(archive.resolve("history"))) {
      lastSegment = segments.sorted().reduce((left, right) -> right)
          .orElseThrow(AssertionError::new);
    }
    Files.write(lastSegment, new byte[]{1, 2, 3},
        java.nio.file.StandardOpenOption.APPEND);
    Files.write(archive.resolve("state_history.idx"), new byte[]{1, 2, 3},
        java.nio.file.StandardOpenOption.APPEND);

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(1_000, reopened.committedHead().getMeta().getEpoch());
      assertEquals(3, reopened.getStartupScannedRecords());
      reopened.accept(diff(1_001));
      assertEquals(1_001, reopened.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void boundsPreparedTailAcrossAnOversizedFlushFailure() throws Exception {
    Path archive = temporaryFolder.newFolder("bounded-large-flush").toPath();
    List<BlockReverseDiff> batch = new ArrayList<>(1_500);
    for (int number = 1; number <= 1_500; number++) {
      batch.add(diff(number));
    }
    ArchiveHistoryWriter.DurabilityHook failSecondChunk = (stage, meta) -> {
      if (stage == Stage.APPEND_BODY && meta.getEpoch() == 1_025) {
        throw new java.io.IOException("injected second chunk failure");
      }
    };
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases(),
        failSecondChunk)) {
      assertThrows(ArchivePersistenceException.class, () -> writer.acceptAll(batch));
      assertEquals(1_024, writer.committedHead().getMeta().getEpoch());
    }

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertEquals(1_024, reopened.committedHead().getMeta().getEpoch());
      assertEquals(3, reopened.getStartupScannedRecords());
    }
  }

  @Test
  public void failsClosedOnCorruptRestartCheckpoint() throws Exception {
    Path archive = temporaryFolder.newFolder("corrupt-checkpoint").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.accept(diff(1));
    }
    Path checkpoint = archive.resolve("restart.checkpoint");
    byte[] encoded = Files.readAllBytes(checkpoint);
    encoded[encoded.length - 1] ^= 1;
    Files.write(checkpoint, encoded);

    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveHistoryWriter(archive, 4096, databases()));
  }

  @Test
  public void completesPreparedTruncationBeforeLoadingRestartCheckpoint() throws Exception {
    Path archive = temporaryFolder.newFolder("writer-truncation-recovery").toPath();
    initializeHistory(archive, 3);
    prepareTruncation(archive, 2);

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(
        archive, 4096, databases())) {
      assertEquals(2, reopened.committedHead().getMeta().getEpoch());
      assertEquals(diff(2).getMeta(), reopened.readCommitted(2).getMeta());
      assertFalse(Files.exists(archive.resolve("truncation.intent")));
      assertEquals(3, reopened.getStartupScannedRecords());
    }
  }

  @Test
  public void failsClosedWhenDerivedAccountIndexIsAheadAfterRecovery() throws Exception {
    Path archive = temporaryFolder.newFolder("writer-index-ahead").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.acceptAll(Arrays.asList(diff(1), diff(2), diff(3)));
    }
    prepareTruncation(archive, 2);

    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveHistoryWriter(archive, 4096, databases()));
    ArchiveRestartCheckpoint checkpoint = ArchiveRestartCheckpoint.load(archive,
        new HistoryCommitMarkerCodec());
    assertEquals(2, checkpoint.getMarker().getMeta().getEpoch());
    assertFalse(Files.exists(archive.resolve("truncation.intent")));
  }

  @Test
  public void buildsPersistentServingGenerationFromCommittedWriterPrefix() throws Exception {
    Path archive = temporaryFolder.newFolder("writer-serving-generation").toPath();
    byte[] key = bytes("key-1");
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      writer.acceptAll(Arrays.asList(diff(1), diff(2)));
      try (PersistentServingKeyIndexGeneration generation = writer.buildServingGeneration(
          archive.resolve("serving-shadow"), "generation-2")) {
        assertEquals(2, generation.getIndexedThrough());
        assertEquals(1, generation.firstChangeAfter("account", key, 0, 2).getAsLong());
        assertFalse(generation.firstChangeAfter("properties", key, 0, 2).isPresent());
      }
    }
  }

  @Test
  public void commitLogForceBoundaryFailurePreservesRecordAsUncertain() throws Exception {
    Path archive = temporaryFolder.newFolder("uncertain-marker").toPath();
    HistoryCommitMarker marker = new HistoryCommitMarker(diff(1).getMeta(), 0,
        new HistoryLocation(0, 0, 100, 17, new byte[32]),
        new HistoryIndexLocation(0, 100, new byte[32]), new byte[16],
        new ArrayList<>(databases()));
    HistoryCommitStore.DirectorySync directorySync = directory -> {
      throw new java.io.IOException("injected directory sync failure");
    };
    try (HistoryCommitStore commits = new HistoryCommitStore(
        archive, new HistoryCommitMarkerCodec(), directorySync)) {
      assertThrows(java.io.IOException.class, () -> commits.commit(marker));
      assertNull(commits.head());
      assertTrue(commits.mayContain(1));
      assertTrue(Files.size(archive.resolve("commits").resolve("commit.log")) > 0);
      assertThrows(IllegalStateException.class,
          () -> commits.removeHead(marker.getMeta()));
    }
  }

  private static Set<String> databases() {
    return new java.util.LinkedHashSet<>(Arrays.asList("account", "properties"));
  }

  private static void initializeHistory(Path archive, int lastEpoch) throws Exception {
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      List<HistoryCommitMarker> markers = new ArrayList<>();
      for (int epoch = 1; epoch <= lastEpoch; epoch++) {
        BlockReverseDiff diff = diff(epoch);
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(diff.getMeta(), epoch - 1L, body, indexLocation,
            new byte[16], new ArrayList<>(databases())));
      }
      bodies.sync();
      index.sync();
      commits.commitAll(markers);
      ArchiveRestartCheckpoint.persist(archive, commits.firstEpoch(), commits.size(),
          commits.getRecordLength(), commits.head(), new HistoryCommitMarkerCodec());
    }
  }

  private static void prepareTruncation(Path archive, long targetEpoch) throws Exception {
    ArchiveRestartCheckpoint checkpoint = ArchiveRestartCheckpoint.load(archive,
        new HistoryCommitMarkerCodec());
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096, checkpoint);
        HistoryIndexStore index = new HistoryIndexStore(
            archive, new HistoryIndexCodec(), checkpoint);
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec(), checkpoint)) {
      ArchiveTruncationIntent.prepare(archive, commits, index, bodies, targetEpoch,
          new HistoryCommitMarkerCodec());
    }
  }

  private static BlockReverseDiff diff(int number) {
    return new BlockReverseDiff(new BlockSnapshotMeta(number, number, hash(number),
        hash(number - 1), number * 3_000L), Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key-" + number), OldValue.present(bytes("value-" + number)))))));
  }

  private static BlockReverseDiff accountDiff(int number, byte[] address, OldValue oldValue) {
    return new BlockReverseDiff(new BlockSnapshotMeta(number, number, hash(number),
        hash(number - 1), number * 3_000L), Collections.singletonList(
        new DbGroup("account", Collections.singletonList(new Entry(address, oldValue)))));
  }

  private static byte[] account(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address)).setBalance(balance)
        .build().toByteArray();
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}
