package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
  public void ignoresUncommittedTemporaryMarkerFiles() throws Exception {
    Path archive = temporaryFolder.newFolder("temporary-marker").toPath();
    Files.createDirectories(archive.resolve("commits"));
    Files.write(archive.resolve("commits").resolve(".tmp-interrupted"), new byte[]{1, 2, 3});
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096, databases())) {
      assertNull(writer.committedHead());
      assertThrows(IllegalArgumentException.class, () -> writer.readCommitted(1));
    }
  }

  @Test
  public void markerDirectorySyncFailurePreservesBodyAndIndexAsUncertain() throws Exception {
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
      try (java.util.stream.Stream<Path> paths = Files.list(archive.resolve("commits"))) {
        assertEquals(1, paths
            .filter(path -> path.getFileName().toString().endsWith(".commit"))
            .count());
      }
      assertThrows(IllegalStateException.class,
          () -> commits.removeHead(marker.getMeta()));
    }
  }

  private static Set<String> databases() {
    return new java.util.LinkedHashSet<>(Arrays.asList("account", "properties"));
  }

  private static BlockReverseDiff diff(int number) {
    return new BlockReverseDiff(new BlockSnapshotMeta(number, number, hash(number),
        hash(number - 1), number * 3_000L), Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key-" + number), OldValue.present(bytes("value-" + number)))))));
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
