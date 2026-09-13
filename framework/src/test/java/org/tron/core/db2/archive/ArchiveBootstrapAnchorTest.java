package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.core.SnapshotManager;

public class ArchiveBootstrapAnchorTest {

  private static final int MAGIC = 0x54414241;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void independentAnchorSurvivesSecondRestartWithoutLegacyPaths() throws Exception {
    Path archive = bootstrap("second-restart", 123);
    byte[] encoded = Files.readAllBytes(archive.resolve("bootstrap.anchor"));

    assertEquals(MAGIC, ByteBuffer.wrap(encoded).getInt());
    assertFalse(Files.exists(archive.resolve("participants")));
    assertFalse(Files.exists(archive.resolve("progress")));
    assertNoAnchorTemporary(archive);
    for (int restart = 0; restart < 2; restart++) {
      try (StateArchiveRuntimeOwner owner = StateArchiveRuntimeOwner.recover(
          new SnapshotManager(""), archive, 4096)) {
        assertEquals(123, owner.getRecoveredHead().getEpoch());
      }
      assertArrayEquals(encoded, Files.readAllBytes(archive.resolve("bootstrap.anchor")));
    }
  }

  @Test
  public void checksumCorruptionAndSubstitutedMarkerFailClosed() throws Exception {
    Path corrupt = bootstrap("corrupt", 123);
    byte[] bytes = Files.readAllBytes(corrupt.resolve("bootstrap.anchor"));
    bytes[bytes.length - 1] ^= 1;
    Files.write(corrupt.resolve("bootstrap.anchor"), bytes);
    assertThrows(ArchivePersistenceException.class,
        () -> openHistory(corrupt));

    Path target = bootstrap("target", 123);
    Path foreign = bootstrap("foreign", 124);
    Files.copy(foreign.resolve("bootstrap.anchor"), target.resolve("bootstrap.anchor"),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    assertThrows(ArchivePersistenceException.class,
        () -> openHistory(target));
  }

  @Test
  public void rejectsLegacyProgressEnvelopeAndWrongStoreScope() throws Exception {
    Path archive = bootstrap("legacy", 123);
    HistoryCommitMarker marker;
    try (ArchiveHistoryWriter writer = openHistory(archive)) {
      marker = writer.committedHead();
    }
    List<String> stores = stores();
    ArchiveProgressEnvelope legacy = new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), new byte[32], stores);
    Files.write(archive.resolve("bootstrap.anchor"),
        new ArchiveProgressEnvelopeCodec().encode(legacy));
    assertThrows(ArchivePersistenceException.class,
        () -> openHistory(archive));

    List<String> incomplete = new ArrayList<>(stores);
    incomplete.remove(incomplete.size() - 1);
    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveBootstrapAnchor.store(archive, marker, incomplete));
  }

  private Path bootstrap(String name, int epoch) throws Exception {
    Path archive = temporaryFolder.newFolder(name).toPath().resolve("state-archive");
    BlockSnapshotMeta head = BlockSnapshotMeta.forBlock(epoch, hash(epoch), hash(epoch - 1),
        epoch * 1_000L);
    try (StateArchiveRuntimeOwner ignored = StateArchiveRuntimeOwner.bootstrapAndRecover(
        new SnapshotManager(""), archive, 4096, head)) {
      return archive;
    }
  }

  private static ArchiveHistoryWriter openHistory(Path archive) throws Exception {
    return new ArchiveHistoryWriter(archive, 4096, ArchiveStoreScope.getStateDatabases());
  }

  private static void assertNoAnchorTemporary(Path archive) throws Exception {
    try (java.util.stream.Stream<Path> entries = Files.list(archive)) {
      assertFalse(entries.anyMatch(path -> path.getFileName().toString()
          .startsWith(".bootstrap.anchor-")));
    }
  }

  private static List<String> stores() {
    List<String> stores = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(stores);
    return stores;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }
}
