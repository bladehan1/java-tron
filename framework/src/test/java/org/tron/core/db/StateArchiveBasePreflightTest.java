package org.tron.core.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveHistoryWriter;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;

public class StateArchiveBasePreflightTest {

  private static final int MANIFEST_MAGIC = 0x54414d46;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void disabledControlDoesNotInspectOrCreateArchivePath() throws Exception {
    Path missingManifest = temporaryFolder.newFolder("disabled").toPath();
    byte[] evidence = new byte[]{1, 2, 3};
    Files.write(missingManifest.resolve("history.bin"), evidence);

    StateArchiveBasePreflight.requireAdmitted(false, missingManifest);

    assertArrayEquals(evidence, Files.readAllBytes(missingManifest.resolve("history.bin")));
    assertFalse(Files.exists(missingManifest.resolve("MANIFEST")));
  }

  @Test
  public void absentAndEmptyArchivesPassWithoutPreflightWrites() throws Exception {
    Path absent = temporaryFolder.getRoot().toPath().resolve("absent");
    StateArchiveBasePreflight.requireAdmitted(true, absent);
    assertFalse(Files.exists(absent));

    Path empty = temporaryFolder.newFolder("empty").toPath();
    StateArchiveBasePreflight.requireAdmitted(true, empty);
    try (java.util.stream.Stream<Path> entries = Files.list(empty)) {
      assertFalse(entries.findAny().isPresent());
    }
  }

  @Test
  public void recoverableGateRejectsFreshBaseWithoutCreatingBootstrapArtifacts() throws Exception {
    Path absent = temporaryFolder.getRoot().toPath().resolve("fresh-recovery");

    assertThrows(IllegalStateException.class,
        () -> StateArchiveBasePreflight.requireRecoverable(true, absent));

    assertFalse(Files.exists(absent));
  }

  @Test
  public void currentManifestPassesWithoutRewrite() throws Exception {
    Path archive = temporaryFolder.newFolder("current").toPath();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive,
        128L * 1024 * 1024, ArchiveStoreScope.getStateDatabases())) {
      writer.accept(new BlockReverseDiff(
          new BlockSnapshotMeta(1, 1, hash(1), hash(0), 1_000L), Collections.emptyList()));
    }
    byte[] before = Files.readAllBytes(archive.resolve("MANIFEST"));

    StateArchiveBasePreflight.requireAdmitted(true, archive);

    assertArrayEquals(before, Files.readAllBytes(archive.resolve("MANIFEST")));
  }

  @Test
  public void missingManifestFailsWithoutCreatingOrChangingEvidence() throws Exception {
    Path archive = temporaryFolder.newFolder("missing-manifest").toPath();
    byte[] evidence = new byte[]{4, 5, 6};
    Path history = archive.resolve("commit.log");
    Files.write(history, evidence);

    assertThrows(IllegalStateException.class,
        () -> StateArchiveBasePreflight.requireAdmitted(true, archive));

    assertArrayEquals(evidence, Files.readAllBytes(history));
    assertFalse(Files.exists(archive.resolve("MANIFEST")));
  }

  @Test
  public void staleExact26ScopeFailsWithoutManifestRewrite() throws Exception {
    Path archive = temporaryFolder.newFolder("stale-exact-26").toPath();
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    participants.remove("abi");
    byte[] stale = manifest("archive-state/exact-26-abi-tombstone/v1", participants);
    Path path = archive.resolve("MANIFEST");
    Files.write(path, stale);

    assertThrows(IllegalStateException.class,
        () -> StateArchiveBasePreflight.requireAdmitted(true, archive));

    assertArrayEquals(stale, Files.readAllBytes(path));
  }

  private static byte[] manifest(String scopeIdentity, List<String> participants)
      throws Exception {
    List<String> sorted = new ArrayList<>(participants);
    Collections.sort(sorted);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(MANIFEST_MAGIC);
    output.writeShort(2);
    output.writeShort(0);
    output.writeInt(0);
    writeString(output, scopeIdentity);
    output.writeLong(0);
    output.write(new byte[32]);
    output.writeInt(sorted.size());
    for (String participant : sorted) {
      writeString(output, participant);
    }
    output.flush();
    byte[] payload = bytes.toByteArray();
    ByteBuffer.wrap(payload).putInt(8, payload.length + Integer.BYTES);
    bytes.reset();
    output = new DataOutputStream(bytes);
    output.write(payload);
    output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
    output.flush();
    return bytes.toByteArray();
  }

  private static void writeString(DataOutputStream output, String value) throws Exception {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private static byte[] hash(long suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }
}
