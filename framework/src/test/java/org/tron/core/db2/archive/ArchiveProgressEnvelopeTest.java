package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

public class ArchiveProgressEnvelopeTest {

  private static final List<String> PARTICIPANTS = Arrays.asList(
      "account", "account-asset", "storage-row");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void deterministicallyRoundTripsCheckpointParticipantAndReaderProgress() {
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    ArchiveProgressEnvelope checkpoint = checkpoint(10, 1);
    byte[] first = codec.encode(checkpoint);
    byte[] second = codec.encode(checkpoint(10, 1));
    assertArrayEquals(first, second);
    assertEnvelope(checkpoint, codec.decode(first));

    ArchiveProgressEnvelope progress = progress("account-asset", 10, 1);
    assertEnvelope(progress, codec.decode(codec.encode(progress)));

    ArchiveProgressEnvelope reader = reader(10, 1);
    assertEnvelope(reader, codec.decode(codec.encode(reader)));

    ArchiveProgressEnvelope bound = new ArchiveProgressEnvelope(Kind.APPLY_CHECKPOINT, null,
        10, bytes(32, 1), bytes(16, 2), bytes(32, 3), bytes(32, 4), PARTICIPANTS);
    assertEnvelope(bound, codec.decode(codec.encode(bound)));
    assertEquals(ArchiveParticipantDescriptor.scopeIdentity(PARTICIPANTS),
        codec.decode(first).getScopeIdentity());

    List<String> approvedParticipants = ArchiveParticipantDescriptor.current().getParticipants();
    ArchiveProgressEnvelope approved = new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null,
        10, bytes(32, 1), bytes(16, 2), bytes(32, 3), approvedParticipants);
    assertEquals(ArchiveParticipantDescriptor.FORMAT_ID,
        codec.decode(codec.encode(approved)).getScopeIdentity());
  }

  @Test
  public void rejectsLegacyVersionAndSameParticipantsWithDifferentScope() {
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    byte[] legacy = codec.encode(checkpoint(10, 1));
    ByteBuffer.wrap(legacy).putShort(4, (short) 2);
    refreshChecksum(legacy);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(legacy));

    ArchiveProgressEnvelope substituted = new ArchiveProgressEnvelope(
        Kind.APPLY_CHECKPOINT, null, 10, bytes(32, 1), bytes(16, 2), bytes(32, 3), null,
        PARTICIPANTS, "experimental/substituted-scope");
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(codec.encode(substituted)));
    assertThrows(ArchivePersistenceException.class,
        () -> substituted.requireIdentity(Kind.APPLY_CHECKPOINT, null, 10, bytes(32, 1),
            bytes(16, 2), bytes(32, 3), PARTICIPANTS));
  }

  @Test
  public void rejectsCorruptionTruncationAndInvalidIdentity() {
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    byte[] encoded = codec.encode(checkpoint(10, 1));
    encoded[encoded.length - 1] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(Arrays.copyOf(encoded, 20)));

    assertThrows(IllegalArgumentException.class, () -> new ArchiveProgressEnvelope(
        Kind.PARTICIPANT_PROGRESS, "contract", 10, bytes(32, 1), bytes(16, 2),
        bytes(32, 3), PARTICIPANTS));
    assertThrows(IllegalArgumentException.class, () -> new ArchiveProgressEnvelope(
        Kind.APPLY_CHECKPOINT, null, 10, bytes(31, 1), bytes(16, 2), bytes(32, 3),
        PARTICIPANTS));
  }

  @Test
  public void rejectsEveryExpectedIdentityMismatch() {
    ArchiveProgressEnvelope progress = progress("account-asset", 10, 1);
    progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
        bytes(32, 1), bytes(16, 2), bytes(32, 3), PARTICIPANTS);

    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "storage-row", 10,
            bytes(32, 1), bytes(16, 2), bytes(32, 3), PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
            bytes(32, 9), bytes(16, 2), bytes(32, 3), PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
            bytes(32, 1), bytes(16, 9), bytes(32, 3), PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
            bytes(32, 1), bytes(16, 2), bytes(32, 9), PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 11,
            bytes(32, 1), bytes(16, 2), bytes(32, 3), PARTICIPANTS));
    assertThrows(ArchivePersistenceException.class,
        () -> progress.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
            bytes(32, 1), bytes(16, 2), bytes(32, 3),
            Arrays.asList("account", "account-asset")));

    ArchiveProgressEnvelope bound = new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS,
        "account-asset", 10, bytes(32, 1), bytes(16, 2), bytes(32, 3), bytes(32, 4),
        PARTICIPANTS);
    bound.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
        bytes(32, 1), bytes(16, 2), bytes(32, 3), bytes(32, 4), PARTICIPANTS);
    assertThrows(ArchivePersistenceException.class,
        () -> bound.requireIdentity(Kind.PARTICIPANT_PROGRESS, "account-asset", 10,
            bytes(32, 1), bytes(16, 2), bytes(32, 3), bytes(32, 5), PARTICIPANTS));
  }

  @Test
  public void preservesOldAuthorityWhenCrashOccursBeforeAtomicReplace() throws Exception {
    Path directory = temporaryFolder.newFolder("progress-file").toPath();
    Path path = directory.resolve("checkpoint.progress");
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(path, codec).store(checkpoint(9, 1));

    ArchiveProgressFile failing = new ArchiveProgressFile(path, codec, temporary -> {
      throw new java.io.IOException("injected after temporary force");
    });
    assertThrows(java.io.IOException.class, () -> failing.store(checkpoint(10, 2)));
    assertTrue(Files.exists(failing.getTemporaryPath()));
    assertEquals(9, new ArchiveProgressFile(path, codec).load().getEpoch());

    new ArchiveProgressFile(path, codec).store(checkpoint(10, 2));
    assertEquals(10, new ArchiveProgressFile(path, codec).load().getEpoch());
    byte[] corrupt = Files.readAllBytes(path);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(path, corrupt);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveProgressFile(path, codec).load());
  }

  private static ArchiveProgressEnvelope checkpoint(long epoch, int seed) {
    return new ArchiveProgressEnvelope(Kind.APPLY_CHECKPOINT, null, epoch, bytes(32, seed),
        bytes(16, seed + 1), bytes(32, seed + 2), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope progress(String participant, long epoch, int seed) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, participant, epoch,
        bytes(32, seed), bytes(16, seed + 1), bytes(32, seed + 2), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope reader(long epoch, int seed) {
    return new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null, epoch, bytes(32, seed),
        bytes(16, seed + 1), bytes(32, seed + 2), PARTICIPANTS);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static void assertEnvelope(ArchiveProgressEnvelope expected,
      ArchiveProgressEnvelope actual) {
    assertEquals(expected.getKind(), actual.getKind());
    assertEquals(expected.getParticipant(), actual.getParticipant());
    assertEquals(expected.getEpoch(), actual.getEpoch());
    assertArrayEquals(expected.getBlockHash(), actual.getBlockHash());
    assertArrayEquals(expected.getBatchId(), actual.getBatchId());
    assertArrayEquals(expected.getPayloadDigest(), actual.getPayloadDigest());
    assertArrayEquals(expected.getMutationPlanDigest(), actual.getMutationPlanDigest());
    assertEquals(expected.getScopeIdentity(), actual.getScopeIdentity());
    assertEquals(expected.getParticipants(), actual.getParticipants());
  }

  private static void refreshChecksum(byte[] encoded) {
    int payloadLength = encoded.length - Integer.BYTES;
    int checksum = Hashing.crc32c().hashBytes(encoded, 0, payloadLength).asInt();
    ByteBuffer.wrap(encoded, payloadLength, Integer.BYTES).putInt(checksum);
  }
}
