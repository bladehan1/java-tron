package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Reason;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Result;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Status;

public class ArchiveFormatAdmissionValidatorTest {

  private static final int MANIFEST_MAGIC = 0x54414d46;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void absentAndEmptyDirectoriesRemainEmptyNewWithoutWrites() throws Exception {
    Path absent = temporaryFolder.getRoot().toPath().resolve("absent");
    Result absentResult = ArchiveFormatAdmissionValidator.inspect(absent);
    assertEquals(Status.EMPTY_NEW, absentResult.getStatus());
    assertEquals(Reason.NONE, absentResult.getReason());
    assertFalse(Files.exists(absent));

    Path empty = temporaryFolder.newFolder("empty").toPath();
    Result emptyResult = ArchiveFormatAdmissionValidator.inspect(empty);
    assertEquals(Status.EMPTY_NEW, emptyResult.getStatus());
    try (java.util.stream.Stream<Path> entries = Files.list(empty)) {
      assertFalse(entries.findAny().isPresent());
    }
  }

  @Test
  public void nonemptyDirectoryWithoutManifestRequiresQuarantineWithoutMutation()
      throws Exception {
    Path archive = temporaryFolder.newFolder("missing-manifest").toPath();
    Path committed = archive.resolve("commits");
    byte[] evidence = new byte[]{1, 2, 3};
    Files.write(committed, evidence);

    Result result = ArchiveFormatAdmissionValidator.inspect(archive);

    assertEquals(Status.QUARANTINE_REQUIRED, result.getStatus());
    assertEquals(Reason.NONEMPTY_WITHOUT_MANIFEST, result.getReason());
    assertArrayEquals(evidence, Files.readAllBytes(committed));
    assertFalse(Files.exists(archive.resolve("MANIFEST")));
  }

  @Test
  public void currentExact27ManifestIsCurrentBaseButNotStartupReady() throws Exception {
    Path archive = temporaryFolder.newFolder("current-base").toPath();
    ArchiveBaseManifest manifest = new ArchiveBaseManifest(archive,
        ArchiveParticipantDescriptor.current().getParticipants());
    manifest.ensureBase(meta(1));
    byte[] before = Files.readAllBytes(archive.resolve("MANIFEST"));

    Result result = ArchiveFormatAdmissionValidator.inspect(archive);

    assertEquals(Status.CURRENT_BASE, result.getStatus());
    assertEquals(Reason.NONE, result.getReason());
    assertArrayEquals(before, Files.readAllBytes(archive.resolve("MANIFEST")));
  }

  @Test
  public void staleExact26ManifestRequiresQuarantineWithoutRewrite() throws Exception {
    Path archive = temporaryFolder.newFolder("stale-exact-26").toPath();
    List<String> exact26 = new ArrayList<>(
        ArchiveParticipantDescriptor.current().getParticipants());
    assertTrue(exact26.remove("abi"));
    byte[] stale = manifest("archive-state/exact-26-abi-tombstone/v1", exact26);
    Path path = archive.resolve("MANIFEST");
    Files.write(path, stale);

    Result result = ArchiveFormatAdmissionValidator.inspect(archive);

    assertEquals(Status.QUARANTINE_REQUIRED, result.getStatus());
    assertEquals(Reason.UNSUPPORTED_OR_CORRUPT_MANIFEST, result.getReason());
    assertArrayEquals(stale, Files.readAllBytes(path));
  }

  @Test
  public void completeExact27AuthorityBundleIsCurrentReadyWithoutWrites() throws Exception {
    Path archive = currentArchive("current-ready");
    byte[] before = Files.readAllBytes(archive.resolve("MANIFEST"));

    Result result = ArchiveFormatAdmissionValidator.inspect(archive, readyBundle(false));

    assertEquals(Status.CURRENT_READY, result.getStatus());
    assertEquals(Reason.NONE, result.getReason());
    assertArrayEquals(before, Files.readAllBytes(archive.resolve("MANIFEST")));
  }

  @Test
  public void activePlanOrIncompleteProgressRequiresQuarantine() throws Exception {
    Path archive = currentArchive("incomplete-authorities");
    ArchiveAuthoritySourceBundle complete = readyBundle(false);
    Map<String, ArchiveProgressEnvelope> incomplete = new LinkedHashMap<>(
        complete.getParticipantProgress());
    incomplete.remove("abi");
    ArchiveAuthoritySourceBundle missingAbi = bundle(false, complete.getApplyCheckpoint(),
        incomplete, complete.getReaderVisible(), complete.getServingGeneration(),
        coverage(1, 2, 2), digest(90));

    for (ArchiveAuthoritySourceBundle candidate : Arrays.asList(readyBundle(true), missingAbi,
        bundle(false, null, complete.getParticipantProgress(), complete.getReaderVisible(),
            complete.getServingGeneration(), coverage(1, 2, 2), digest(90)))) {
      Result result = ArchiveFormatAdmissionValidator.inspect(archive, candidate);
      assertEquals(Status.QUARANTINE_REQUIRED, result.getStatus());
      assertEquals(Reason.INCOMPLETE_OR_INCONSISTENT_AUTHORITIES, result.getReason());
    }
  }

  @Test
  public void staleReaderOrServingSourceRequiresQuarantine() throws Exception {
    Path archive = currentArchive("mismatched-authorities");
    ArchiveAuthoritySourceBundle complete = readyBundle(false);
    HistoryCommitMarker first = marker(1);
    ArchiveProgressEnvelope staleReader = progress(ArchiveProgressEnvelope.Kind.READER_VISIBLE,
        null, first, null);
    ArchiveAuthoritySourceBundle staleReaderBundle = bundle(false,
        complete.getApplyCheckpoint(), complete.getParticipantProgress(), staleReader,
        complete.getServingGeneration(), coverage(1, 2, 2), digest(90));
    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot wrongSource = serving(digest(91));
    ArchiveAuthoritySourceBundle wrongSourceBundle = bundle(false,
        complete.getApplyCheckpoint(), complete.getParticipantProgress(),
        complete.getReaderVisible(), wrongSource, coverage(1, 2, 2), digest(90));

    for (ArchiveAuthoritySourceBundle candidate
        : Arrays.asList(staleReaderBundle, wrongSourceBundle)) {
      Result result = ArchiveFormatAdmissionValidator.inspect(archive, candidate);
      assertEquals(Status.QUARANTINE_REQUIRED, result.getStatus());
      assertEquals(Reason.INCOMPLETE_OR_INCONSISTENT_AUTHORITIES, result.getReason());
    }
  }

  @Test
  public void nonContiguousOrMisalignedHistoryCoverageRequiresQuarantine() throws Exception {
    Path archive = currentArchive("bad-history-coverage");
    ArchiveAuthoritySourceBundle complete = readyBundle(false);

    for (HistoryCoverage invalid : Arrays.asList(
        coverage(1, 1, 2), coverage(1, 2, 3), coverage(1, 2, 2, hash(3)))) {
      ArchiveAuthoritySourceBundle candidate = bundle(false, complete.getApplyCheckpoint(),
          complete.getParticipantProgress(), complete.getReaderVisible(),
          complete.getServingGeneration(), invalid, digest(90));
      Result result = ArchiveFormatAdmissionValidator.inspect(archive, candidate);
      assertEquals(Status.QUARANTINE_REQUIRED, result.getStatus());
      assertEquals(Reason.INCOMPLETE_OR_INCONSISTENT_AUTHORITIES, result.getReason());
    }
  }

  @Test
  public void servingPrefixDigestIsNotAHistoryCoverageAuthority() throws Exception {
    Path archive = currentArchive("serving-prefix-not-coverage");
    ArchiveAuthoritySourceBundle complete = readyBundle(false);
    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot differentInternalDigest =
        new ArchiveAuthoritySourceBundle.ServingGenerationSnapshot(
            ArchiveParticipantDescriptor.FORMAT_ID,
            ArchiveParticipantDescriptor.current().getParticipants(), 0, 2, hash(2),
            digest(81), digest(90));
    ArchiveAuthoritySourceBundle candidate = bundle(false, complete.getApplyCheckpoint(),
        complete.getParticipantProgress(), complete.getReaderVisible(),
        differentInternalDigest, coverage(1, 2, 2), digest(90));

    Result result = ArchiveFormatAdmissionValidator.inspect(archive, candidate);

    assertEquals(Status.CURRENT_READY, result.getStatus());
  }

  private static byte[] manifest(String scopeIdentity, List<String> participants) throws Exception {
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

  private static BlockSnapshotMeta meta(long epoch) {
    byte[] hash = new byte[32];
    hash[31] = (byte) epoch;
    return new BlockSnapshotMeta(epoch, epoch, hash, new byte[32], epoch * 1_000L);
  }

  private Path currentArchive(String name) throws Exception {
    Path archive = temporaryFolder.newFolder(name).toPath();
    ArchiveBaseManifest manifest = new ArchiveBaseManifest(archive,
        ArchiveParticipantDescriptor.current().getParticipants());
    manifest.ensureBase(meta(1));
    return archive;
  }

  private static ArchiveAuthoritySourceBundle readyBundle(boolean activePlan) {
    HistoryCommitMarker head = marker(2);
    byte[] planDigest = null;
    ArchiveProgressEnvelope checkpoint = progress(ArchiveProgressEnvelope.Kind.APPLY_CHECKPOINT,
        null, head, planDigest);
    Map<String, ArchiveProgressEnvelope> participantProgress = new LinkedHashMap<>();
    for (String participant : ArchiveParticipantDescriptor.current().getParticipants()) {
      participantProgress.put(participant, progress(
          ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, participant, head, planDigest));
    }
    return bundle(activePlan, checkpoint, participantProgress,
        progress(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, head, planDigest),
        serving(digest(90)), coverage(1, 2, 2), digest(90));
  }

  private static ArchiveAuthoritySourceBundle bundle(boolean activePlan,
      ArchiveProgressEnvelope checkpoint,
      Map<String, ArchiveProgressEnvelope> participantProgress,
      ArchiveProgressEnvelope readerVisible,
      ArchiveAuthoritySourceBundle.ServingGenerationSnapshot serving,
      HistoryCoverage coverage, byte[] latestSourceDigest) {
    return new ArchiveAuthoritySourceBundle(activePlan, coverage, marker(1), marker(2),
        checkpoint, participantProgress, readerVisible, serving, latestSourceDigest);
  }

  private static HistoryCoverage coverage(long firstEpoch, long recordCount, long headEpoch) {
    return coverage(firstEpoch, recordCount, headEpoch, hash(headEpoch));
  }

  private static HistoryCoverage coverage(long firstEpoch, long recordCount, long headEpoch,
      byte[] headHash) {
    return new HistoryCoverage(firstEpoch, recordCount, headEpoch, headHash);
  }

  private static ArchiveAuthoritySourceBundle.ServingGenerationSnapshot serving(
      byte[] sourceDigest) {
    return new ArchiveAuthoritySourceBundle.ServingGenerationSnapshot(
        ArchiveParticipantDescriptor.FORMAT_ID,
        ArchiveParticipantDescriptor.current().getParticipants(), 0, 2, hash(2), digest(80),
        sourceDigest);
  }

  private static ArchiveProgressEnvelope progress(ArchiveProgressEnvelope.Kind kind,
      String participant, HistoryCommitMarker marker, byte[] planDigest) {
    return new ArchiveProgressEnvelope(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), planDigest,
        ArchiveParticipantDescriptor.current().getParticipants());
  }

  private static HistoryCommitMarker marker(long epoch) {
    return new HistoryCommitMarker(
        new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L),
        epoch - 1, new HistoryLocation(0, epoch * 100, 100, (int) epoch,
            digest(20 + (int) epoch)),
        new HistoryIndexLocation(epoch * 50, 50, digest(30 + (int) epoch)),
        digest16(40 + (int) epoch),
        ArchiveParticipantDescriptor.current().getParticipants());
  }

  private static byte[] hash(long suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }

  private static byte[] digest(int value) {
    byte[] digest = new byte[32];
    Arrays.fill(digest, (byte) value);
    return digest;
  }

  private static byte[] digest16(int value) {
    byte[] digest = new byte[16];
    Arrays.fill(digest, (byte) value);
    return digest;
  }
}
