package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/** Read-only first-stage admission for the current archive base format. */
public final class ArchiveFormatAdmissionValidator {

  private ArchiveFormatAdmissionValidator() {
  }

  /**
   * Inspects only directory existence and the base MANIFEST. CURRENT_BASE is deliberately not a
   * claim that plan/C/D/R, serving generations, or latest-source authorities are startup-ready.
   */
  public static Result inspect(Path archiveDirectory) {
    return inspectBase(archiveDirectory).result;
  }

  /**
   * Compares explicit read-only authority snapshots after validating the current exact-27 base.
   * No source is opened, repaired, created, or rewritten by this method.
   */
  public static Result inspect(Path archiveDirectory, ArchiveAuthoritySourceBundle authorities) {
    BaseInspection baseInspection = inspectBase(archiveDirectory);
    if (baseInspection.result.getStatus() != Status.CURRENT_BASE) {
      return baseInspection.result;
    }
    try {
      requireReady(baseInspection.base, authorities);
      return Result.currentReady();
    } catch (RuntimeException failure) {
      return Result.quarantine(Reason.INCOMPLETE_OR_INCONSISTENT_AUTHORITIES,
          failure.getMessage());
    }
  }

  private static BaseInspection inspectBase(Path archiveDirectory) {
    Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    if (!Files.exists(archiveDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return BaseInspection.of(Result.emptyNew(), null);
    }
    if (Files.isSymbolicLink(archiveDirectory)
        || !Files.isDirectory(archiveDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return BaseInspection.of(Result.quarantine(Reason.INVALID_DIRECTORY,
          "Archive path is not a real directory"), null);
    }
    final boolean empty;
    try (Stream<Path> entries = Files.list(archiveDirectory)) {
      empty = !entries.findAny().isPresent();
    } catch (IOException failure) {
      return BaseInspection.of(Result.quarantine(Reason.INSPECTION_FAILED,
          failure.getMessage()), null);
    }
    if (empty) {
      return BaseInspection.of(Result.emptyNew(), null);
    }
    Path manifest = archiveDirectory.resolve("MANIFEST");
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      return BaseInspection.of(Result.quarantine(Reason.NONEMPTY_WITHOUT_MANIFEST,
          "Non-empty archive directory has no regular MANIFEST"), null);
    }
    try {
      ArchiveBaseManifest.ExistingBase base = ArchiveBaseManifest.validateExisting(archiveDirectory,
          ArchiveParticipantDescriptor.current().getParticipants());
      return BaseInspection.of(Result.currentBase(), base);
    } catch (IOException | RuntimeException failure) {
      return BaseInspection.of(Result.quarantine(Reason.UNSUPPORTED_OR_CORRUPT_MANIFEST,
          failure.getMessage()), null);
    }
  }

  private static void requireReady(ArchiveBaseManifest.ExistingBase base,
      ArchiveAuthoritySourceBundle authorities) {
    Objects.requireNonNull(authorities, "authorities");
    if (authorities.isMutationPlanPresent()) {
      throw new ArchivePersistenceException("Active mutation plan requires recovery");
    }
    HistoryCoverage coverage = Objects.requireNonNull(authorities.getHistoryCoverage(),
        "historyCoverage");
    HistoryCommitMarker first = Objects.requireNonNull(authorities.getFirstHistoryMarker(),
        "firstHistoryMarker");
    HistoryCommitMarker head = Objects.requireNonNull(authorities.getHeadHistoryMarker(),
        "headHistoryMarker");
    List<String> participants = ArchiveParticipantDescriptor.current().getParticipants();
    ArchiveParticipantDescriptor.current().requireExactParticipants(first.getDatabases());
    ArchiveParticipantDescriptor.current().requireExactParticipants(head.getDatabases());
    long expectedRecordCount;
    try {
      expectedRecordCount = Math.addExact(
          Math.subtractExact(coverage.getHeadEpoch(), coverage.getFirstEpoch()), 1);
    } catch (ArithmeticException failure) {
      throw new ArchivePersistenceException("History coverage range overflows", failure);
    }
    if (coverage.getFirstEpoch() != first.getMeta().getEpoch()
        || coverage.getHeadEpoch() != head.getMeta().getEpoch()
        || coverage.getRecordCount() != expectedRecordCount
        || !Arrays.equals(coverage.getHeadHash(), head.getMeta().getBlockHash())
        || first.getMeta().getEpoch() != base.getEpoch() + 1
        || first.getPreviousEpoch() != base.getEpoch()
        || !Arrays.equals(first.getMeta().getParentHash(), base.getHash())
        || head.getMeta().getEpoch() < first.getMeta().getEpoch()) {
      throw new ArchivePersistenceException("History markers do not extend the manifest base");
    }

    ArchiveProgressEnvelope checkpoint = Objects.requireNonNull(
        authorities.getApplyCheckpoint(), "applyCheckpoint");
    byte[] planDigest = checkpoint.getMutationPlanDigest();
    requireProgress(checkpoint, ArchiveProgressEnvelope.Kind.APPLY_CHECKPOINT, null, head,
        planDigest, participants);
    Map<String, ArchiveProgressEnvelope> progress = Objects.requireNonNull(
        authorities.getParticipantProgress(), "participantProgress");
    if (!progress.keySet().equals(new java.util.LinkedHashSet<>(participants))) {
      throw new ArchivePersistenceException("Participant progress set is not exact-27");
    }
    for (String participant : participants) {
      requireProgress(Objects.requireNonNull(progress.get(participant), participant),
          ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS, participant, head, planDigest,
          participants);
    }
    requireProgress(Objects.requireNonNull(authorities.getReaderVisible(), "readerVisible"),
        ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, head, planDigest, participants);

    ArchiveAuthoritySourceBundle.ServingGenerationSnapshot serving = Objects.requireNonNull(
        authorities.getServingGeneration(), "servingGeneration");
    byte[] latestSource = exactDigest(authorities.getLatestSourceIdentityDigest(),
        "latestSourceIdentityDigest");
    if (!ArchiveParticipantDescriptor.FORMAT_ID.equals(serving.getScopeIdentity())
        || !participants.equals(serving.getParticipants())
        || serving.getIndexedFromEpoch() != base.getEpoch()
        || serving.getIndexedThroughEpoch() != head.getMeta().getEpoch()
        || !Arrays.equals(serving.getHeadHash(), head.getMeta().getBlockHash())
        || !Arrays.equals(serving.getLatestSourceIdentityDigest(), latestSource)) {
      throw new ArchivePersistenceException("Serving generation authority mismatch");
    }
  }

  private static void requireProgress(ArchiveProgressEnvelope progress,
      ArchiveProgressEnvelope.Kind kind, String participant, HistoryCommitMarker head,
      byte[] planDigest, List<String> participants) {
    progress.requireIdentity(kind, participant, head.getMeta().getEpoch(),
        head.getMeta().getBlockHash(), head.getBatchId(),
        head.getHistoryLocation().getBodyDigest(), planDigest, participants);
  }

  private static byte[] exactDigest(byte[] digest, String name) {
    if (digest == null || digest.length != 32) {
      throw new ArchivePersistenceException(name + " must be exactly 32 bytes");
    }
    byte[] zero = new byte[32];
    if (Arrays.equals(digest, zero)) {
      throw new ArchivePersistenceException(name + " must not be zero");
    }
    return digest;
  }

  public enum Status {
    EMPTY_NEW,
    CURRENT_BASE,
    CURRENT_READY,
    QUARANTINE_REQUIRED
  }

  public enum Reason {
    NONE,
    INVALID_DIRECTORY,
    INSPECTION_FAILED,
    NONEMPTY_WITHOUT_MANIFEST,
    UNSUPPORTED_OR_CORRUPT_MANIFEST,
    INCOMPLETE_OR_INCONSISTENT_AUTHORITIES
  }

  /** Immutable admission result; CURRENT_READY requires the explicit authority-bundle overload. */
  public static final class Result {
    private final Status status;
    private final Reason reason;
    private final String detail;

    private Result(Status status, Reason reason, String detail) {
      this.status = status;
      this.reason = reason;
      this.detail = detail;
    }

    public Status getStatus() {
      return status;
    }

    public Reason getReason() {
      return reason;
    }

    public String getDetail() {
      return detail;
    }

    private static Result emptyNew() {
      return new Result(Status.EMPTY_NEW, Reason.NONE, "");
    }

    private static Result currentBase() {
      return new Result(Status.CURRENT_BASE, Reason.NONE, "");
    }

    private static Result currentReady() {
      return new Result(Status.CURRENT_READY, Reason.NONE, "");
    }

    private static Result quarantine(Reason reason, String detail) {
      return new Result(Status.QUARANTINE_REQUIRED, reason, detail == null ? "" : detail);
    }
  }

  private static final class BaseInspection {
    private final Result result;
    private final ArchiveBaseManifest.ExistingBase base;

    private BaseInspection(Result result, ArchiveBaseManifest.ExistingBase base) {
      this.result = result;
      this.base = base;
    }

    private static BaseInspection of(Result result, ArchiveBaseManifest.ExistingBase base) {
      return new BaseInspection(result, base);
    }
  }
}
