package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Validates recovered Chainbase WAL identity before the normal archive writer is attached. */
final class ArchiveWalStartupValidator {

  private static final int MAX_FLUSH_MARKERS = 500;

  private ArchiveWalStartupValidator() {
  }

  static void requireFixedPoint(ArchiveHistoryWriter history, BlockSnapshotMeta canonicalHead,
      ArchiveWalBinding recoveredBinding, List<String> stores) throws IOException {
    ArchiveHistoryWriter writer = Objects.requireNonNull(history, "history");
    BlockSnapshotMeta canonical = Objects.requireNonNull(canonicalHead, "canonicalHead");
    List<String> expectedStores = new ArrayList<>(Objects.requireNonNull(stores, "stores"));
    HistoryCommitMarker head = writer.committedHead();
    if (head == null || !canonical.equals(head.getMeta())) {
      throw new ArchivePersistenceException(
          "Archive committed history does not match canonical Chainbase head");
    }

    HistoryCommitMarker first = writer.get(writer.firstEpoch());
    if (first == null) {
      throw new ArchivePersistenceException("Archive committed history base marker is missing");
    }
    ArchiveBaseManifest.ExistingBase manifest = ArchiveBaseManifest.validateExisting(
        writer.getArchiveDirectory(), expectedStores);
    if (manifest.getEpoch() != first.getPreviousEpoch()
        || !Arrays.equals(manifest.getHash(), first.getMeta().getParentHash())) {
      throw new ArchivePersistenceException(
          "Archive MANIFEST base does not match committed history");
    }

    HistoryCommitMarker bootstrap = ArchiveBootstrapAnchor.loadAndValidateIfPresent(
        writer.getArchiveDirectory(), writer, expectedStores);
    if (recoveredBinding == null) {
      HistoryCoverage coverage = writer.coverage();
      if (bootstrap == null || coverage.getRecordCount() != 1
          || !bootstrap.getMeta().equals(head.getMeta())) {
        throw new ArchivePersistenceException(
            "Archive WAL binding is missing outside the exact fresh bootstrap baseline");
      }
      return;
    }
    if (bootstrap != null && writer.coverage().getRecordCount() == 1) {
      throw new ArchivePersistenceException(
          "Fresh bootstrap baseline must not claim a normal Archive WAL binding");
    }
    if (!recoveredBinding.getLast().equals(head.getMeta())) {
      throw new ArchivePersistenceException(
          "Recovered Archive WAL binding does not end at committed H/canonical P");
    }
    long count;
    try {
      count = Math.addExact(Math.subtractExact(recoveredBinding.getLast().getEpoch(),
          recoveredBinding.getFirst().getEpoch()), 1L);
    } catch (ArithmeticException invalid) {
      throw new ArchivePersistenceException("Recovered Archive WAL binding range overflows",
          invalid);
    }
    if (count <= 0 || count > MAX_FLUSH_MARKERS) {
      throw new ArchivePersistenceException("Recovered Archive WAL binding range is invalid");
    }
    List<HistoryCommitMarker> markers = new ArrayList<>((int) count);
    for (long epoch = recoveredBinding.getFirst().getEpoch();
        epoch <= recoveredBinding.getLast().getEpoch(); epoch++) {
      HistoryCommitMarker marker = writer.get(epoch);
      if (marker == null) {
        throw new ArchivePersistenceException(
            "Recovered Archive WAL binding references missing committed history");
      }
      markers.add(marker);
    }
    ArchiveWalBinding expected = ArchiveWalBinding.fromMarkers(markers);
    if (!sameIdentity(recoveredBinding, expected)) {
      throw new ArchivePersistenceException(
          "Recovered Archive WAL binding differs from committed history refs");
    }
  }

  private static boolean sameIdentity(ArchiveWalBinding left, ArchiveWalBinding right) {
    return left.getFirst().equals(right.getFirst())
        && left.getLast().equals(right.getLast())
        && left.getPredecessorEpoch() == right.getPredecessorEpoch()
        && Arrays.equals(left.getPredecessorHash(), right.getPredecessorHash())
        && Arrays.equals(left.getBatchDigest(), right.getBatchDigest())
        && Arrays.equals(left.getStoreScopeDigest(), right.getStoreScopeDigest())
        && Arrays.equals(left.getHistoryRefsDigest(), right.getHistoryRefsDigest())
        && Arrays.equals(left.getBlockIndexRefsDigest(), right.getBlockIndexRefsDigest());
  }
}
