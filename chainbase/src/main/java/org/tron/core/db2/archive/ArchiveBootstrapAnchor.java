package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;

/** Explicit identity which makes a synthetic empty first H a non-queryable bootstrap anchor. */
final class ArchiveBootstrapAnchor {

  private static final String PATH = "progress/bootstrap.progress";

  private ArchiveBootstrapAnchor() {
  }

  static void store(Path archiveDirectory, HistoryCommitMarker marker, byte[] planDigest,
      List<String> participants) throws IOException {
    ArchiveProgressEnvelope anchor = new ArchiveProgressEnvelope(Kind.READER_VISIBLE, null,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), planDigest, participants);
    new ArchiveProgressFile(archiveDirectory.resolve(PATH), new ArchiveProgressEnvelopeCodec())
        .store(anchor);
  }

  static HistoryCommitMarker loadAndValidateIfPresent(Path archiveDirectory,
      CommittedHistoryAuthority history, List<String> participants) throws IOException {
    Path path = archiveDirectory.resolve(PATH);
    if (!Files.exists(path)) {
      return null;
    }
    HistoryCommitMarker first = history.get(history.firstEpoch());
    if (first == null) {
      throw new ArchivePersistenceException("Archive bootstrap anchor has no history marker");
    }
    ArchiveProgressEnvelope anchor = new ArchiveProgressFile(path,
        new ArchiveProgressEnvelopeCodec()).load();
    if (anchor.getMutationPlanDigest() == null) {
      throw new ArchivePersistenceException("Archive bootstrap anchor plan digest is missing");
    }
    anchor.requireIdentity(Kind.READER_VISIBLE, null, first.getMeta().getEpoch(),
        first.getMeta().getBlockHash(), first.getBatchId(),
        first.getHistoryLocation().getBodyDigest(), anchor.getMutationPlanDigest(), participants);
    BlockReverseDiff diff;
    if (history instanceof ArchiveHistoryWriter) {
      diff = ((ArchiveHistoryWriter) history).readCommitted(first.getMeta().getEpoch());
    } else {
      return first;
    }
    if (!diff.getGroups().isEmpty()) {
      throw new ArchivePersistenceException("Archive bootstrap anchor history is not empty");
    }
    return first;
  }
}
