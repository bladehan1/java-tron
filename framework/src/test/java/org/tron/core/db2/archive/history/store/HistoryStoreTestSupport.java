package org.tron.core.db2.archive.history.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.history.codec.HistoryCommitMarkerCodec;
import org.tron.core.db2.archive.history.codec.HistoryIndexCodec;
import org.tron.core.db2.archive.history.model.HistoryCommitMarker;
import org.tron.core.db2.archive.history.write.CommittedHistoryEvidenceSource;
import org.tron.core.db2.archive.history.write.DurableHistoryMarkerRangeEvidence;

/** Package-local Store construction for integration fixtures outside this test package. */
public final class HistoryStoreTestSupport {

  private HistoryStoreTestSupport() {
  }

  public static HistoryIndexStore openIndex(Path directory, HistoryIndexCodec codec)
      throws IOException {
    return new HistoryIndexStore(directory, codec);
  }

  public static HistoryCommitStore openCommits(Path directory, HistoryCommitMarkerCodec codec)
      throws IOException {
    return new HistoryCommitStore(directory, codec);
  }

  public interface Source {
    HistoryCommitMarker marker(long epoch);

    BlockReverseDiff readCommitted(long epoch);
  }

  public static DurableHistoryMarkerRangeEvidence evidence(Source source, int maxMarkers) {
    return new DurableHistoryMarkerRangeEvidence(new CommittedHistoryEvidenceSource() {
      @Override
      public HistoryCommitMarker marker(long epoch) {
        return source.marker(epoch);
      }

      @Override
      public BlockReverseDiff readCommitted(long epoch) {
        return source.readCommitted(epoch);
      }
    }, maxMarkers);
  }

  public static final class Manifest {
    private final ArchiveBaseManifest delegate;

    public Manifest(Path directory, List<String> participants) throws IOException {
      delegate = new ArchiveBaseManifest(directory, participants);
    }

    public void ensureBase(BlockSnapshotMeta first) throws IOException {
      delegate.ensureBase(first);
    }
  }
}
