package org.tron.core.db2.archive.history.v5;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.contract.StateArchiveCanonicalBlockMetaSource;
import org.tron.core.db2.archive.history.format.V5HistoryTail;
import org.tron.core.db2.archive.serving.ServingHistorySource;
import org.tron.core.db2.archive.serving.StateArchiveServingSource;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

public final class V5TestSupport {
  private V5TestSupport() {}

  public static final class Writer implements Closeable {
    private final StateArchiveFiveLaneWriterV5 writer;

    public Writer(Path root, long first, byte[] parent, long rotation) throws IOException {
      writer = new StateArchiveFiveLaneWriterV5(root, first, parent, rotation);
    }

    public void append(BlockReverseDiff diff) throws IOException {
      writer.append(diff);
    }

    public V5HistoryTail forceTailReady(CommonCheckpointTarget target) throws IOException {
      return writer.forceTailReady(target);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  public static final class Source implements ServingHistorySource, StateArchiveServingSource {
    private final StateArchiveServingSourceV5 source;

    public Source(
        Path root, long first, byte[] parent, StateArchiveCanonicalBlockMetaSource canonical) {
      source = new StateArchiveServingSourceV5(root, first, parent, canonical);
    }

    public void updatePublished(V5HistoryTail tail, CommonCheckpointTarget target)
        throws IOException {
      source.updatePublished(tail, target);
    }

    @Override
    public Committed committed() {
      return source.committed();
    }

    @Override
    public long[] bundleBytes(Committed view, long first, long last) throws IOException {
      return source.bundleBytes(view, first, last);
    }

    @Override
    public List<CommittedBlock> read(Committed view, long first, long last) throws IOException {
      return source.read(view, first, last);
    }

    @Override
    public byte[] blockHash(Committed view, long block) throws IOException {
      return source.blockHash(view, block);
    }

    @Override
    public long getHistoryStartBlock() {
      return source.getHistoryStartBlock();
    }

    @Override
    public List<BlockReverseDiff> readCommittedDiffs(long first, long last, long max)
        throws IOException {
      return source.readCommittedDiffs(first, last, max);
    }
  }
}
