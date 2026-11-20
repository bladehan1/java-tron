package org.tron.core.db2.archive.query.snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.query.snapshot.ArchiveReadSnapshot.PinnedLatestState;

/** Latest-state pin and point-read boundary shared with common-checkpoint accessors. */
@FunctionalInterface
public interface CheckpointLatestStateFactory {
  PinnedLatestState pin(long blockNumber, byte[] blockHash) throws IOException;

  default OldValue get(long blockNumber, byte[] blockHash, String dbName,
      byte[] physicalRawKey) throws IOException {
    try (PinnedLatestState latest = Objects.requireNonNull(
        pin(blockNumber, blockHash), "pinned latest state")) {
      if (latest.getBlockNumber() != blockNumber
          || !Arrays.equals(blockHash, latest.getBlockHash())) {
        throw new IllegalArgumentException("checkpoint latest access identity mismatch");
      }
      return latest.get(dbName, physicalRawKey);
    }
  }
}
