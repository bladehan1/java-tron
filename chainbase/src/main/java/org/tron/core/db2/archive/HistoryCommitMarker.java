package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Reader visibility boundary proving that one body and index delta are durable. */
public final class HistoryCommitMarker {

  private final BlockSnapshotMeta meta;
  private final long previousEpoch;
  private final HistoryLocation historyLocation;
  private final HistoryIndexLocation indexLocation;
  private final byte[] batchId;
  private final List<String> databases;

  public HistoryCommitMarker(BlockSnapshotMeta meta, long previousEpoch,
      HistoryLocation historyLocation, HistoryIndexLocation indexLocation, byte[] batchId,
      List<String> databases) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.previousEpoch = previousEpoch;
    this.historyLocation = Objects.requireNonNull(historyLocation, "historyLocation");
    this.indexLocation = Objects.requireNonNull(indexLocation, "indexLocation");
    if (batchId == null || batchId.length != 16) {
      throw new IllegalArgumentException("batchId must be exactly 16 bytes");
    }
    this.batchId = Arrays.copyOf(Objects.requireNonNull(batchId, "batchId"), batchId.length);
    List<String> sorted = new ArrayList<>(databases);
    Collections.sort(sorted);
    this.databases = Collections.unmodifiableList(sorted);
  }

  public BlockSnapshotMeta getMeta() {
    return meta;
  }

  public long getPreviousEpoch() {
    return previousEpoch;
  }

  public HistoryLocation getHistoryLocation() {
    return historyLocation;
  }

  public HistoryIndexLocation getIndexLocation() {
    return indexLocation;
  }

  public byte[] getBatchId() {
    return Arrays.copyOf(batchId, batchId.length);
  }

  public List<String> getDatabases() {
    return databases;
  }
}
