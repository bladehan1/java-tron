package org.tron.core.db2.archive;

import java.util.Arrays;

/** Immutable height coverage of one validated, contiguous history commit log. */
public final class HistoryCoverage {

  private final long firstEpoch;
  private final long recordCount;
  private final long headEpoch;
  private final byte[] headHash;

  public HistoryCoverage(long firstEpoch, long recordCount, long headEpoch, byte[] headHash) {
    if (firstEpoch < 0 || recordCount <= 0 || headEpoch < firstEpoch) {
      throw new IllegalArgumentException("History coverage range is invalid");
    }
    if (headHash == null || headHash.length != 32) {
      throw new IllegalArgumentException("History coverage head hash must be exactly 32 bytes");
    }
    this.firstEpoch = firstEpoch;
    this.recordCount = recordCount;
    this.headEpoch = headEpoch;
    this.headHash = Arrays.copyOf(headHash, headHash.length);
  }

  public long getFirstEpoch() {
    return firstEpoch;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public long getHeadEpoch() {
    return headEpoch;
  }

  public byte[] getHeadHash() {
    return Arrays.copyOf(headHash, headHash.length);
  }
}
