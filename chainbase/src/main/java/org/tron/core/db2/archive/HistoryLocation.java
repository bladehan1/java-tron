package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Objects;

/** Durable location and identity of one encoded block history body. */
public final class HistoryLocation {

  private final int segmentId;
  private final long offset;
  private final int recordLength;
  private final int bodyChecksum;
  private final byte[] bodyDigest;

  public HistoryLocation(int segmentId, long offset, int recordLength, int bodyChecksum,
      byte[] bodyDigest) {
    if (segmentId < 0 || offset < 0 || recordLength <= 0) {
      throw new IllegalArgumentException("Invalid history location");
    }
    this.segmentId = segmentId;
    this.offset = offset;
    this.recordLength = recordLength;
    this.bodyChecksum = bodyChecksum;
    this.bodyDigest = Arrays.copyOf(Objects.requireNonNull(bodyDigest, "bodyDigest"),
        bodyDigest.length);
  }

  public int getSegmentId() {
    return segmentId;
  }

  public long getOffset() {
    return offset;
  }

  public int getRecordLength() {
    return recordLength;
  }

  public int getBodyChecksum() {
    return bodyChecksum;
  }

  public byte[] getBodyDigest() {
    return Arrays.copyOf(bodyDigest, bodyDigest.length);
  }

  public long endOffset() {
    return offset + recordLength;
  }
}
