package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Objects;

/** Byte range and digest of an authoritative index delta. */
public final class HistoryIndexLocation {

  private final long offset;
  private final int recordLength;
  private final byte[] digest;

  public HistoryIndexLocation(long offset, int recordLength, byte[] digest) {
    if (offset < 0 || recordLength <= 0) {
      throw new IllegalArgumentException("Invalid history index location");
    }
    this.offset = offset;
    this.recordLength = recordLength;
    this.digest = Arrays.copyOf(Objects.requireNonNull(digest, "digest"), digest.length);
  }

  public long getOffset() {
    return offset;
  }

  public int getRecordLength() {
    return recordLength;
  }

  public byte[] getDigest() {
    return Arrays.copyOf(digest, digest.length);
  }

  public long endOffset() {
    return offset + recordLength;
  }
}
