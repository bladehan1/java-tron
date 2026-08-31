package org.tron.core.db2.stateroot;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable per-Store F-ingest cursor bound to one pinned physical source identity. */
final class PathStatePhysicalIngestCheckpoint {

  private static final int VERSION = 1;
  private final byte[] sourceIdentity;
  private final byte[] cursor;
  private final long rows;
  private final long bytes;

  PathStatePhysicalIngestCheckpoint(byte[] sourceIdentity, byte[] cursor, long rows, long bytes) {
    this.sourceIdentity = copy32(sourceIdentity, "sourceIdentity");
    this.cursor = Arrays.copyOf(Objects.requireNonNull(cursor, "cursor"), cursor.length);
    if (rows < 0 || bytes < 0) {
      throw new IllegalArgumentException("checkpoint rows and bytes must not be negative");
    }
    this.rows = rows;
    this.bytes = bytes;
  }

  byte[] encode() {
    return ByteBuffer.allocate(Integer.BYTES + sourceIdentity.length + Integer.BYTES + cursor.length
        + Long.BYTES * 2).putInt(VERSION).put(sourceIdentity).putInt(cursor.length).put(cursor)
        .putLong(rows).putLong(bytes).array();
  }

  static PathStatePhysicalIngestCheckpoint decode(byte[] encoded) {
    ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
    if (input.remaining() < Integer.BYTES + 32 + Integer.BYTES + Long.BYTES * 2
        || input.getInt() != VERSION) {
      throw new IllegalArgumentException("physical ingest checkpoint is invalid");
    }
    byte[] identity = new byte[32];
    input.get(identity);
    int cursorLength = input.getInt();
    if (cursorLength < 0 || input.remaining() != cursorLength + Long.BYTES * 2) {
      throw new IllegalArgumentException("physical ingest checkpoint cursor is invalid");
    }
    byte[] cursor = new byte[cursorLength];
    input.get(cursor);
    return new PathStatePhysicalIngestCheckpoint(identity, cursor, input.getLong(), input.getLong());
  }

  byte[] getSourceIdentity() { return Arrays.copyOf(sourceIdentity, sourceIdentity.length); }

  byte[] getCursor() { return Arrays.copyOf(cursor, cursor.length); }

  long getRows() { return rows; }

  long getBytes() { return bytes; }

  private static byte[] copy32(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != 32) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return copy;
  }
}
