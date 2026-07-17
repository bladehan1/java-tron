package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Checksummed commit-marker codec. */
public final class HistoryCommitMarkerCodec {

  private static final int MAGIC = 0x54415243; // TARC
  private static final short VERSION = 1;
  private static final int MAX_MARKER_LENGTH = 1024 * 1024;

  public byte[] encode(HistoryCommitMarker marker) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0); // patched after payload is complete
      output.writeLong(marker.getMeta().getEpoch());
      output.writeLong(marker.getPreviousEpoch());
      output.writeLong(marker.getMeta().getBlockNumber());
      output.write(marker.getMeta().getBlockHash());
      output.write(marker.getMeta().getParentHash());
      output.writeLong(marker.getMeta().getTimestamp());
      HistoryLocation body = marker.getHistoryLocation();
      output.writeInt(body.getSegmentId());
      output.writeLong(body.getOffset());
      output.writeInt(body.getRecordLength());
      output.writeInt(body.getBodyChecksum());
      output.write(body.getBodyDigest());
      HistoryIndexLocation index = marker.getIndexLocation();
      output.writeLong(index.getOffset());
      output.writeInt(index.getRecordLength());
      output.write(index.getDigest());
      writeBytes(output, marker.getBatchId());
      output.writeInt(marker.getDatabases().size());
      String previousDb = null;
      for (String database : marker.getDatabases()) {
        if (previousDb != null && previousDb.compareTo(database) >= 0) {
          throw new IllegalArgumentException("Marker databases are not strictly sorted");
        }
        previousDb = database;
        writeBytes(output, database.getBytes(StandardCharsets.UTF_8));
      }
      output.flush();
      byte[] withoutChecksum = bytes.toByteArray();
      int finalLength = withoutChecksum.length + Integer.BYTES;
      if (finalLength > MAX_MARKER_LENGTH) {
        throw new IllegalArgumentException("History commit marker is too large");
      }
      ByteBuffer.wrap(withoutChecksum).putInt(8, finalLength);
      output = new DataOutputStream(bytes);
      bytes.reset();
      output.write(withoutChecksum);
      output.writeInt(crc32c(withoutChecksum));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected in-memory marker encoding failure", e);
    }
  }

  public HistoryCommitMarker decode(byte[] encoded) {
    if (encoded == null || encoded.length < 12 + Integer.BYTES
        || encoded.length > MAX_MARKER_LENGTH) {
      throw new IllegalArgumentException("History commit marker length is invalid");
    }
    int expectedChecksum = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES,
        Integer.BYTES).getInt();
    if (expectedChecksum != crc32c(Arrays.copyOf(encoded, encoded.length - Integer.BYTES))) {
      throw new IllegalArgumentException("History commit marker checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported history commit marker header");
      }
      long epoch = input.readLong();
      long previousEpoch = input.readLong();
      long blockNumber = input.readLong();
      byte[] blockHash = readExact(input, 32);
      byte[] parentHash = readExact(input, 32);
      long timestamp = input.readLong();
      HistoryLocation body = new HistoryLocation(input.readInt(), input.readLong(),
          input.readInt(), input.readInt(), readExact(input, 32));
      HistoryIndexLocation index = new HistoryIndexLocation(input.readLong(), input.readInt(),
          readExact(input, 32));
      byte[] batchId = readBytes(input);
      int dbCount = input.readInt();
      if (dbCount < 0) {
        throw new IllegalArgumentException("Negative marker database count");
      }
      List<String> databases = new ArrayList<>(dbCount);
      String previousDb = null;
      for (int i = 0; i < dbCount; i++) {
        String dbName = new String(readBytes(input), StandardCharsets.UTF_8);
        if (previousDb != null && previousDb.compareTo(dbName) >= 0) {
          throw new IllegalArgumentException("Marker databases are not strictly sorted");
        }
        previousDb = dbName;
        databases.add(dbName);
      }
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("History commit marker payload mismatch");
      }
      BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, blockNumber, blockHash, parentHash,
          timestamp);
      return new HistoryCommitMarker(meta, previousEpoch, body, index, batchId, databases);
    } catch (EOFException e) {
      throw new IllegalArgumentException("History commit marker is truncated", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid history commit marker", e);
    }
  }

  private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }

  private static byte[] readBytes(DataInputStream input) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > MAX_MARKER_LENGTH) {
      throw new IllegalArgumentException("Invalid marker field length");
    }
    return readExact(input, length);
  }

  private static byte[] readExact(DataInputStream input, int length) throws IOException {
    byte[] value = new byte[length];
    input.readFully(value);
    return value;
  }

  private static int crc32c(byte[] value) {
    return Hashing.crc32c().hashBytes(value).asInt();
  }
}
