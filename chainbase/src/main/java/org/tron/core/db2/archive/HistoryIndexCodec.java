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
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;

/** Deterministic codec for one authoritative state_history.idx delta. */
public final class HistoryIndexCodec {

  public static final int MAGIC = 0x54415249; // TARI
  public static final short VERSION = 1;
  public static final int FIXED_HEADER_LENGTH = 160;
  public static final int CHECKSUM_LENGTH = Integer.BYTES;

  private final int maxRecordLength;

  public HistoryIndexCodec() {
    this(BlockHistoryCodec.DEFAULT_MAX_RECORD_LENGTH);
  }

  public HistoryIndexCodec(int maxRecordLength) {
    this.maxRecordLength = maxRecordLength;
  }

  public byte[] encode(HistoryIndexRecord record) {
    try {
      byte[] payload = encodePayload(record);
      int length = FIXED_HEADER_LENGTH + payload.length + CHECKSUM_LENGTH;
      checkLength(length);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(length);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(length);
      output.writeLong(record.getMeta().getEpoch());
      output.writeLong(record.getMeta().getBlockNumber());
      output.write(record.getMeta().getBlockHash());
      output.write(record.getMeta().getParentHash());
      output.writeLong(record.getMeta().getTimestamp());
      output.writeInt(record.getHistoryLocation().getSegmentId());
      output.writeLong(record.getHistoryLocation().getOffset());
      output.writeInt(record.getHistoryLocation().getRecordLength());
      output.writeInt(record.getHistoryLocation().getBodyChecksum());
      output.write(record.getHistoryLocation().getBodyDigest());
      output.writeInt(record.getGroups().size());
      output.writeInt(entryCount(record));
      output.write(payload);
      output.flush();
      byte[] withoutChecksum = bytes.toByteArray();
      output.writeInt(crc32c(withoutChecksum));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected in-memory index encoding failure", e);
    }
  }

  public HistoryIndexRecord decode(byte[] encoded) {
    if (encoded == null || encoded.length < FIXED_HEADER_LENGTH + CHECKSUM_LENGTH) {
      throw new IllegalArgumentException("History index record is truncated");
    }
    checkLength(encoded.length);
    int expectedChecksum = ByteBuffer.wrap(encoded, encoded.length - CHECKSUM_LENGTH,
        CHECKSUM_LENGTH).getInt();
    if (expectedChecksum != crc32c(Arrays.copyOf(encoded,
        encoded.length - CHECKSUM_LENGTH))) {
      throw new IllegalArgumentException("History index checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new IllegalArgumentException("Unsupported history index header");
      }
      int recordLength = input.readInt();
      if (recordLength != encoded.length) {
        throw new IllegalArgumentException("History index length mismatch");
      }
      long epoch = input.readLong();
      long blockNumber = input.readLong();
      byte[] blockHash = readExact(input, 32);
      byte[] parentHash = readExact(input, 32);
      long timestamp = input.readLong();
      int segmentId = input.readInt();
      long offset = input.readLong();
      int bodyLength = input.readInt();
      int bodyChecksum = input.readInt();
      byte[] bodyDigest = readExact(input, 32);
      int groupCount = input.readInt();
      int entryCount = input.readInt();
      if (groupCount < 0 || entryCount < 0) {
        throw new IllegalArgumentException("Invalid history index counts");
      }
      List<KeyGroup> groups = decodePayload(input, groupCount, entryCount);
      if (input.available() != CHECKSUM_LENGTH) {
        throw new IllegalArgumentException("History index payload length mismatch");
      }
      BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, blockNumber, blockHash, parentHash,
          timestamp);
      HistoryLocation history = new HistoryLocation(segmentId, offset, bodyLength, bodyChecksum,
          bodyDigest);
      return new HistoryIndexRecord(meta, history, groups);
    } catch (EOFException e) {
      throw new IllegalArgumentException("History index record is truncated", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid history index record", e);
    }
  }

  public int recordLength(byte[] prefix) {
    if (prefix.length < 12) {
      throw new IllegalArgumentException("History index prefix is truncated");
    }
    ByteBuffer buffer = ByteBuffer.wrap(prefix);
    if (buffer.getInt() != MAGIC || buffer.getShort() != VERSION || buffer.getShort() != 0) {
      throw new IllegalArgumentException("Unsupported history index header");
    }
    int length = buffer.getInt();
    checkLength(length);
    return length;
  }

  private byte[] encodePayload(HistoryIndexRecord record) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    String previousDb = null;
    for (KeyGroup group : record.getGroups()) {
      if (previousDb != null && previousDb.compareTo(group.getDbName()) >= 0) {
        throw new IllegalArgumentException("Index database groups are not strictly sorted");
      }
      previousDb = group.getDbName();
      byte[] dbName = group.getDbName().getBytes(StandardCharsets.UTF_8);
      writeUnsignedVarInt(output, dbName.length);
      output.write(dbName);
      List<byte[]> keys = group.getKeys();
      writeUnsignedVarInt(output, keys.size());
      byte[] previousKey = null;
      for (byte[] key : keys) {
        if (previousKey != null && BlockReverseDiff.compareUnsigned(previousKey, key) >= 0) {
          throw new IllegalArgumentException("Index keys are not strictly sorted");
        }
        previousKey = key;
        writeUnsignedVarInt(output, key.length);
        output.write(key);
      }
    }
    output.flush();
    return bytes.toByteArray();
  }

  private List<KeyGroup> decodePayload(DataInputStream input, int groupCount, int expectedEntries)
      throws IOException {
    List<KeyGroup> groups = new ArrayList<>(groupCount);
    int actualEntries = 0;
    String previousDb = null;
    for (int groupIndex = 0; groupIndex < groupCount; groupIndex++) {
      String dbName = new String(readLengthPrefixed(input), StandardCharsets.UTF_8);
      if (previousDb != null && previousDb.compareTo(dbName) >= 0) {
        throw new IllegalArgumentException("Decoded index groups are not strictly sorted");
      }
      previousDb = dbName;
      int count = readUnsignedVarInt(input);
      List<byte[]> keys = new ArrayList<>(count);
      byte[] previousKey = null;
      for (int keyIndex = 0; keyIndex < count; keyIndex++) {
        byte[] key = readLengthPrefixed(input);
        if (previousKey != null && BlockReverseDiff.compareUnsigned(previousKey, key) >= 0) {
          throw new IllegalArgumentException("Decoded index keys are not strictly sorted");
        }
        previousKey = key;
        keys.add(key);
      }
      actualEntries += count;
      groups.add(new KeyGroup(dbName, keys));
    }
    if (actualEntries != expectedEntries) {
      throw new IllegalArgumentException("History index entry count mismatch");
    }
    return groups;
  }

  private byte[] readLengthPrefixed(DataInputStream input) throws IOException {
    int length = readUnsignedVarInt(input);
    if (length > maxRecordLength) {
      throw new IllegalArgumentException("History index field is too large");
    }
    return readExact(input, length);
  }

  private static byte[] readExact(DataInputStream input, int length) throws IOException {
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private static int entryCount(HistoryIndexRecord record) {
    return record.getGroups().stream().mapToInt(group -> group.getKeys().size()).sum();
  }

  private static void writeUnsignedVarInt(DataOutputStream output, int value) throws IOException {
    int remaining = value;
    while ((remaining & 0xffffff80) != 0) {
      output.writeByte((remaining & 0x7f) | 0x80);
      remaining >>>= 7;
    }
    output.writeByte(remaining);
  }

  private static int readUnsignedVarInt(DataInputStream input) throws IOException {
    int value = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      int next = input.readUnsignedByte();
      if (shift == 28 && (next & 0xf0) != 0) {
        throw new IllegalArgumentException("Unsigned varint overflows int");
      }
      value |= (next & 0x7f) << shift;
      if ((next & 0x80) == 0) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unsigned varint is too long");
  }

  private static int crc32c(byte[] bytes) {
    return Hashing.crc32c().hashBytes(bytes).asInt();
  }

  private void checkLength(int length) {
    if (length < FIXED_HEADER_LENGTH + CHECKSUM_LENGTH || length > maxRecordLength) {
      throw new IllegalArgumentException("History index record length is invalid: " + length);
    }
  }
}
