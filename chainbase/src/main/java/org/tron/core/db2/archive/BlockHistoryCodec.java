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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

/** Deterministic, checksummed codec for one block history record. */
public final class BlockHistoryCodec {

  public static final int MAGIC = 0x54415248; // TARH
  public static final short VERSION = 1;
  public static final int HEADER_LENGTH = 128;
  public static final int CHECKSUM_LENGTH = Integer.BYTES;
  public static final int DEFAULT_MAX_RECORD_LENGTH = 64 * 1024 * 1024;

  private static final short FLAG_DEFLATE = 1;
  private static final int HASH_LENGTH = 32;

  private final int maxRecordLength;

  public BlockHistoryCodec() {
    this(DEFAULT_MAX_RECORD_LENGTH);
  }

  public BlockHistoryCodec(int maxRecordLength) {
    if (maxRecordLength <= HEADER_LENGTH + CHECKSUM_LENGTH) {
      throw new IllegalArgumentException("maxRecordLength is too small");
    }
    this.maxRecordLength = maxRecordLength;
  }

  public byte[] encode(BlockReverseDiff diff) {
    try {
      byte[] rawPayload = encodePayload(diff);
      byte[] payload = deflate(rawPayload);
      long recordLength = HEADER_LENGTH + (long) payload.length + CHECKSUM_LENGTH;
      checkRecordLength(recordLength);

      ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) recordLength);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(FLAG_DEFLATE);
      output.writeInt(HEADER_LENGTH);
      output.writeLong(payload.length);
      output.writeLong(diff.getMeta().getEpoch());
      output.writeLong(diff.getMeta().getBlockNumber());
      output.write(diff.getMeta().getBlockHash());
      output.write(diff.getMeta().getParentHash());
      output.writeLong(diff.getMeta().getTimestamp());
      output.writeInt(diff.getGroups().size());
      output.writeLong(entryCount(diff));
      output.writeLong(rawPayload.length);
      output.write(payload);
      output.flush();

      byte[] withoutChecksum = bytes.toByteArray();
      output.writeInt(crc32c(withoutChecksum));
      output.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected in-memory history encoding failure", e);
    }
  }

  public BlockReverseDiff decode(byte[] record) {
    if (record == null || record.length < HEADER_LENGTH + CHECKSUM_LENGTH) {
      throw new IllegalArgumentException("History record is truncated");
    }
    checkRecordLength(record.length);
    int expectedChecksum = ByteBuffer.wrap(record, record.length - CHECKSUM_LENGTH,
        CHECKSUM_LENGTH).getInt();
    int actualChecksum = crc32c(Arrays.copyOf(record, record.length - CHECKSUM_LENGTH));
    if (expectedChecksum != actualChecksum) {
      throw new IllegalArgumentException("History record checksum mismatch");
    }

    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(record));
      if (input.readInt() != MAGIC) {
        throw new IllegalArgumentException("Invalid history record magic");
      }
      short version = input.readShort();
      if (version != VERSION) {
        throw new IllegalArgumentException("Unsupported history record version: " + version);
      }
      short flags = input.readShort();
      if (flags != FLAG_DEFLATE) {
        throw new IllegalArgumentException("Unsupported history record flags: " + flags);
      }
      int headerLength = input.readInt();
      if (headerLength != HEADER_LENGTH) {
        throw new IllegalArgumentException("Invalid history header length: " + headerLength);
      }
      long payloadLength = input.readLong();
      long expectedLength = HEADER_LENGTH + payloadLength + CHECKSUM_LENGTH;
      if (payloadLength < 0 || expectedLength != record.length) {
        throw new IllegalArgumentException("Invalid history payload length");
      }
      long epoch = input.readLong();
      long blockNumber = input.readLong();
      byte[] blockHash = readExact(input, HASH_LENGTH);
      byte[] parentHash = readExact(input, HASH_LENGTH);
      long timestamp = input.readLong();
      int groupCount = input.readInt();
      long entryCount = input.readLong();
      long rawPayloadLength = input.readLong();
      if (groupCount < 0 || entryCount < 0 || rawPayloadLength < 0
          || rawPayloadLength > maxRecordLength) {
        throw new IllegalArgumentException("Invalid history header counts");
      }
      byte[] payload = readExact(input, (int) payloadLength);
      byte[] rawPayload = inflate(payload, (int) rawPayloadLength);
      List<DbGroup> groups = decodePayload(rawPayload, groupCount, entryCount);
      BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, blockNumber, blockHash, parentHash,
          timestamp);
      return new BlockReverseDiff(meta, groups);
    } catch (EOFException e) {
      throw new IllegalArgumentException("History record is truncated", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid history record", e);
    }
  }

  /** Returns the complete record length described by a fixed header. */
  public int recordLength(byte[] header) {
    if (header.length < HEADER_LENGTH) {
      throw new IllegalArgumentException("History header is truncated");
    }
    ByteBuffer buffer = ByteBuffer.wrap(header);
    if (buffer.getInt() != MAGIC) {
      throw new IllegalArgumentException("Invalid history record magic");
    }
    if (buffer.getShort() != VERSION) {
      throw new IllegalArgumentException("Unsupported history record version");
    }
    if (buffer.getShort() != FLAG_DEFLATE) {
      throw new IllegalArgumentException("Unsupported history record flags");
    }
    if (buffer.getInt() != HEADER_LENGTH) {
      throw new IllegalArgumentException("Invalid history header length");
    }
    long payloadLength = buffer.getLong();
    long length = HEADER_LENGTH + payloadLength + CHECKSUM_LENGTH;
    checkRecordLength(length);
    return (int) length;
  }

  private byte[] encodePayload(BlockReverseDiff diff) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    String previousDb = null;
    for (DbGroup group : diff.getGroups()) {
      if (previousDb != null && previousDb.compareTo(group.getDbName()) >= 0) {
        throw new IllegalArgumentException("History database groups are not strictly sorted");
      }
      previousDb = group.getDbName();
      byte[] dbName = group.getDbName().getBytes(StandardCharsets.UTF_8);
      writeUnsignedVarInt(output, dbName.length);
      output.write(dbName);
      writeUnsignedVarInt(output, group.getEntries().size());
      byte[] previousKey = null;
      for (Entry entry : group.getEntries()) {
        byte[] key = entry.getKey();
        if (previousKey != null && BlockReverseDiff.compareUnsigned(previousKey, key) >= 0) {
          throw new IllegalArgumentException("History entry keys are not strictly sorted");
        }
        previousKey = key;
        writeUnsignedVarInt(output, key.length);
        output.write(key);
        if (entry.getOldValue().isPresent()) {
          byte[] value = entry.getOldValue().getValue();
          writeUnsignedVarInt(output, value.length + 1);
          output.write(value);
        } else {
          writeUnsignedVarInt(output, 0);
        }
      }
    }
    output.flush();
    return bytes.toByteArray();
  }

  private List<DbGroup> decodePayload(byte[] payload, int groupCount, long expectedEntryCount)
      throws IOException {
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
    List<DbGroup> groups = new ArrayList<>(groupCount);
    long actualEntryCount = 0;
    String previousDb = null;
    for (int groupIndex = 0; groupIndex < groupCount; groupIndex++) {
      String dbName = new String(readLengthPrefixed(input), StandardCharsets.UTF_8);
      if (previousDb != null && previousDb.compareTo(dbName) >= 0) {
        throw new IllegalArgumentException("Decoded database groups are not strictly sorted");
      }
      previousDb = dbName;
      int count = readUnsignedVarInt(input);
      List<Entry> entries = new ArrayList<>(count);
      byte[] previousKey = null;
      for (int entryIndex = 0; entryIndex < count; entryIndex++) {
        byte[] key = readLengthPrefixed(input);
        if (previousKey != null && BlockReverseDiff.compareUnsigned(previousKey, key) >= 0) {
          throw new IllegalArgumentException("Decoded history keys are not strictly sorted");
        }
        previousKey = key;
        int encodedOldLength = readUnsignedVarInt(input);
        OldValue oldValue = encodedOldLength == 0 ? OldValue.absent()
            : OldValue.present(readExact(input, encodedOldLength - 1));
        entries.add(new Entry(key, oldValue));
      }
      actualEntryCount += count;
      groups.add(new DbGroup(dbName, entries));
    }
    if (input.available() != 0 || actualEntryCount != expectedEntryCount) {
      throw new IllegalArgumentException("History payload count or length mismatch");
    }
    return groups;
  }

  private byte[] readLengthPrefixed(DataInputStream input) throws IOException {
    int length = readUnsignedVarInt(input);
    if (length > maxRecordLength) {
      throw new IllegalArgumentException("History field exceeds maximum record length");
    }
    return readExact(input, length);
  }

  private byte[] deflate(byte[] input) {
    Deflater deflater = new Deflater(Deflater.BEST_SPEED, true);
    deflater.setInput(input);
    deflater.finish();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    while (!deflater.finished()) {
      int count = deflater.deflate(buffer);
      output.write(buffer, 0, count);
    }
    deflater.end();
    return output.toByteArray();
  }

  private byte[] inflate(byte[] input, int expectedLength) {
    Inflater inflater = new Inflater(true);
    inflater.setInput(input);
    byte[] output = new byte[expectedLength];
    try {
      int count = inflater.inflate(output);
      if (count != expectedLength || !inflater.finished() || inflater.getRemaining() != 0) {
        throw new IllegalArgumentException("History compressed payload length mismatch");
      }
      return output;
    } catch (DataFormatException e) {
      throw new IllegalArgumentException("Invalid compressed history payload", e);
    } finally {
      inflater.end();
    }
  }

  private static long entryCount(BlockReverseDiff diff) {
    return diff.getGroups().stream().mapToLong(group -> group.getEntries().size()).sum();
  }

  private static byte[] readExact(DataInputStream input, int length) throws IOException {
    if (length < 0) {
      throw new IllegalArgumentException("Negative history field length");
    }
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private static void writeUnsignedVarInt(DataOutputStream output, int value) throws IOException {
    if (value < 0) {
      throw new IllegalArgumentException("Negative unsigned varint");
    }
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

  private void checkRecordLength(long length) {
    if (length < HEADER_LENGTH + CHECKSUM_LENGTH || length > maxRecordLength
        || length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("History record exceeds configured maximum: " + length);
    }
  }
}
