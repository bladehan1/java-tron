package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Versioned and checksummed codec for the Chainbase checkpoint Archive binding. */
public final class ArchiveWalBindingCodec {

  private static final int MAGIC = 0x54415742; // TAWB
  private static final short VERSION = 1;
  private static final int ENCODED_LENGTH = 360;

  public byte[] encode(ArchiveWalBinding binding) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(ENCODED_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(ENCODED_LENGTH);
      writeMeta(output, binding.getFirst());
      writeMeta(output, binding.getLast());
      output.writeLong(binding.getPredecessorEpoch());
      output.write(binding.getPredecessorHash());
      output.write(binding.getBatchDigest());
      output.write(binding.getStoreScopeDigest());
      output.write(binding.getHistoryRefsDigest());
      output.write(binding.getBlockIndexRefsDigest());
      output.flush();
      byte[] payload = bytes.toByteArray();
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected Archive WAL binding encoding failure", impossible);
    }
  }

  public ArchiveWalBinding decode(byte[] encoded) {
    if (encoded == null || encoded.length != ENCODED_LENGTH) {
      throw new IllegalArgumentException("Archive WAL binding length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Archive WAL binding checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported Archive WAL binding header");
      }
      BlockSnapshotMeta first = readMeta(input);
      BlockSnapshotMeta last = readMeta(input);
      long predecessorEpoch = input.readLong();
      byte[] predecessorHash = readExact(input, 32);
      byte[] batchDigest = readExact(input, 32);
      byte[] scopeDigest = readExact(input, 32);
      byte[] historyDigest = readExact(input, 32);
      byte[] indexDigest = readExact(input, 32);
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Archive WAL binding payload mismatch");
      }
      return new ArchiveWalBinding(first, last, predecessorEpoch, predecessorHash,
          batchDigest, scopeDigest, historyDigest, indexDigest);
    } catch (EOFException truncated) {
      throw new IllegalArgumentException("Archive WAL binding is truncated", truncated);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Archive WAL binding is invalid", invalid);
    }
  }

  private static void writeMeta(DataOutputStream output, BlockSnapshotMeta meta)
      throws IOException {
    output.writeLong(meta.getEpoch());
    output.writeLong(meta.getBlockNumber());
    output.write(meta.getBlockHash());
    output.write(meta.getParentHash());
    output.writeLong(meta.getTimestamp());
  }

  private static BlockSnapshotMeta readMeta(DataInputStream input) throws IOException {
    return new BlockSnapshotMeta(input.readLong(), input.readLong(), readExact(input, 32),
        readExact(input, 32), input.readLong());
  }

  private static byte[] readExact(DataInputStream input, int length) throws IOException {
    byte[] value = new byte[length];
    input.readFully(value);
    return value;
  }
}
