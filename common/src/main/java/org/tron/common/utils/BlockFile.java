package org.tron.common.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.tron.protos.Protocol.Block;

/**
 * Versioned container for a consecutive range of raw protobuf blocks.
 */
public final class BlockFile {

  private static final byte[] MAGIC = new byte[] {'T', 'R', 'O', 'N', 'B', 'L', 'K', '1'};
  private static final int VERSION = 1;
  private static final int BLOCK_ID_LENGTH = 32;
  private static final int MAX_BLOCK_LENGTH = 64 * 1024 * 1024;

  private BlockFile() {
  }

  /** Supplies one source record for the requested height. */
  @FunctionalInterface
  public interface RecordSource {
    Record get(long height) throws IOException;
  }

  /** Writes an inclusive, consecutive block range without keeping the range in memory. */
  public static Header write(Path output, long start, long end, boolean overwrite,
      RecordSource source) throws IOException {
    validateRange(start, end);
    Path absoluteOutput = output.toAbsolutePath().normalize();
    if (Files.exists(absoluteOutput) && !overwrite) {
      throw new IOException("Output file already exists: " + absoluteOutput);
    }
    Path parent = absoluteOutput.getParent();
    if (parent == null) {
      throw new IOException("Output file must have a parent directory: " + absoluteOutput);
    }
    Files.createDirectories(parent);
    Path temporary = Files.createTempFile(parent, absoluteOutput.getFileName().toString(), ".tmp");
    boolean completed = false;
    Header header = new Header(start, end, end - start + 1);
    try {
      try (DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(
          Files.newOutputStream(temporary, StandardOpenOption.TRUNCATE_EXISTING)))) {
        writeHeader(outputStream, header);
        byte[] previousBlockId = null;
        for (long height = start; height <= end; height++) {
          Record record = source.get(height);
          validateRecord(record, height, previousBlockId);
          writeRecord(outputStream, record);
          previousBlockId = record.getBlockId();
          if (height == Long.MAX_VALUE) {
            break;
          }
        }
      }
      move(temporary, absoluteOutput, overwrite);
      completed = true;
      return header;
    } finally {
      if (!completed) {
        Files.deleteIfExists(temporary);
      }
    }
  }

  /** Opens a streaming reader. The caller must close it. */
  public static Reader open(Path input) throws IOException {
    return new Reader(input);
  }

  private static void validateRange(long start, long end) {
    if (start < 0) {
      throw new IllegalArgumentException("Start height must be non-negative");
    }
    if (end < start) {
      throw new IllegalArgumentException("End height must be greater than or equal to start");
    }
    if (end - start == Long.MAX_VALUE) {
      throw new IllegalArgumentException("Block range is too large");
    }
  }

  private static void writeHeader(DataOutputStream output, Header header) throws IOException {
    output.write(MAGIC);
    output.writeInt(VERSION);
    output.writeLong(header.getStart());
    output.writeLong(header.getEnd());
    output.writeLong(header.getCount());
  }

  private static Header readHeader(DataInputStream input) throws IOException {
    byte[] magic = new byte[MAGIC.length];
    input.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new IOException("Not a java-tron block file");
    }
    int version = input.readInt();
    if (version != VERSION) {
      throw new IOException("Unsupported block file version: " + version);
    }
    long start = input.readLong();
    long end = input.readLong();
    long count = input.readLong();
    try {
      validateRange(start, end);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid block file range", e);
    }
    if (count != end - start + 1) {
      throw new IOException("Block file count does not match its range");
    }
    return new Header(start, end, count);
  }

  private static void writeRecord(DataOutputStream output, Record record) throws IOException {
    byte[] blockData = record.getBlockData();
    output.writeLong(record.getHeight());
    output.write(record.getBlockId());
    output.writeInt(blockData.length);
    output.write(blockData);
    output.writeInt(checksum(blockData));
  }

  private static Record readRecord(DataInputStream input) throws IOException {
    long height = input.readLong();
    byte[] blockId = new byte[BLOCK_ID_LENGTH];
    input.readFully(blockId);
    int blockLength = input.readInt();
    if (blockLength <= 0 || blockLength > MAX_BLOCK_LENGTH) {
      throw new IOException("Invalid block length " + blockLength + " at height " + height);
    }
    byte[] blockData = new byte[blockLength];
    input.readFully(blockData);
    int expectedChecksum = input.readInt();
    if (checksum(blockData) != expectedChecksum) {
      throw new IOException("Block checksum mismatch at height " + height);
    }
    return new Record(height, blockId, blockData);
  }

  private static void validateRecord(Record record, long expectedHeight, byte[] previousBlockId)
      throws IOException {
    if (record == null) {
      throw new IOException("Missing block at height " + expectedHeight);
    }
    if (record.getHeight() != expectedHeight) {
      throw new IOException("Expected block " + expectedHeight + " but got " + record.getHeight());
    }
    Block block = record.getBlock();
    long protoHeight = block.getBlockHeader().getRawData().getNumber();
    if (protoHeight != expectedHeight) {
      throw new IOException("Block protobuf height " + protoHeight
          + " does not match record height " + expectedHeight);
    }
    if (previousBlockId != null && !Arrays.equals(previousBlockId,
        block.getBlockHeader().getRawData().getParentHash().toByteArray())) {
      throw new IOException("Block parent mismatch at height " + expectedHeight);
    }
  }

  private static int checksum(byte[] value) {
    CRC32 crc32 = new CRC32();
    crc32.update(value);
    return (int) crc32.getValue();
  }

  private static void move(Path source, Path target, boolean overwrite) throws IOException {
    StandardCopyOption[] atomicOptions = overwrite
        ? new StandardCopyOption[] {StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING}
        : new StandardCopyOption[] {StandardCopyOption.ATOMIC_MOVE};
    try {
      Files.move(source, target, atomicOptions);
    } catch (AtomicMoveNotSupportedException e) {
      StandardCopyOption[] options = overwrite
          ? new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING}
          : new StandardCopyOption[0];
      Files.move(source, target, options);
    }
  }

  /** Immutable file header. */
  public static final class Header {
    private final long start;
    private final long end;
    private final long count;

    private Header(long start, long end, long count) {
      this.start = start;
      this.end = end;
      this.count = count;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public long getCount() {
      return count;
    }
  }

  /** One raw block and the ID used as its source database key. */
  public static final class Record {
    private final long height;
    private final byte[] blockId;
    private final byte[] blockData;

    public Record(long height, byte[] blockId, byte[] blockData) {
      if (blockId == null || blockId.length != BLOCK_ID_LENGTH) {
        throw new IllegalArgumentException("Block ID must be 32 bytes");
      }
      if (blockData == null || blockData.length == 0 || blockData.length > MAX_BLOCK_LENGTH) {
        throw new IllegalArgumentException("Block data length is invalid");
      }
      this.height = height;
      this.blockId = Arrays.copyOf(blockId, blockId.length);
      this.blockData = Arrays.copyOf(blockData, blockData.length);
    }

    public long getHeight() {
      return height;
    }

    public byte[] getBlockId() {
      return Arrays.copyOf(blockId, blockId.length);
    }

    public byte[] getBlockData() {
      return Arrays.copyOf(blockData, blockData.length);
    }

    public Block getBlock() throws IOException {
      try {
        return Block.parseFrom(blockData);
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("Invalid block protobuf at height " + height, e);
      }
    }
  }

  /** Streaming block reader with structural and chain-continuity validation. */
  public static final class Reader implements Closeable {
    private final DataInputStream input;
    private final Header header;
    private long recordsRead;
    private byte[] previousBlockId;
    private boolean endChecked;

    private Reader(Path inputFile) throws IOException {
      input = new DataInputStream(new BufferedInputStream(Files.newInputStream(inputFile)));
      try {
        header = readHeader(input);
      } catch (IOException e) {
        input.close();
        throw e;
      }
    }

    public Header getHeader() {
      return header;
    }

    public boolean hasNext() throws IOException {
      if (recordsRead < header.getCount()) {
        return true;
      }
      if (!endChecked) {
        endChecked = true;
        if (input.read() != -1) {
          throw new IOException("Unexpected trailing data after block records");
        }
      }
      return false;
    }

    public Record next() throws IOException {
      if (!hasNext()) {
        throw new EOFException("No more block records");
      }
      long expectedHeight = header.getStart() + recordsRead;
      Record record;
      try {
        record = readRecord(input);
      } catch (EOFException e) {
        throw new IOException("Truncated block file at height " + expectedHeight, e);
      }
      validateRecord(record, expectedHeight, previousBlockId);
      previousBlockId = record.getBlockId();
      recordsRead++;
      return record;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }
}
