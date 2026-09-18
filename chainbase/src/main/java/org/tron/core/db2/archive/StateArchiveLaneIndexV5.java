package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;

/** One lane-wide append-only block boundary index for the State Archive V5 prototype. */
final class StateArchiveLaneIndexV5 implements Closeable {

  private static final long UINT32_MAX = 0xffffffffL;
  private static final byte[] HEADER_DOMAIN = "TRON-STATE-ARCHIVE-LANE-INDEX-HEADER-V5\0"
      .getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  private final Path path;
  private final FileChannel channel;
  private final Header header;
  private final boolean writable;
  private long frameCount;
  private Boundary lastBoundary;

  private StateArchiveLaneIndexV5(Path path, FileChannel channel, Header header,
      boolean writable, long frameCount, Boundary lastBoundary) {
    this.path = path;
    this.channel = channel;
    this.header = header;
    this.writable = writable;
    this.frameCount = frameCount;
    this.lastBoundary = lastBoundary;
  }

  static StateArchiveLaneIndexV5 create(Path path, int laneId, long firstBlockNumber)
      throws IOException {
    Path admitted = Objects.requireNonNull(path, "path");
    StateArchiveGethFormatV5.requireLane(laneId);
    requireNonNegative(firstBlockNumber, "first block number");
    Path parent = admitted.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    FileChannel channel = FileChannel.open(admitted, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.READ, StandardOpenOption.WRITE);
    try {
      Header header = new Header(laneId, firstBlockNumber);
      writeFully(channel, ByteBuffer.wrap(header.encode()), 0);
      Boundary initial = new Boundary(0, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
      writeFully(channel, ByteBuffer.wrap(initial.encode()),
          StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH);
      return new StateArchiveLaneIndexV5(admitted, channel, header, true, 0, initial);
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  static StateArchiveLaneIndexV5 openCommitted(Path path, long committedFrameCount)
      throws IOException {
    return openCommitted(path, committedFrameCount, false, true);
  }

  static StateArchiveLaneIndexV5 openCommittedPrefix(Path path, long committedFrameCount)
      throws IOException {
    return openCommitted(path, committedFrameCount, false, false);
  }

  static StateArchiveLaneIndexV5 openWritableCommitted(Path path, long committedFrameCount)
      throws IOException {
    return openCommitted(path, committedFrameCount, true, true);
  }

  private static StateArchiveLaneIndexV5 openCommitted(Path path, long committedFrameCount,
      boolean writable, boolean requireExactLength) throws IOException {
    Path admitted = Objects.requireNonNull(path, "path");
    requireNonNegative(committedFrameCount, "committed frame count");
    FileChannel channel = writable
        ? FileChannel.open(admitted, StandardOpenOption.READ, StandardOpenOption.WRITE)
        : FileChannel.open(admitted, StandardOpenOption.READ);
    try {
      Header header = Header.decode(readFully(channel, 0,
          StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH));
      long expectedLength = expectedLength(committedFrameCount);
      long actualLength = channel.size();
      if (actualLength < expectedLength || requireExactLength && actualLength != expectedLength) {
        throw new IOException("State Archive V5 lane index committed length mismatch");
      }
      Boundary initial = readBoundary(channel, 0);
      if (initial.fileId != 0
          || initial.endOffset != StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH) {
        throw new IOException("State Archive V5 lane index initial boundary mismatch");
      }
      Boundary terminal = committedFrameCount == 0 ? initial
          : readBoundary(channel, committedFrameCount);
      if (committedFrameCount > 0) {
        Boundary previous = readBoundary(channel, committedFrameCount - 1);
        validateTransition(previous, terminal);
      }
      return new StateArchiveLaneIndexV5(admitted, channel, header, writable,
          committedFrameCount, terminal);
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  synchronized void append(long blockNumber, long fileId, long endOffset)
      throws IOException {
    requireWritable();
    long expectedBlock;
    try {
      expectedBlock = Math.addExact(header.firstBlockNumber, frameCount);
    } catch (ArithmeticException failure) {
      throw new IllegalStateException("State Archive V5 block number overflow", failure);
    }
    if (blockNumber != expectedBlock) {
      throw new IllegalArgumentException("State Archive V5 block is not the next ordinal");
    }
    Boundary next = new Boundary(fileId, endOffset);
    validateTransition(lastBoundary, next);
    writeFully(channel, ByteBuffer.wrap(next.encode()), boundaryPosition(frameCount + 1));
    frameCount++;
    lastBoundary = next;
  }

  synchronized FrameRange locate(long blockNumber) throws IOException {
    long ordinal;
    try {
      ordinal = Math.subtractExact(blockNumber, header.firstBlockNumber);
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 block ordinal overflow", failure);
    }
    if (ordinal < 0 || ordinal >= frameCount) {
      throw new IllegalArgumentException("State Archive V5 block is outside the visible index");
    }
    Boundary previous = readBoundary(channel, ordinal);
    Boundary current = readBoundary(channel, ordinal + 1);
    validateTransition(previous, current);
    long start = previous.fileId == current.fileId
        ? previous.endOffset : StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH;
    return new FrameRange(current.fileId, start, current.endOffset);
  }

  synchronized void force() throws IOException {
    requireWritable();
    channel.force(false);
  }

  long getFirstBlockNumber() {
    return header.firstBlockNumber;
  }

  synchronized long getFrameCount() {
    return frameCount;
  }

  synchronized Boundary getLastBoundary() {
    return lastBoundary;
  }

  int getLaneId() {
    return header.laneId;
  }

  byte[] getHeaderDigest() {
    return header.digest();
  }

  Path getPath() {
    return path;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  static long expectedLength(long frameCount) {
    requireNonNegative(frameCount, "frame count");
    try {
      return Math.addExact(StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH,
          Math.multiplyExact(Math.addExact(frameCount, 1),
              StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH));
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 index length overflow", failure);
    }
  }

  static long boundaryPosition(long ordinal) {
    requireNonNegative(ordinal, "boundary ordinal");
    try {
      return Math.addExact(StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH,
          Math.multiplyExact(ordinal, StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH));
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 index position overflow", failure);
    }
  }

  static byte[] encodeBoundary(long fileId, long endOffset) {
    return new Boundary(fileId, endOffset).encode();
  }

  static Boundary decodeBoundary(byte[] encoded) {
    if (encoded == null || encoded.length != StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH) {
      throw new IllegalArgumentException("State Archive V5 boundary length mismatch");
    }
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    return new Boundary(Integer.toUnsignedLong(bytes.getInt()),
        Integer.toUnsignedLong(bytes.getInt()));
  }

  private void requireWritable() {
    if (!writable) {
      throw new IllegalStateException("State Archive V5 committed lane index is read-only");
    }
  }

  private static Boundary readBoundary(FileChannel channel, long ordinal) throws IOException {
    return decodeBoundary(readFully(channel, boundaryPosition(ordinal),
        StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH));
  }

  private static void validateTransition(Boundary previous, Boundary current) {
    long startOffset;
    if (current.fileId == previous.fileId) {
      if (previous.endOffset >= StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES) {
        throw new IllegalArgumentException(
            "State Archive V5 segment must rotate after reaching its target");
      }
      startOffset = previous.endOffset;
    } else {
      if (previous.fileId == UINT32_MAX || current.fileId != previous.fileId + 1) {
        throw new IllegalArgumentException("State Archive V5 rotation file ID mismatch");
      }
      startOffset = StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH;
    }
    long frameBytes = current.endOffset - startOffset;
    if (frameBytes <= 0 || frameBytes > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
      if (current.fileId == previous.fileId && frameBytes <= 0) {
        throw new IllegalArgumentException(
            "State Archive V5 same-file boundary does not advance");
      }
      throw new IllegalArgumentException("State Archive V5 frame length mismatch");
    }
  }

  private static byte[] readFully(FileChannel channel, long position, int length)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int read = channel.read(bytes, position + bytes.position());
      if (read < 0) {
        throw new EOFException("State Archive V5 lane index is truncated");
      }
      if (read == 0) {
        throw new IOException("State Archive V5 lane index read made no progress");
      }
    }
    return bytes.array();
  }

  private static void writeFully(FileChannel channel, ByteBuffer bytes, long position)
      throws IOException {
    while (bytes.hasRemaining()) {
      int written = channel.write(bytes, position + bytes.position());
      if (written == 0) {
        throw new IOException("State Archive V5 lane index write made no progress");
      }
    }
  }

  private static long requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException("State Archive V5 " + name + " is negative");
    }
    return value;
  }

  static final class FrameRange {
    private final long fileId;
    private final long startOffset;
    private final long endOffset;

    private FrameRange(long fileId, long startOffset, long endOffset) {
      this.fileId = fileId;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    long getFileId() {
      return fileId;
    }

    long getStartOffset() {
      return startOffset;
    }

    long getEndOffset() {
      return endOffset;
    }
  }

  static final class Boundary {
    private final long fileId;
    private final long endOffset;

    private Boundary(long fileId, long endOffset) {
      if (fileId < 0 || fileId > UINT32_MAX || endOffset < 0 || endOffset > UINT32_MAX) {
        throw new IllegalArgumentException("State Archive V5 boundary exceeds u32");
      }
      this.fileId = fileId;
      this.endOffset = endOffset;
    }

    private byte[] encode() {
      return ByteBuffer.allocate(StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH)
          .putInt((int) fileId).putInt((int) endOffset).array();
    }

    long getFileId() {
      return fileId;
    }

    long getEndOffset() {
      return endOffset;
    }

  }

  private static final class Header {
    private static final int DIGEST_OFFSET = 80;
    private static final int CRC_OFFSET = 124;

    private final int laneId;
    private final long firstBlockNumber;

    private Header(int laneId, long firstBlockNumber) {
      StateArchiveGethFormatV5.requireLane(laneId);
      this.laneId = laneId;
      this.firstBlockNumber = requireNonNegative(firstBlockNumber, "first block number");
    }

    private byte[] encode() {
      ByteBuffer bytes = ByteBuffer.allocate(StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH);
      bytes.putInt(StateArchiveGethFormatV5.LANE_INDEX_MAGIC)
          .putShort(StateArchiveGethFormatV5.MAJOR_VERSION)
          .putShort(StateArchiveGethFormatV5.MINOR_VERSION)
          .putInt(StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH)
          .putShort((short) StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH)
          .putShort((short) laneId).putInt(0).putInt(0).putLong(firstBlockNumber)
          .putInt(StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH)
          .putInt(StateArchiveGethFormatV5.MAX_FRAME_BYTES)
          .putLong(StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES)
          .put(StateArchiveGethFormatV5.formatDigest());
      if (bytes.position() != DIGEST_OFFSET) {
        throw new IllegalStateException("Invalid State Archive V5 index header layout");
      }
      bytes.put(StateArchiveFileFormatV3.sha256(HEADER_DOMAIN,
          Arrays.copyOf(bytes.array(), DIGEST_OFFSET))).put(new byte[12]);
      bytes.putInt(crc32c(bytes.array(), 0, CRC_OFFSET));
      return bytes.array();
    }

    private byte[] digest() {
      return Arrays.copyOfRange(encode(), DIGEST_OFFSET, DIGEST_OFFSET + 32);
    }

    private static Header decode(byte[] encoded) throws IOException {
      if (encoded.length != StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH) {
        throw new IOException("State Archive V5 index header length mismatch");
      }
      ByteBuffer bytes = ByteBuffer.wrap(encoded);
      require(bytes.getInt() == StateArchiveGethFormatV5.LANE_INDEX_MAGIC,
          "index header magic mismatch");
      require(bytes.getShort() == StateArchiveGethFormatV5.MAJOR_VERSION
              && bytes.getShort() == StateArchiveGethFormatV5.MINOR_VERSION,
          "index header version mismatch");
      require(bytes.getInt() == StateArchiveGethFormatV5.LANE_INDEX_HEADER_LENGTH,
          "index header size mismatch");
      require(Short.toUnsignedInt(bytes.getShort())
              == StateArchiveGethFormatV5.LANE_INDEX_ENTRY_LENGTH,
          "index entry size mismatch");
      int laneId = Short.toUnsignedInt(bytes.getShort());
      try {
        StateArchiveGethFormatV5.requireLane(laneId);
      } catch (IllegalArgumentException failure) {
        throw new IOException(failure.getMessage(), failure);
      }
      int flags = bytes.getInt();
      int firstFileId = bytes.getInt();
      require(flags == 0 && firstFileId == 0,
          "index header flags or first file mismatch");
      long firstBlockNumber = bytes.getLong();
      require(firstBlockNumber >= 0, "index first block mismatch");
      require(bytes.getInt() == StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
              && bytes.getInt() == StateArchiveGethFormatV5.MAX_FRAME_BYTES
              && bytes.getLong() == StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
          "index size policy mismatch");
      byte[] formatDigest = new byte[32];
      bytes.get(formatDigest);
      require(Arrays.equals(formatDigest, StateArchiveGethFormatV5.formatDigest()),
          "index format identity mismatch");
      byte[] headerDigest = new byte[32];
      bytes.get(headerDigest);
      require(Arrays.equals(headerDigest, StateArchiveFileFormatV3.sha256(
          HEADER_DOMAIN, Arrays.copyOf(encoded, DIGEST_OFFSET))),
          "index header digest mismatch");
      byte[] reserved = new byte[12];
      bytes.get(reserved);
      for (byte value : reserved) {
        require(value == 0, "index header reserved bytes mismatch");
      }
      require(bytes.getInt() == crc32c(encoded, 0, CRC_OFFSET),
          "index header checksum mismatch");
      return new Header(laneId, firstBlockNumber);
    }

    private static void require(boolean condition, String message) throws IOException {
      if (!condition) {
        throw new IOException("State Archive V5 " + message);
      }
    }
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }
}
