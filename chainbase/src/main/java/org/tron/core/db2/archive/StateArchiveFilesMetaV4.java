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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.tron.core.db2.archive.StateArchiveCataloglessFormatV4.FileDescriptor;

/** One append-only sealed-segment directory for a catalogless v4 lane. */
final class StateArchiveFilesMetaV4 implements Closeable {

  private static final int MAGIC = 0x53464d34;
  static final int HEADER_LENGTH = 128;
  private static final int HEADER_CRC_OFFSET = 124;
  private final int laneId;
  private final FileChannel channel;
  private final List<FileDescriptor> descriptors;

  private StateArchiveFilesMetaV4(int laneId, FileChannel channel,
      List<FileDescriptor> descriptors) {
    this.laneId = laneId;
    this.channel = channel;
    this.descriptors = descriptors;
  }

  static StateArchiveFilesMetaV4 openOrCreate(Path root, int laneId) throws IOException {
    requireLane(laneId);
    Files.createDirectories(root);
    Path path = root.resolve(String.format("lane-%04d.files.meta", laneId));
    if (!Files.exists(path)) {
      try (FileChannel created = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        writeFully(created, ByteBuffer.wrap(encodeHeader(laneId)), 0);
        created.force(true);
      }
    }
    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
    try {
      validateHeader(readFully(channel, 0, HEADER_LENGTH), laneId);
      long size = channel.size();
      require(size >= HEADER_LENGTH
          && (size - HEADER_LENGTH) % StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH == 0,
          "files.meta length mismatch");
      return new StateArchiveFilesMetaV4(laneId, channel, readDescriptors(channel, size));
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  synchronized void append(FileDescriptor descriptor) throws IOException {
    FileDescriptor admitted = descriptor;
    long expectedId = descriptors.size();
    require(admitted.getFileId() == expectedId, "files.meta file id is discontinuous");
    if (!descriptors.isEmpty()) {
      FileDescriptor previous = descriptors.get(descriptors.size() - 1);
      require(admitted.getFirstRecordBlockNumber() == previous.getEndBlockNumber() + 1,
          "files.meta block range is discontinuous");
    }
    long offset = committedLength();
    writeFully(channel, ByteBuffer.wrap(
        StateArchiveCataloglessFormatV4.encodeFileDescriptor(admitted)), offset);
    descriptors.add(admitted);
  }

  synchronized void force() throws IOException {
    channel.force(true);
  }

  synchronized long committedLength() {
    return HEADER_LENGTH + (long) descriptors.size()
        * StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH;
  }

  synchronized List<FileDescriptor> snapshot() {
    return Collections.unmodifiableList(new ArrayList<>(descriptors));
  }

  synchronized List<FileDescriptor> readCommitted(long committedBytes) throws IOException {
    require(committedBytes >= HEADER_LENGTH
        && (committedBytes - HEADER_LENGTH)
        % StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH == 0,
        "files.meta committed length mismatch");
    require(committedBytes <= channel.size(), "files.meta committed prefix is missing");
    return Collections.unmodifiableList(readDescriptors(channel, committedBytes));
  }

  int getLaneId() {
    return laneId;
  }

  @Override
  public synchronized void close() throws IOException {
    channel.close();
  }

  private static List<FileDescriptor> readDescriptors(FileChannel channel, long limit)
      throws IOException {
    List<FileDescriptor> result = new ArrayList<>();
    long offset = HEADER_LENGTH;
    while (offset < limit) {
      FileDescriptor descriptor = StateArchiveCataloglessFormatV4.decodeFileDescriptor(
          readFully(channel, offset, StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH));
      require(descriptor.getFileId() == result.size(), "files.meta file id mismatch");
      if (!result.isEmpty()) {
        FileDescriptor previous = result.get(result.size() - 1);
        require(descriptor.getFirstRecordBlockNumber() == previous.getEndBlockNumber() + 1,
            "files.meta block range mismatch");
      }
      result.add(descriptor);
      offset += StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH;
    }
    return result;
  }

  private static byte[] encodeHeader(int laneId) {
    ByteBuffer bytes = ByteBuffer.allocate(HEADER_LENGTH);
    bytes.putInt(MAGIC).putShort(StateArchiveCataloglessFormatV4.MAJOR_VERSION)
        .putShort(StateArchiveCataloglessFormatV4.MINOR_VERSION).putInt(HEADER_LENGTH)
        .putShort((short) laneId).putShort((short) 0)
        .putInt(StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH).putInt(0)
        .putLong(StateArchiveCataloglessFormatV4.SEGMENT_TARGET_BYTES)
        .put(StateArchiveCataloglessFormatV4.formatDigest()).put(new byte[60]);
    bytes.putInt(Hashing.crc32c().hashBytes(bytes.array(), 0, HEADER_CRC_OFFSET).asInt());
    return bytes.array();
  }

  private static void validateHeader(byte[] encoded, int laneId) {
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    require(bytes.getInt() == MAGIC, "files.meta magic mismatch");
    require(bytes.getShort() == StateArchiveCataloglessFormatV4.MAJOR_VERSION,
        "files.meta major version mismatch");
    require(bytes.getShort() == StateArchiveCataloglessFormatV4.MINOR_VERSION,
        "files.meta minor version mismatch");
    require(bytes.getInt() == HEADER_LENGTH, "files.meta header length mismatch");
    require(Short.toUnsignedInt(bytes.getShort()) == laneId, "files.meta lane mismatch");
    require(bytes.getShort() == 0, "files.meta flags mismatch");
    require(bytes.getInt() == StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH,
        "files.meta descriptor length mismatch");
    require(bytes.getInt() == 0, "files.meta header reserved value mismatch");
    require(bytes.getLong() == StateArchiveCataloglessFormatV4.SEGMENT_TARGET_BYTES,
        "files.meta segment target mismatch");
    byte[] digest = new byte[32];
    bytes.get(digest);
    require(java.util.Arrays.equals(digest, StateArchiveCataloglessFormatV4.formatDigest()),
        "files.meta format digest mismatch");
    byte[] reserved = new byte[60];
    bytes.get(reserved);
    for (byte value : reserved) {
      require(value == 0, "files.meta reserved bytes mismatch");
    }
    require(bytes.getInt() == Hashing.crc32c()
        .hashBytes(encoded, 0, HEADER_CRC_OFFSET).asInt(), "files.meta header checksum mismatch");
  }

  private static byte[] readFully(FileChannel channel, long offset, int length)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    while (buffer.hasRemaining()) {
      int read = channel.read(buffer, offset + buffer.position());
      if (read < 0) {
        throw new EOFException("files.meta prefix is truncated");
      }
    }
    return buffer.array();
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer, long offset)
      throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer, offset + buffer.position());
    }
  }

  private static void requireLane(int laneId) {
    require(laneId == 0 || laneId == 4 || laneId == 5 || laneId == 13 || laneId == 22,
        "files.meta lane mismatch");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
