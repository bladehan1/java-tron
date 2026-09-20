package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.PointLocation;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.Lease;

/** Positioned target-lane point reader; full-frame integrity remains a replay/scrub concern. */
final class StateArchivePointReaderV5 implements StateArchivePointSource {

  private static final int VARIABLE_LAYOUT = 1;
  private static final int FIXED_LAYOUT = 2;
  private static final ReadObserver NO_OP = new ReadObserver() { };

  private final StateArchiveCommittedViewV5 view;
  private final StateArchiveDataHandlePoolV5 handlePool;
  private final ReadObserver observer;

  StateArchivePointReaderV5(StateArchiveCommittedViewV5 view) {
    this(view, null, NO_OP);
  }

  StateArchivePointReaderV5(StateArchiveCommittedViewV5 view, ReadObserver observer) {
    this(view, null, observer);
  }

  StateArchivePointReaderV5(StateArchiveCommittedViewV5 view,
      StateArchiveDataHandlePoolV5 handlePool) {
    this(view, handlePool, NO_OP);
  }

  StateArchivePointReaderV5(StateArchiveCommittedViewV5 view,
      StateArchiveDataHandlePoolV5 handlePool, ReadObserver observer) {
    this.view = Objects.requireNonNull(view, "view");
    this.handlePool = handlePool;
    this.observer = Objects.requireNonNull(observer, "observer");
  }

  @Override
  public OldValue readCommittedOldValue(String dbName, byte[] rawKey, long blockNumber)
      throws IOException {
    String admittedDb = Objects.requireNonNull(dbName, "dbName");
    byte[] admittedKey = Objects.requireNonNull(rawKey, "rawKey");
    int storeId = StateArchiveFileFormatV3.storeId(admittedDb);
    int laneId = StateArchiveFileFormatV3.laneId(storeId);
    int fixedKeyWidth = StateArchiveFileFormatV3.fixedKeyWidth(laneId);
    if (fixedKeyWidth != 0 && admittedKey.length != fixedKeyWidth) {
      throw new IllegalArgumentException("State Archive V5 point key width mismatch");
    }

    // Capture only immutable path/range metadata. All data-file I/O happens afterwards and
    // this reader has no reference to, or monitor dependency on, the append writer.
    PointLocation location = view.capture(laneId, blockNumber);
    try {
      if (handlePool != null) {
        try (Lease lease = handlePool.acquire(location)) {
          return readPoint(lease.getChannel(), location, storeId, admittedKey, blockNumber);
        }
      }
      observer.opened(laneId, location.getDataPath());
      try (FileChannel channel = FileChannel.open(location.getDataPath(),
          StandardOpenOption.READ)) {
        return readPoint(channel, location, storeId, admittedKey, blockNumber);
      }
    } catch (IllegalArgumentException failure) {
      throw new IOException("State Archive V5 point structure is invalid", failure);
    }
  }

  private OldValue readPoint(FileChannel channel, PointLocation location, int targetStoreId,
      byte[] targetKey, long blockNumber) throws IOException {
    int laneId = location.getLaneId();
    long frameBytes = location.getEndOffset() - location.getStartOffset();
    if (frameBytes < StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
        + StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH
        || frameBytes > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
      throw invalid("frame range");
    }
    byte[] header = read(channel, laneId, location.getStartOffset(),
        StateArchiveGethFormatV5.FRAME_HEADER_LENGTH);
    ByteBuffer frame = ByteBuffer.wrap(header);
    if (frame.getInt() != StateArchiveGethFormatV5.BLOCK_FRAME_MAGIC
        || frame.getShort() != StateArchiveGethFormatV5.MAJOR_VERSION
        || frame.getShort() != 0 || frame.getLong() != blockNumber) {
      throw invalid("frame identity");
    }
    frame.position(frame.position() + 32);
    if (Short.toUnsignedInt(frame.getShort()) != laneId) {
      throw invalid("frame lane");
    }
    int sectionCount = Short.toUnsignedInt(frame.getShort());
    long sectionsBytes = Integer.toUnsignedLong(frame.getInt());
    if (frame.getInt() != crc32c(header, 0, 56)
        || frameBytes != StateArchiveGethFormatV5.FRAME_HEADER_LENGTH + sectionsBytes
            + StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH) {
      throw invalid("frame header");
    }

    long sectionPosition = location.getStartOffset()
        + StateArchiveGethFormatV5.FRAME_HEADER_LENGTH;
    long sectionsEnd = sectionPosition + sectionsBytes;
    int previousStoreId = 0;
    for (int index = 0; index < sectionCount; index++) {
      Section section = readSection(channel, laneId, sectionPosition, sectionsEnd);
      if (section.storeId <= previousStoreId) {
        throw invalid("Store order");
      }
      previousStoreId = section.storeId;
      if (section.storeId == targetStoreId) {
        observer.visitedStore(laneId, section.storeId);
        return readValue(channel, laneId, section, targetKey);
      }
      if (section.storeId > targetStoreId) {
        break;
      }
      sectionPosition += section.sectionBytes;
    }
    throw invalid("point index references a missing Store or key");
  }

  private Section readSection(FileChannel channel, int laneId, long position, long sectionsEnd)
      throws IOException {
    if (position < 0 || sectionsEnd - position < StateArchiveGethFormatV5.SECTION_HEADER_LENGTH) {
      throw invalid("section header bounds");
    }
    byte[] encoded = read(channel, laneId, position,
        StateArchiveGethFormatV5.SECTION_HEADER_LENGTH);
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    int storeId = Short.toUnsignedInt(bytes.getShort());
    int layout = Short.toUnsignedInt(bytes.getShort());
    if (bytes.getInt() != StateArchiveGethFormatV5.SECTION_HEADER_LENGTH) {
      throw invalid("section header size");
    }
    long entryCount = Integer.toUnsignedLong(bytes.getInt());
    long keyWidth = Integer.toUnsignedLong(bytes.getInt());
    long keysBytes = bytes.getLong();
    long valuesBytes = bytes.getLong();
    long sectionBytes = bytes.getLong();
    if (bytes.getInt() != 0 || bytes.getInt() != crc32c(encoded, 0, 44)) {
      throw invalid("section header checksum");
    }
    int expectedLane;
    try {
      expectedLane = StateArchiveFileFormatV3.laneId(storeId);
    } catch (IllegalArgumentException failure) {
      throw invalid("Store ID", failure);
    }
    int expectedWidth = StateArchiveFileFormatV3.fixedKeyWidth(expectedLane);
    if (entryCount == 0 || keysBytes < 0 || valuesBytes < 0
        || expectedLane != laneId || keyWidth != expectedWidth
        || (keyWidth == 0 ? layout != VARIABLE_LAYOUT : layout != FIXED_LAYOUT)
        || sectionBytes < StateArchiveGethFormatV5.SECTION_HEADER_LENGTH
        || sectionBytes > sectionsEnd - position) {
      throw invalid("section identity or bounds");
    }
    long keyOffsetBytes = keyWidth == 0 ? multiply(add(entryCount, 1), Integer.BYTES) : 0;
    long presenceBytes = add(entryCount, 7) / 8;
    long valueOffsetBytes = multiply(add(entryCount, 1), Integer.BYTES);
    long expectedSectionBytes = add(StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        keyOffsetBytes, keysBytes, presenceBytes, valueOffsetBytes, valuesBytes);
    if (sectionBytes != expectedSectionBytes
        || keyWidth != 0 && keysBytes != multiply(entryCount, keyWidth)) {
      throw invalid("section arrays");
    }
    return new Section(storeId, position, sectionBytes, checkedInt(entryCount),
        checkedInt(keyWidth), checkedInt(keysBytes), checkedInt(valuesBytes),
        checkedInt(keyOffsetBytes), checkedInt(presenceBytes), checkedInt(valueOffsetBytes));
  }

  private OldValue readValue(FileChannel channel, int laneId, Section section,
      byte[] targetKey) throws IOException {
    long body = section.position + StateArchiveGethFormatV5.SECTION_HEADER_LENGTH;
    long keys = body + section.keyOffsetBytes;
    int low = 0;
    int high = section.entryCount - 1;
    int found = -1;
    while (low <= high) {
      int middle = low + (high - low) / 2;
      byte[] candidate;
      if (section.keyWidth == 0) {
        long offsets = body + (long) middle * Integer.BYTES;
        ByteBuffer pair = ByteBuffer.wrap(read(channel, laneId, offsets, 2 * Integer.BYTES));
        long start = Integer.toUnsignedLong(pair.getInt());
        long end = Integer.toUnsignedLong(pair.getInt());
        if (start > end || end > section.keysBytes) {
          throw invalid("key offsets");
        }
        candidate = read(channel, laneId, keys + start, checkedInt(end - start));
      } else {
        candidate = read(channel, laneId, keys + (long) middle * section.keyWidth,
            section.keyWidth);
      }
      int comparison = BlockReverseDiff.compareUnsigned(candidate, targetKey);
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle - 1;
      } else {
        found = middle;
        break;
      }
    }
    if (found < 0) {
      throw invalid("point index references a missing key");
    }

    long presence = keys + section.keysBytes;
    byte presenceByte = read(channel, laneId, presence + found / 8, 1)[0];
    long valueOffsets = presence + section.presenceBytes;
    ByteBuffer pair = ByteBuffer.wrap(read(channel, laneId,
        valueOffsets + (long) found * Integer.BYTES, 2 * Integer.BYTES));
    long valueStart = Integer.toUnsignedLong(pair.getInt());
    long valueEnd = Integer.toUnsignedLong(pair.getInt());
    if (valueStart > valueEnd || valueEnd > section.valuesBytes) {
      throw invalid("value offsets");
    }
    boolean present = (presenceByte & (1 << (found % 8))) != 0;
    if (!present) {
      if (valueStart != valueEnd) {
        throw invalid("ABSENT value length");
      }
      observer.decodedValue(laneId, section.storeId, 0);
      return OldValue.absent();
    }
    long values = valueOffsets + section.valueOffsetBytes;
    byte[] value = read(channel, laneId, values + valueStart,
        checkedInt(valueEnd - valueStart));
    observer.decodedValue(laneId, section.storeId, value.length);
    return OldValue.present(value);
  }

  private byte[] read(FileChannel channel, int laneId, long position, int length)
      throws IOException {
    if (position < 0 || length < 0) {
      throw invalid("read bounds");
    }
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int count = channel.read(bytes, position + bytes.position());
      if (count < 0) {
        throw new EOFException("State Archive V5 point file is truncated");
      }
      if (count == 0) {
        throw new IOException("State Archive V5 point read made no progress");
      }
    }
    observer.read(laneId, position, length);
    return bytes.array();
  }

  private static long add(long... values) {
    long result = 0;
    try {
      for (long value : values) {
        result = StrictMathWrapper.addExact(result, value);
      }
      return result;
    } catch (ArithmeticException failure) {
      throw invalid("size overflow", failure);
    }
  }

  private static long multiply(long left, long right) {
    try {
      return StrictMathWrapper.multiplyExact(left, right);
    } catch (ArithmeticException failure) {
      throw invalid("size overflow", failure);
    }
  }

  private static int checkedInt(long value) throws IOException {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw invalid("size exceeds int");
    }
    return (int) value;
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static IOException invalid(String name) {
    return new IOException("State Archive V5 invalid " + name);
  }

  private static IllegalArgumentException invalid(String name, Exception cause) {
    return new IllegalArgumentException("State Archive V5 invalid " + name, cause);
  }

  interface ReadObserver {
    default void opened(int laneId, Path path) { }
    default void read(int laneId, long position, int bytes) { }
    default void visitedStore(int laneId, int storeId) { }
    default void decodedValue(int laneId, int storeId, int valueBytes) { }
  }

  private static final class Section {
    private final int storeId;
    private final long position;
    private final long sectionBytes;
    private final int entryCount;
    private final int keyWidth;
    private final int keysBytes;
    private final int valuesBytes;
    private final int keyOffsetBytes;
    private final int presenceBytes;
    private final int valueOffsetBytes;

    private Section(int storeId, long position, long sectionBytes, int entryCount,
        int keyWidth, int keysBytes, int valuesBytes, int keyOffsetBytes,
        int presenceBytes, int valueOffsetBytes) {
      this.storeId = storeId;
      this.position = position;
      this.sectionBytes = sectionBytes;
      this.entryCount = entryCount;
      this.keyWidth = keyWidth;
      this.keysBytes = keysBytes;
      this.valuesBytes = valuesBytes;
      this.keyOffsetBytes = keyOffsetBytes;
      this.presenceBytes = presenceBytes;
      this.valueOffsetBytes = valueOffsetBytes;
    }
  }
}
