package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

/** Canonical codec for one State Archive V5 lane block frame. */
final class StateArchiveBlockFrameCodecV5 {

  private static final short VARIABLE_SECTION_LAYOUT = 1;
  private static final short FIXED_SECTION_LAYOUT = 2;
  private static final byte[] SECTION_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-STORE-SECTION-V5\0");
  private static final byte[] FRAME_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-BLOCK-FRAME-V5\0");
  private static final byte[] LANE_ITEM_DOMAIN = ascii(
      "TRON-STATE-ARCHIVE-LANE-ITEM-V5\0");

  EncodedFrame encode(BlockReverseDiff diff, int laneId) {
    Objects.requireNonNull(diff, "diff");
    StateArchiveGethFormatV5.requireLane(laneId);
    long blockNumber = diff.getMeta().getBlockNumber();
    if (blockNumber < 0) {
      throw new IllegalArgumentException("State Archive V5 block number is negative");
    }
    byte[] blockHash = requireHash(diff.getMeta().getBlockHash(), "block hash");
    List<StoreGroup> groups = admit(diff.getGroups(), laneId);
    List<byte[]> sections = new ArrayList<>(groups.size());
    long sectionsBytes = 0;
    long entryCount = 0;
    List<byte[]> sectionDigests = new ArrayList<>(groups.size());
    for (StoreGroup group : groups) {
      byte[] section = encodeSection(group);
      sections.add(section);
      sectionsBytes = checkedAdd(sectionsBytes, section.length, "sections bytes");
      entryCount = checkedAdd(entryCount, group.entries.size(), "entry count");
      sectionDigests.add(Arrays.copyOfRange(section, 48, 80));
    }
    long frameBytes = checkedAdd(StateArchiveGethFormatV5.FRAME_HEADER_LENGTH,
        sectionsBytes, StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH, "frame bytes");
    if (frameBytes > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
      throw new IllegalArgumentException("State Archive V5 frame exceeds 64 MiB");
    }
    ByteBuffer frame = ByteBuffer.allocate(checkedInt(frameBytes, "frame bytes"));
    frame.putInt(StateArchiveGethFormatV5.BLOCK_FRAME_MAGIC)
        .putShort(StateArchiveGethFormatV5.MAJOR_VERSION).putShort((short) 0)
        .putLong(blockNumber).put(blockHash).putShort((short) laneId)
        .putShort((short) groups.size()).putInt(checkedInt(sectionsBytes, "sections bytes"));
    frame.putInt(crc32c(frame.array(), 0, 56));
    for (byte[] section : sections) {
      frame.put(section);
    }
    int digestEnd = frame.position();
    byte[] frameDigest = StateArchiveFileFormatV3.sha256(
        FRAME_DOMAIN, Arrays.copyOf(frame.array(), digestEnd));
    frame.put(frameDigest);
    frame.putInt(crc32c(frame.array(), 0, frame.position()));
    byte[] laneItemDigest = laneItemDigest(laneId, groups.size(), entryCount,
        sectionsBytes, sectionDigests);
    return new EncodedFrame(laneId, blockNumber, blockHash, frame.array(), frameDigest,
        laneItemDigest, groups.size(), entryCount);
  }

  DecodedFrame decode(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length < StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
        + StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH
        || encoded.length > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
      throw invalid("frame length");
    }
    ByteBuffer frame = ByteBuffer.wrap(encoded);
    require(frame.getInt() == StateArchiveGethFormatV5.BLOCK_FRAME_MAGIC, "frame magic");
    require(frame.getShort() == StateArchiveGethFormatV5.MAJOR_VERSION, "frame version");
    require(frame.getShort() == 0, "frame flags");
    long blockNumber = frame.getLong();
    require(blockNumber >= 0, "block number");
    byte[] blockHash = new byte[32];
    frame.get(blockHash);
    int laneId = Short.toUnsignedInt(frame.getShort());
    try {
      StateArchiveGethFormatV5.requireLane(laneId);
    } catch (IllegalArgumentException failure) {
      throw invalid("lane", failure);
    }
    int sectionCount = Short.toUnsignedInt(frame.getShort());
    int sectionsBytes = frame.getInt();
    require(sectionsBytes >= 0
            && encoded.length == StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
                + sectionsBytes + StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH,
        "sections length");
    require(frame.getInt() == crc32c(encoded, 0, 56), "frame header checksum");

    int sectionsEnd = StateArchiveGethFormatV5.FRAME_HEADER_LENGTH + sectionsBytes;
    List<DbGroup> groups = new ArrayList<>(sectionCount);
    List<byte[]> sectionDigests = new ArrayList<>(sectionCount);
    long entryCount = 0;
    int previousStoreId = 0;
    for (int index = 0; index < sectionCount; index++) {
      DecodedSection section = decodeSection(frame, sectionsEnd, laneId);
      require(section.storeId > previousStoreId, "Store order");
      previousStoreId = section.storeId;
      groups.add(new DbGroup(StateArchiveFileFormatV3.dbName(section.storeId),
          section.entries));
      sectionDigests.add(section.digest);
      entryCount = checkedAdd(entryCount, section.entries.size(), "entry count");
    }
    require(frame.position() == sectionsEnd, "section count or bytes");
    byte[] storedFrameDigest = new byte[32];
    frame.get(storedFrameDigest);
    byte[] actualFrameDigest = StateArchiveFileFormatV3.sha256(
        FRAME_DOMAIN, Arrays.copyOf(encoded, sectionsEnd));
    require(Arrays.equals(storedFrameDigest, actualFrameDigest), "frame digest");
    require(frame.getInt() == crc32c(encoded, 0, encoded.length - Integer.BYTES),
        "frame checksum");
    byte[] itemDigest = laneItemDigest(laneId, sectionCount, entryCount,
        sectionsBytes, sectionDigests);
    return new DecodedFrame(laneId, blockNumber, blockHash, groups, actualFrameDigest,
        itemDigest, entryCount);
  }

  private List<StoreGroup> admit(Collection<DbGroup> input, int targetLane) {
    List<StoreGroup> selected = new ArrayList<>();
    Set<Integer> seen = new HashSet<>();
    for (DbGroup group : input) {
      int storeId = StateArchiveFileFormatV3.storeId(group.getDbName());
      if (!seen.add(storeId)) {
        throw new IllegalArgumentException("Duplicate State Archive V5 Store");
      }
      if (group.getEntries().isEmpty()) {
        throw new IllegalArgumentException("State Archive V5 Store section is empty");
      }
      int laneId = StateArchiveFileFormatV3.laneId(storeId);
      if (laneId != targetLane) {
        continue;
      }
      int keyWidth = StateArchiveFileFormatV3.fixedKeyWidth(laneId);
      byte[] previous = null;
      for (Entry entry : group.getEntries()) {
        byte[] key = entry.getKey();
        if (keyWidth != 0 && key.length != keyWidth) {
          throw new IllegalArgumentException("Invalid State Archive V5 fixed-width key");
        }
        if (previous != null && BlockReverseDiff.compareUnsigned(previous, key) >= 0) {
          throw new IllegalArgumentException("State Archive V5 keys are not unique");
        }
        previous = key;
      }
      selected.add(new StoreGroup(storeId, keyWidth, group.getEntries()));
    }
    selected.sort(Comparator.comparingInt(group -> group.storeId));
    return selected;
  }

  private byte[] encodeSection(StoreGroup group) {
    int count = group.entries.size();
    ByteArrayOutputStream keys = new ByteArrayOutputStream();
    ByteArrayOutputStream values = new ByteArrayOutputStream();
    ByteBuffer keyOffsets = group.keyWidth == 0
        ? ByteBuffer.allocate(checkedInt(checkedMultiply(count + 1L, Integer.BYTES,
            "key offsets"), "key offsets")) : null;
    ByteBuffer valueOffsets = ByteBuffer.allocate(checkedInt(
        checkedMultiply(count + 1L, Integer.BYTES, "value offsets"), "value offsets"));
    byte[] presence = new byte[(count + 7) / 8];
    if (keyOffsets != null) {
      keyOffsets.putInt(0);
    }
    valueOffsets.putInt(0);
    for (int index = 0; index < count; index++) {
      Entry entry = group.entries.get(index);
      byte[] key = entry.getKey();
      keys.write(key, 0, key.length);
      if (keyOffsets != null) {
        keyOffsets.putInt(keys.size());
      }
      OldValue oldValue = entry.getOldValue();
      if (oldValue.isPresent()) {
        presence[index / 8] |= (byte) (1 << (index % 8));
        byte[] value = oldValue.getValue();
        values.write(value, 0, value.length);
      }
      valueOffsets.putInt(values.size());
    }
    byte[] keyBytes = keys.toByteArray();
    byte[] valueBytes = values.toByteArray();
    int keyOffsetBytes = keyOffsets == null ? 0 : keyOffsets.capacity();
    long bodyBytes = checkedAdd(keyOffsetBytes, keyBytes.length, presence.length,
        valueOffsets.capacity(), valueBytes.length, "section body");
    long sectionBytes = checkedAdd(StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        bodyBytes, "section bytes");
    ByteBuffer section = ByteBuffer.allocate(checkedInt(sectionBytes, "section bytes"));
    section.putShort((short) group.storeId)
        .putShort(group.keyWidth == 0 ? VARIABLE_SECTION_LAYOUT : FIXED_SECTION_LAYOUT)
        .putInt(StateArchiveGethFormatV5.SECTION_HEADER_LENGTH).putInt(count)
        .putInt(group.keyWidth).putLong(keyBytes.length).putLong(valueBytes.length)
        .putLong(sectionBytes).putInt(0);
    section.putInt(crc32c(section.array(), 0, 44));
    int digestOffset = section.position();
    section.position(StateArchiveGethFormatV5.SECTION_HEADER_LENGTH);
    if (keyOffsets != null) {
      section.put(keyOffsets.array());
    }
    section.put(keyBytes).put(presence).put(valueOffsets.array()).put(valueBytes);
    byte[] digestInput = new byte[48 + checkedInt(bodyBytes, "section body")];
    System.arraycopy(section.array(), 0, digestInput, 0, 48);
    System.arraycopy(section.array(), StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        digestInput, 48, digestInput.length - 48);
    byte[] digest = StateArchiveFileFormatV3.sha256(SECTION_DOMAIN, digestInput);
    System.arraycopy(digest, 0, section.array(), digestOffset, digest.length);
    return section.array();
  }

  private DecodedSection decodeSection(ByteBuffer frame, int sectionsEnd, int laneId) {
    int start = frame.position();
    require(sectionsEnd - start >= StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        "section header length");
    int storeId = Short.toUnsignedInt(frame.getShort());
    int layout = Short.toUnsignedInt(frame.getShort());
    require(frame.getInt() == StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        "section header size");
    int count = frame.getInt();
    int keyWidth = frame.getInt();
    long keysBytes = frame.getLong();
    long valuesBytes = frame.getLong();
    long sectionBytes = frame.getLong();
    require(frame.getInt() == 0, "section flags");
    require(frame.getInt() == crc32c(frame.array(), start, 44), "section header checksum");
    byte[] storedDigest = new byte[32];
    frame.get(storedDigest);
    require(count > 0 && keysBytes >= 0 && valuesBytes >= 0
            && sectionBytes >= StateArchiveGethFormatV5.SECTION_HEADER_LENGTH
            && sectionBytes <= sectionsEnd - start,
        "section bounds");
    int expectedLane;
    try {
      expectedLane = StateArchiveFileFormatV3.laneId(storeId);
    } catch (IllegalArgumentException failure) {
      throw invalid("Store ID", failure);
    }
    require(expectedLane == laneId, "Store lane");
    int expectedWidth = StateArchiveFileFormatV3.fixedKeyWidth(expectedLane);
    require(keyWidth == expectedWidth
            && ((keyWidth == 0 && layout == VARIABLE_SECTION_LAYOUT)
                || (keyWidth != 0 && layout == FIXED_SECTION_LAYOUT)),
        "section layout");
    int bodyBytes = checkedInt(sectionBytes - StateArchiveGethFormatV5.SECTION_HEADER_LENGTH,
        "section body");
    require(bodyBytes <= frame.remaining(), "section body bounds");
    byte[] body = new byte[bodyBytes];
    frame.get(body);
    byte[] digestInput = new byte[48 + body.length];
    System.arraycopy(frame.array(), start, digestInput, 0, 48);
    System.arraycopy(body, 0, digestInput, 48, body.length);
    require(Arrays.equals(storedDigest,
        StateArchiveFileFormatV3.sha256(SECTION_DOMAIN, digestInput)), "section digest");
    List<Entry> entries = decodeSectionBody(storeId, count, keyWidth,
        checkedInt(keysBytes, "keys bytes"), checkedInt(valuesBytes, "values bytes"), body);
    return new DecodedSection(storeId, entries, storedDigest);
  }

  private List<Entry> decodeSectionBody(int storeId, int count, int keyWidth,
      int keysBytes, int valuesBytes, byte[] body) {
    int keyOffsetsBytes = keyWidth == 0
        ? checkedInt(checkedMultiply(count + 1L, Integer.BYTES, "key offsets"),
            "key offsets") : 0;
    int presenceBytes = (count + 7) / 8;
    int valueOffsetsBytes = checkedInt(
        checkedMultiply(count + 1L, Integer.BYTES, "value offsets"), "value offsets");
    long expectedBody = checkedAdd(keyOffsetsBytes, keysBytes, presenceBytes,
        valueOffsetsBytes, valuesBytes, "section body");
    require(expectedBody == body.length, "section body length");
    if (keyWidth != 0) {
      require(keysBytes == checkedMultiply(count, keyWidth, "fixed keys"),
          "fixed keys length");
    }
    ByteBuffer bytes = ByteBuffer.wrap(body);
    int[] keyOffsets = keyWidth == 0 ? readOffsets(bytes, count, keysBytes, "key") : null;
    byte[] keys = new byte[keysBytes];
    bytes.get(keys);
    byte[] presence = new byte[presenceBytes];
    bytes.get(presence);
    if (count % 8 != 0) {
      int unusedMask = ~((1 << (count % 8)) - 1) & 0xff;
      require((presence[presence.length - 1] & unusedMask) == 0, "presence padding");
    }
    int[] valueOffsets = readOffsets(bytes, count, valuesBytes, "value");
    byte[] values = new byte[valuesBytes];
    bytes.get(values);
    require(!bytes.hasRemaining(), "section trailing bytes");
    List<Entry> entries = new ArrayList<>(count);
    byte[] previousKey = null;
    for (int index = 0; index < count; index++) {
      int keyStart = keyWidth == 0 ? keyOffsets[index] : index * keyWidth;
      int keyEnd = keyWidth == 0 ? keyOffsets[index + 1] : keyStart + keyWidth;
      byte[] key = Arrays.copyOfRange(keys, keyStart, keyEnd);
      require(previousKey == null || BlockReverseDiff.compareUnsigned(previousKey, key) < 0,
          "key order");
      previousKey = key;
      boolean present = (presence[index / 8] & (1 << (index % 8))) != 0;
      int valueStart = valueOffsets[index];
      int valueEnd = valueOffsets[index + 1];
      require(present || valueStart == valueEnd, "ABSENT value length");
      OldValue oldValue = present
          ? OldValue.present(Arrays.copyOfRange(values, valueStart, valueEnd))
          : OldValue.absent();
      entries.add(new Entry(key, oldValue));
    }
    return entries;
  }

  private int[] readOffsets(ByteBuffer bytes, int count, int terminal, String name) {
    int[] offsets = new int[count + 1];
    long previous = -1;
    for (int index = 0; index <= count; index++) {
      long current = Integer.toUnsignedLong(bytes.getInt());
      require(current <= terminal && current >= previous, name + " offsets");
      offsets[index] = (int) current;
      previous = current;
    }
    require(offsets[0] == 0 && offsets[count] == terminal, name + " offset endpoints");
    return offsets;
  }

  private byte[] laneItemDigest(int laneId, int sectionCount, long entryCount,
      long sectionsBytes, List<byte[]> sectionDigests) {
    ByteBuffer identity = ByteBuffer.allocate(2 * Short.BYTES + Integer.BYTES + Long.BYTES
        + sectionDigests.size() * 32);
    identity.putShort((short) laneId).putShort((short) sectionCount)
        .putInt(checkedInt(entryCount, "entry count")).putLong(sectionsBytes);
    for (byte[] digest : sectionDigests) {
      identity.put(digest);
    }
    return StateArchiveFileFormatV3.sha256(LANE_ITEM_DOMAIN, identity.array());
  }

  private static byte[] requireHash(byte[] hash, String name) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException("State Archive V5 " + name + " length mismatch");
    }
    return Arrays.copyOf(hash, hash.length);
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static long checkedAdd(long left, long right, String name) {
    try {
      return StrictMathWrapper.addExact(left, right);
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 " + name + " overflow", failure);
    }
  }

  private static long checkedAdd(long a, long b, long c, String name) {
    return checkedAdd(checkedAdd(a, b, name), c, name);
  }

  private static long checkedAdd(long a, long b, long c, long d, long e, String name) {
    return checkedAdd(checkedAdd(checkedAdd(a, b, name), checkedAdd(c, d, name), name),
        e, name);
  }

  private static long checkedMultiply(long left, long right, String name) {
    try {
      return StrictMathWrapper.multiplyExact(left, right);
    } catch (ArithmeticException failure) {
      throw new IllegalArgumentException("State Archive V5 " + name + " overflow", failure);
    }
  }

  private static int checkedInt(long value, String name) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("State Archive V5 " + name + " exceeds int");
    }
    return (int) value;
  }

  private static void require(boolean condition, String name) {
    if (!condition) {
      throw invalid(name);
    }
  }

  private static IllegalArgumentException invalid(String name) {
    return new IllegalArgumentException("State Archive V5 invalid " + name);
  }

  private static IllegalArgumentException invalid(String name, Exception cause) {
    return new IllegalArgumentException("State Archive V5 invalid " + name, cause);
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  private static final class StoreGroup {
    private final int storeId;
    private final int keyWidth;
    private final List<Entry> entries;

    private StoreGroup(int storeId, int keyWidth, List<Entry> entries) {
      this.storeId = storeId;
      this.keyWidth = keyWidth;
      this.entries = entries;
    }
  }

  private static final class DecodedSection {
    private final int storeId;
    private final List<Entry> entries;
    private final byte[] digest;

    private DecodedSection(int storeId, List<Entry> entries, byte[] digest) {
      this.storeId = storeId;
      this.entries = entries;
      this.digest = digest;
    }
  }

  static final class EncodedFrame {
    private final int laneId;
    private final long blockNumber;
    private final byte[] blockHash;
    private final byte[] bytes;
    private final byte[] frameDigest;
    private final byte[] laneItemDigest;
    private final int sectionCount;
    private final long entryCount;

    private EncodedFrame(int laneId, long blockNumber, byte[] blockHash, byte[] bytes,
        byte[] frameDigest, byte[] laneItemDigest, int sectionCount, long entryCount) {
      this.laneId = laneId;
      this.blockNumber = blockNumber;
      this.blockHash = Arrays.copyOf(blockHash, blockHash.length);
      this.bytes = Arrays.copyOf(bytes, bytes.length);
      this.frameDigest = Arrays.copyOf(frameDigest, frameDigest.length);
      this.laneItemDigest = Arrays.copyOf(laneItemDigest, laneItemDigest.length);
      this.sectionCount = sectionCount;
      this.entryCount = entryCount;
    }

    int getLaneId() { return laneId; }
    long getBlockNumber() { return blockNumber; }
    byte[] getBlockHash() { return Arrays.copyOf(blockHash, blockHash.length); }
    byte[] getBytes() { return Arrays.copyOf(bytes, bytes.length); }
    byte[] getFrameDigest() { return Arrays.copyOf(frameDigest, frameDigest.length); }
    byte[] getLaneItemDigest() { return Arrays.copyOf(laneItemDigest, laneItemDigest.length); }
    int getSectionCount() { return sectionCount; }
    long getEntryCount() { return entryCount; }
  }

  static final class DecodedFrame {
    private final int laneId;
    private final long blockNumber;
    private final byte[] blockHash;
    private final List<DbGroup> groups;
    private final byte[] frameDigest;
    private final byte[] laneItemDigest;
    private final long entryCount;

    private DecodedFrame(int laneId, long blockNumber, byte[] blockHash,
        List<DbGroup> groups, byte[] frameDigest, byte[] laneItemDigest, long entryCount) {
      this.laneId = laneId;
      this.blockNumber = blockNumber;
      this.blockHash = Arrays.copyOf(blockHash, blockHash.length);
      this.groups = Collections.unmodifiableList(new ArrayList<>(groups));
      this.frameDigest = Arrays.copyOf(frameDigest, frameDigest.length);
      this.laneItemDigest = Arrays.copyOf(laneItemDigest, laneItemDigest.length);
      this.entryCount = entryCount;
    }

    int getLaneId() { return laneId; }
    long getBlockNumber() { return blockNumber; }
    byte[] getBlockHash() { return Arrays.copyOf(blockHash, blockHash.length); }
    List<DbGroup> getGroups() { return groups; }
    byte[] getFrameDigest() { return Arrays.copyOf(frameDigest, frameDigest.length); }
    byte[] getLaneItemDigest() { return Arrays.copyOf(laneItemDigest, laneItemDigest.length); }
    long getEntryCount() { return entryCount; }
  }
}
