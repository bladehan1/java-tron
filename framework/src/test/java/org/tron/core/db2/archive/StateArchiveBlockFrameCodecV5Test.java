package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveBlockFrameCodecV5.DecodedFrame;
import org.tron.core.db2.archive.StateArchiveBlockFrameCodecV5.EncodedFrame;

public class StateArchiveBlockFrameCodecV5Test {

  private static final String EMPTY_FRAME_HEX =
      "5341463500050000000000000000002a00000000000000000000000000000000"
          + "0000000000000000000000000000002a0000000000000000f0da6a56e01daa75"
          + "bd125f2293be14930c7e91f3578950edba7559fe86c79f3e6ad326d62910bb7b";
  private static final String VARIABLE_FRAME_HASH =
      "59100e71e8deb0fb71dfb5018f63ca0b059ce1e6bae23812ec35316a9c2d26f4";
  private static final String FIXED_FRAME_HASH =
      "bb1c7516dea51b7298a6ba5fa2f112a32233a74dedeeebee9e5800bbd79108c6";
  private final StateArchiveBlockFrameCodecV5 codec = new StateArchiveBlockFrameCodecV5();

  @Test
  public void encodesUnifiedEmptyFrameWithGoldenBytes() {
    EncodedFrame encoded = codec.encode(diff(42, Collections.emptyList()), 0);

    assertEquals(96, encoded.getBytes().length);
    assertEquals(0, encoded.getSectionCount());
    assertEquals(0, encoded.getEntryCount());
    assertEquals(EMPTY_FRAME_HEX, hex(encoded.getBytes()));

    DecodedFrame decoded = codec.decode(encoded.getBytes());
    assertEquals(0, decoded.getLaneId());
    assertEquals(42, decoded.getBlockNumber());
    assertArrayEquals(hash(42), decoded.getBlockHash());
    assertEquals(0, decoded.getGroups().size());
    assertArrayEquals(encoded.getFrameDigest(), decoded.getFrameDigest());
    assertArrayEquals(encoded.getLaneItemDigest(), decoded.getLaneItemDigest());
  }

  @Test
  public void roundTripsVariableAndFixedSectionsWithThreeOldValueStates() {
    BlockReverseDiff input = diff(77, Arrays.asList(
        new DbGroup("abi", Arrays.asList(
            new Entry(bytes(0), OldValue.absent()),
            new Entry(bytes(0x80), OldValue.present(new byte[0])),
            new Entry(bytes(0xff, 1), OldValue.present(bytes(7, 8))))),
        new DbGroup("account", Arrays.asList(
            new Entry(fixedKey(21, 1), OldValue.absent()),
            new Entry(fixedKey(21, 2), OldValue.present(new byte[0])),
            new Entry(fixedKey(21, 3), OldValue.present(bytes(9))))),
        new DbGroup("account-asset", Collections.singletonList(
            new Entry(bytes(3, 4, 5), OldValue.present(bytes(6))))),
        new DbGroup("storage-row", Collections.singletonList(
            new Entry(fixedKey(32, 4), OldValue.present(bytes(10, 11)))))));

    EncodedFrame variable = codec.encode(input, 0);
    EncodedFrame fixed = codec.encode(input, 4);
    assertEquals(VARIABLE_FRAME_HASH,
        hex(StateArchiveFileFormatV3.sha256(variable.getBytes())));
    assertEquals(FIXED_FRAME_HASH,
        hex(StateArchiveFileFormatV3.sha256(fixed.getBytes())));
    assertGroups(Collections.singletonList(group(input, "abi")),
        codec.decode(variable.getBytes()).getGroups());
    assertGroups(Collections.singletonList(group(input, "account")),
        codec.decode(fixed.getBytes()).getGroups());
    assertGroups(Collections.singletonList(group(input, "account-asset")),
        codec.decode(codec.encode(input, 5).getBytes()).getGroups());
    assertGroups(Collections.singletonList(group(input, "storage-row")),
        codec.decode(codec.encode(input, 22).getBytes()).getGroups());
    assertEquals(0, codec.decode(codec.encode(input, 13).getBytes()).getGroups().size());
  }

  @Test
  public void roundTripsAllExact27StoresThroughAssignedLanes() {
    ArchiveParticipantDescriptor descriptor = ArchiveParticipantDescriptor.current();
    List<DbGroup> groups = new ArrayList<>();
    for (String name : descriptor.getActiveDatabases()) {
      int storeId = descriptor.getStoreId(name);
      int laneId = StateArchiveFileFormatV3.laneId(storeId);
      int keyWidth = StateArchiveFileFormatV3.fixedKeyWidth(laneId);
      byte[] key = keyWidth == 0 ? bytes(storeId, 255 - storeId)
          : fixedKey(keyWidth, storeId);
      OldValue value = storeId % 3 == 0 ? OldValue.absent()
          : storeId % 3 == 1 ? OldValue.present(new byte[0])
          : OldValue.present(bytes(storeId));
      groups.add(new DbGroup(name, Collections.singletonList(new Entry(key, value))));
    }
    BlockReverseDiff input = diff(91, groups);

    List<DbGroup> decoded = new ArrayList<>();
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      decoded.addAll(codec.decode(codec.encode(input, laneId).getBytes()).getGroups());
    }
    decoded.sort((left, right) -> Integer.compare(
        StateArchiveFileFormatV3.storeId(left.getDbName()),
        StateArchiveFileFormatV3.storeId(right.getDbName())));
    List<DbGroup> expected = new ArrayList<>(input.getGroups());
    expected.sort((left, right) -> Integer.compare(
        StateArchiveFileFormatV3.storeId(left.getDbName()),
        StateArchiveFileFormatV3.storeId(right.getDbName())));
    assertGroups(expected, decoded);
  }

  @Test
  public void isDeterministicAndRejectsInvalidKeysAndCorruption() {
    List<Entry> entries = Arrays.asList(
        new Entry(bytes(0xff), OldValue.present(bytes(3))),
        new Entry(bytes(0), OldValue.absent()),
        new Entry(bytes(0x80), OldValue.present(new byte[0])));
    BlockReverseDiff first = diff(12, Arrays.asList(
        new DbGroup("votes", entries),
        new DbGroup("abi", Collections.singletonList(
            new Entry(bytes(4), OldValue.present(bytes(5)))))));
    List<Entry> reversed = new ArrayList<>(entries);
    Collections.reverse(reversed);
    BlockReverseDiff second = diff(12, Arrays.asList(
        new DbGroup("abi", Collections.singletonList(
            new Entry(bytes(4), OldValue.present(bytes(5))))),
        new DbGroup("votes", reversed)));
    byte[] canonical = codec.encode(first, 0).getBytes();
    assertArrayEquals(canonical, codec.encode(second, 0).getBytes());

    BlockReverseDiff badWidth = diff(12, Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(fixedKey(20, 1), OldValue.absent())))));
    assertThrows(IllegalArgumentException.class, () -> codec.encode(badWidth, 4));
    BlockReverseDiff duplicates = diff(12, Collections.singletonList(
        new DbGroup("abi", Arrays.asList(
            new Entry(bytes(1), OldValue.absent()),
            new Entry(bytes(1), OldValue.present(bytes(2)))))));
    assertThrows(IllegalArgumentException.class, () -> codec.encode(duplicates, 0));

    assertCorrupt(canonical, 8);
    assertCorrupt(canonical, 64);
    assertCorrupt(canonical, canonical.length - 8);
  }

  private void assertCorrupt(byte[] canonical, int offset) {
    byte[] corrupted = Arrays.copyOf(canonical, canonical.length);
    corrupted[offset] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(corrupted));
  }

  private static BlockReverseDiff diff(long blockNumber, List<DbGroup> groups) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber, hash((int) blockNumber),
        hash((int) blockNumber - 1), blockNumber * 3_000), groups);
  }

  private static DbGroup group(BlockReverseDiff diff, String name) {
    for (DbGroup group : diff.getGroups()) {
      if (group.getDbName().equals(name)) {
        return group;
      }
    }
    throw new AssertionError("Missing group " + name);
  }

  private static void assertGroups(List<DbGroup> expected, List<DbGroup> actual) {
    assertEquals(expected.size(), actual.size());
    for (int groupIndex = 0; groupIndex < expected.size(); groupIndex++) {
      DbGroup expectedGroup = expected.get(groupIndex);
      DbGroup actualGroup = actual.get(groupIndex);
      assertEquals(expectedGroup.getDbName(), actualGroup.getDbName());
      assertEquals(expectedGroup.getEntries().size(), actualGroup.getEntries().size());
      for (int entryIndex = 0; entryIndex < expectedGroup.getEntries().size(); entryIndex++) {
        Entry expectedEntry = expectedGroup.getEntries().get(entryIndex);
        Entry actualEntry = actualGroup.getEntries().get(entryIndex);
        assertArrayEquals(expectedEntry.getKey(), actualEntry.getKey());
        assertEquals(expectedEntry.getOldValue(), actualEntry.getOldValue());
      }
    }
  }

  private static byte[] fixedKey(int length, int suffix) {
    byte[] key = new byte[length];
    key[length - 1] = (byte) suffix;
    return key;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(int... values) {
    byte[] bytes = new byte[values.length];
    for (int index = 0; index < values.length; index++) {
      bytes[index] = (byte) values[index];
    }
    return bytes;
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte element : value) {
      encoded.append(String.format("%02x", element));
    }
    return encoded.toString();
  }
}
