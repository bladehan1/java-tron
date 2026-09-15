package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class BlockHistoryCodecTest {

  private final BlockHistoryCodec codec = new BlockHistoryCodec();

  @Test
  public void encodesDeterministicallyAndRoundTripsValueStates() {
    BlockReverseDiff first = diff(Arrays.asList(
        new DbGroup("votes", Arrays.asList(
            new Entry(bytes("z"), OldValue.present(bytes("value"))),
            new Entry(bytes("a"), OldValue.absent()))),
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("empty"), OldValue.present(new byte[0]))))));
    BlockReverseDiff reordered = diff(Arrays.asList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("empty"), OldValue.present(new byte[0])))),
        new DbGroup("votes", Arrays.asList(
            new Entry(bytes("a"), OldValue.absent()),
            new Entry(bytes("z"), OldValue.present(bytes("value")))))));

    byte[] encoded = codec.encode(first);
    assertArrayEquals(encoded, codec.encode(reordered));
    assertEquals(encoded.length,
        codec.recordLength(Arrays.copyOf(encoded, BlockHistoryCodec.HEADER_LENGTH)));

    BlockReverseDiff decoded = codec.decode(encoded);
    assertEquals(first.getMeta(), decoded.getMeta());
    assertEquals(2, decoded.getGroups().size());
    Entry empty = decoded.getGroups().get(0).getEntries().get(0);
    assertTrue(empty.getOldValue().isPresent());
    assertEquals(0, empty.getOldValue().getValue().length);
    Entry absent = decoded.getGroups().get(1).getEntries().get(0);
    assertFalse(absent.getOldValue().isPresent());
    assertArrayEquals(bytes("value"),
        decoded.getGroups().get(1).getEntries().get(1).getOldValue().getValue());
  }

  @Test
  public void rejectsCorruptionTruncationAndOversizedRecords() {
    byte[] encoded = codec.encode(diff(Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key"), OldValue.present(bytes("value"))))))));
    byte[] corrupted = Arrays.copyOf(encoded, encoded.length);
    corrupted[BlockHistoryCodec.HEADER_LENGTH] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(corrupted));
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(Arrays.copyOf(encoded, encoded.length - 1)));

    BlockHistoryCodec smallCodec = new BlockHistoryCodec(180);
    byte[] incompressible = new byte[512];
    new java.util.Random(17L).nextBytes(incompressible);
    BlockReverseDiff oversized = diff(Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key"), OldValue.present(incompressible))))));
    assertThrows(IllegalArgumentException.class, () -> smallCodec.encode(oversized));
  }

  @Test
  public void preservesNoopBlockMetadata() {
    BlockReverseDiff empty = diff(Collections.emptyList());
    BlockReverseDiff decoded = codec.decode(codec.encode(empty));
    assertEquals(empty.getMeta(), decoded.getMeta());
    assertTrue(decoded.getGroups().isEmpty());
  }

  private static BlockReverseDiff diff(java.util.List<DbGroup> groups) {
    return new BlockReverseDiff(new BlockSnapshotMeta(
        12, 12, hash(12), hash(11), 36_000L), groups);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}
