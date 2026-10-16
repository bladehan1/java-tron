package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ArchiveWalBindingTest {

  private final ArchiveWalBindingCodec codec = new ArchiveWalBindingCodec();

  @Test
  public void roundTripsContiguousHistoryIdentityAndCheckpointEntry() {
    ArchiveWalBinding binding = ArchiveWalBinding.fromMarkers(Arrays.asList(
        marker(7, 6), marker(8, 7)));

    ArchiveWalBinding decoded = codec.decode(codec.encode(binding));

    assertEquals(7, decoded.getFirst().getEpoch());
    assertEquals(8, decoded.getLast().getEpoch());
    assertEquals(6, decoded.getPredecessorEpoch());
    assertArrayEquals(hash(6), decoded.getPredecessorHash());
    assertArrayEquals(binding.getBatchDigest(), decoded.getBatchDigest());
    assertArrayEquals(binding.getStoreScopeDigest(), decoded.getStoreScopeDigest());
    assertArrayEquals(binding.getHistoryRefsDigest(), decoded.getHistoryRefsDigest());
    assertArrayEquals(binding.getBlockIndexRefsDigest(), decoded.getBlockIndexRefsDigest());

    Map<byte[], byte[]> checkpoint = new LinkedHashMap<>();
    checkpoint.put(ArchiveWalBinding.getCheckpointKey(), codec.encode(binding));
    assertNotNull(ArchiveWalBinding.fromCheckpointBatch(checkpoint));
  }

  @Test
  public void rejectsCorruptionAndNonContiguousMarkers() {
    byte[] encoded = codec.encode(ArchiveWalBinding.fromMarkers(
        Collections.singletonList(marker(7, 6))));
    encoded[encoded.length - 5] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));

    List<HistoryCommitMarker> gap = Arrays.asList(marker(7, 6), marker(9, 8));
    assertThrows(IllegalArgumentException.class, () -> ArchiveWalBinding.fromMarkers(gap));
  }

  private static HistoryCommitMarker marker(int epoch, int previousEpoch) {
    return new HistoryCommitMarker(
        new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(previousEpoch), epoch * 1_000L),
        previousEpoch, new HistoryLocation(0, epoch * 100L, 80, epoch, hash(epoch + 20)),
        new HistoryIndexLocation(epoch * 120L, 96, hash(epoch + 40)),
        Arrays.copyOf(hash(epoch + 60), 16), Arrays.asList("account", "properties"));
  }

  private static byte[] hash(int suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }
}
