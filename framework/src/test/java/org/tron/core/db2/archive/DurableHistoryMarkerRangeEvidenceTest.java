package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DurableHistoryMarkerRangeEvidenceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void readsOnlyExactDurableRangeAndReopensWithIdenticalEvidence() throws Exception {
    Path archive = temporaryFolder.newFolder("marker-evidence").toPath();
    List<BlockSnapshotMeta> expected = Arrays.asList(meta(2), meta(3));
    List<byte[]> encoded = new ArrayList<>();
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
        archive, 4096, new java.util.LinkedHashSet<>(participants()))) {
      writer.acceptAll(Arrays.asList(diff(1), diff(2), diff(3), diff(4)));
      List<HistoryCommitMarker> evidence =
          writer.createMarkerRangeEvidence(2).read(expected);
      assertEquals(Arrays.asList(2L, 3L), epochs(evidence));
      HistoryCommitMarkerCodec codec = new HistoryCommitMarkerCodec();
      evidence.forEach(marker -> encoded.add(codec.encode(marker)));
    }

    try (ArchiveHistoryWriter reopened = new ArchiveHistoryWriter(
        archive, 4096, new java.util.LinkedHashSet<>(participants()))) {
      List<HistoryCommitMarker> evidence =
          reopened.createMarkerRangeEvidence(2).read(expected);
      HistoryCommitMarkerCodec codec = new HistoryCommitMarkerCodec();
      assertArrayEquals(encoded.get(0), codec.encode(evidence.get(0)));
      assertArrayEquals(encoded.get(1), codec.encode(evidence.get(1)));
      assertEquals(4L, reopened.committedHead().getMeta().getEpoch());
    }
  }

  @Test
  public void markerPreflightRejectsMissingSubstitutedAndReorderedBeforeBodyRead() {
    FakeSource source = new FakeSource();
    source.put(marker(meta(1)), diff(1));
    DurableHistoryMarkerRangeEvidence evidence =
        new DurableHistoryMarkerRangeEvidence(source, 2);
    List<BlockSnapshotMeta> expected = Arrays.asList(meta(1), meta(2));

    assertThrows(ArchivePersistenceException.class, () -> evidence.read(expected));
    assertEquals(0, source.bodyReads);

    source.putAt(2, marker(meta(3)), diff(2));
    assertThrows(ArchivePersistenceException.class, () -> evidence.read(expected));
    assertEquals(0, source.bodyReads);

    source.putAt(1, marker(meta(2)), diff(1));
    source.putAt(2, marker(meta(1)), diff(2));
    assertThrows(ArchivePersistenceException.class, () -> evidence.read(expected));
    assertEquals(0, source.bodyReads);

    source.clear();
    source.put(marker(meta(1)), diff(1));
    source.put(marker(meta(2)), diff(2));
    assertEquals(Arrays.asList(1L, 2L), epochs(evidence.read(expected)));
    assertEquals(2, source.bodyReads);
  }

  @Test
  public void referenceFailureAndMarkerDriftLeaveEvidenceRetryable() {
    FakeSource source = new FakeSource();
    source.put(marker(meta(1)), diff(1));
    source.failBody = true;
    DurableHistoryMarkerRangeEvidence evidence =
        new DurableHistoryMarkerRangeEvidence(source, 1);

    assertThrows(ArchivePersistenceException.class,
        () -> evidence.read(Collections.singletonList(meta(1))));
    source.failBody = false;
    assertEquals(1, evidence.read(Collections.singletonList(meta(1))).size());

    source.bodyReads = 0;
    source.driftAfterBody = true;
    assertThrows(ArchivePersistenceException.class,
        () -> evidence.read(Collections.singletonList(meta(1))));
    source.driftAfterBody = false;
    assertEquals(1, evidence.read(Collections.singletonList(meta(1))).size());
  }

  @Test
  public void invalidOrOversizedExpectedRangeFailsBeforeSourceAction() {
    FakeSource source = new FakeSource();
    DurableHistoryMarkerRangeEvidence evidence =
        new DurableHistoryMarkerRangeEvidence(source, 1);

    assertThrows(ArchivePersistenceException.class,
        () -> evidence.read(Collections.emptyList()));
    assertThrows(ArchivePersistenceException.class,
        () -> evidence.read(Arrays.asList(meta(1), meta(2))));
    assertThrows(ArchivePersistenceException.class,
        () -> new DurableHistoryMarkerRangeEvidence(source, 2)
            .read(Arrays.asList(meta(1), meta(3))));
    assertEquals(0, source.markerReads);
    assertEquals(0, source.bodyReads);
  }

  private static List<Long> epochs(List<HistoryCommitMarker> markers) {
    List<Long> epochs = new ArrayList<>();
    markers.forEach(marker -> epochs.add(marker.getMeta().getEpoch()));
    return epochs;
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return participants;
  }

  private static BlockReverseDiff diff(int epoch) {
    return new BlockReverseDiff(meta(epoch), Collections.emptyList());
  }

  private static BlockSnapshotMeta meta(int epoch) {
    return BlockSnapshotMeta.forBlock(epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L);
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta) {
    int epoch = (int) meta.getEpoch();
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100L, 100, epoch, bytes(32, epoch + 20)),
        new HistoryIndexLocation(epoch * 50L, 50, bytes(32, epoch + 30)),
        bytes(16, epoch + 40), participants());
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(int length, int value) {
    byte[] result = new byte[length];
    Arrays.fill(result, (byte) value);
    return result;
  }

  private static final class FakeSource implements DurableHistoryMarkerRangeEvidence.Source {
    private final Map<Long, HistoryCommitMarker> markers = new LinkedHashMap<>();
    private final Map<Long, BlockReverseDiff> bodies = new LinkedHashMap<>();
    private int markerReads;
    private int bodyReads;
    private boolean failBody;
    private boolean driftAfterBody;

    private void put(HistoryCommitMarker marker, BlockReverseDiff body) {
      putAt(marker.getMeta().getEpoch(), marker, body);
    }

    private void putAt(long epoch, HistoryCommitMarker marker, BlockReverseDiff body) {
      markers.put(epoch, marker);
      bodies.put(epoch, body);
    }

    private void clear() {
      markers.clear();
      bodies.clear();
      markerReads = 0;
      bodyReads = 0;
    }

    @Override
    public HistoryCommitMarker marker(long epoch) {
      markerReads++;
      if (driftAfterBody && bodyReads > 0) {
        return DurableHistoryMarkerRangeEvidenceTest.marker(meta((int) epoch + 1));
      }
      return markers.get(epoch);
    }

    @Override
    public BlockReverseDiff readCommitted(long epoch) {
      bodyReads++;
      if (failBody) {
        throw new ArchivePersistenceException("injected body/index reference failure");
      }
      return bodies.get(epoch);
    }
  }
}
