package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Bounded authoritative marker evidence for one exact frozen flush range. */
public final class DurableHistoryMarkerRangeEvidence {

  private final Source source;
  private final int maxMarkers;
  private final List<String> participants;
  private final HistoryCommitMarkerCodec codec = new HistoryCommitMarkerCodec();

  public DurableHistoryMarkerRangeEvidence(ArchiveHistoryWriter writer, int maxMarkers) {
    this(new WriterSource(writer), maxMarkers);
  }

  DurableHistoryMarkerRangeEvidence(Source source, int maxMarkers) {
    this.source = Objects.requireNonNull(source, "source");
    if (maxMarkers <= 0) {
      throw new IllegalArgumentException("maxMarkers must be positive");
    }
    this.maxMarkers = maxMarkers;
    participants = ArchiveParticipantDescriptor.current().getParticipants();
  }

  public List<HistoryCommitMarker> read(List<BlockSnapshotMeta> expectedMetas) {
    List<BlockSnapshotMeta> expected = new ArrayList<>(
        Objects.requireNonNull(expectedMetas, "expectedMetas"));
    validateExpectedRange(expected);

    List<HistoryCommitMarker> markers = new ArrayList<>(expected.size());
    for (BlockSnapshotMeta meta : expected) {
      HistoryCommitMarker marker = source.marker(meta.getEpoch());
      validateMarker(meta, marker);
      markers.add(marker);
    }

    List<HistoryCommitMarker> evidence = new ArrayList<>(markers.size());
    for (int i = 0; i < markers.size(); i++) {
      HistoryCommitMarker marker = markers.get(i);
      BlockReverseDiff body = source.readCommitted(marker.getMeta().getEpoch());
      if (body == null || !expected.get(i).equals(body.getMeta())) {
        throw new ArchivePersistenceException(
            "Committed history body does not match marker range");
      }
      HistoryCommitMarker reloaded = source.marker(marker.getMeta().getEpoch());
      validateMarker(expected.get(i), reloaded);
      if (!Arrays.equals(codec.encode(marker), codec.encode(reloaded))) {
        throw new ArchivePersistenceException("History marker changed while building evidence");
      }
      evidence.add(codec.decode(codec.encode(reloaded)));
    }
    return Collections.unmodifiableList(evidence);
  }

  private void validateExpectedRange(List<BlockSnapshotMeta> expected) {
    if (expected.isEmpty()) {
      throw new ArchivePersistenceException("Marker evidence range must not be empty");
    }
    if (expected.size() > maxMarkers) {
      throw new ArchivePersistenceException("Marker evidence range exceeds configured bound");
    }
    BlockSnapshotMeta previous = null;
    for (BlockSnapshotMeta meta : expected) {
      BlockSnapshotMeta current = Objects.requireNonNull(meta, "expectedMeta");
      if (previous != null && !isNext(previous, current)) {
        throw new ArchivePersistenceException("Expected marker evidence range is not contiguous");
      }
      previous = current;
    }
  }

  private void validateMarker(BlockSnapshotMeta expected, HistoryCommitMarker marker) {
    if (marker == null) {
      throw new ArchivePersistenceException(
          "Committed history marker is missing for epoch " + expected.getEpoch());
    }
    if (!expected.equals(marker.getMeta())) {
      throw new ArchivePersistenceException("Committed history marker target mismatch");
    }
    if (marker.getPreviousEpoch() != expected.getEpoch() - 1) {
      throw new ArchivePersistenceException("Committed history marker predecessor mismatch");
    }
    if (!participants.equals(marker.getDatabases())) {
      throw new ArchivePersistenceException("Committed history marker participant set mismatch");
    }
  }

  private static boolean isNext(BlockSnapshotMeta previous, BlockSnapshotMeta current) {
    return current.getEpoch() == previous.getEpoch() + 1
        && current.getBlockNumber() == previous.getBlockNumber() + 1
        && Arrays.equals(current.getParentHash(), previous.getBlockHash());
  }

  interface Source {
    HistoryCommitMarker marker(long epoch);

    BlockReverseDiff readCommitted(long epoch);
  }

  private static final class WriterSource implements Source {
    private final ArchiveHistoryWriter writer;

    private WriterSource(ArchiveHistoryWriter writer) {
      this.writer = Objects.requireNonNull(writer, "writer");
    }

    @Override
    public HistoryCommitMarker marker(long epoch) {
      return writer.committedMarker(epoch);
    }

    @Override
    public BlockReverseDiff readCommitted(long epoch) {
      return writer.readCommitted(epoch);
    }
  }
}
