package org.tron.core.db2.archive;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.StateArchiveBlockFrameCodecV5.DecodedFrame;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.PointLocation;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Full five-lane V5 replay source for the existing single serving-index owner. */
final class StateArchiveServingSourceV5 implements StateArchiveServingSource {

  static final long MAX_REPLAY_DATA_HANDLES = 5;

  private final Path root;
  private final long firstBlockNumber;
  private final byte[] baselineHistoryDigest;
  private final StateArchiveCanonicalBlockMetaSource canonical;
  private final StateArchiveBlockFrameCodecV5 codec = new StateArchiveBlockFrameCodecV5();
  private StateArchiveTailV5 tail;
  private CommonCheckpointTarget target;
  private boolean validated;

  StateArchiveServingSourceV5(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, byte[] baselineHistoryDigest,
      StateArchiveCanonicalBlockMetaSource canonical) {
    this(root, Objects.requireNonNull(tail, "tail").getFirstBlockNumber(),
        baselineHistoryDigest, canonical);
    updatePublishedUnchecked(tail, target);
  }

  StateArchiveServingSourceV5(Path root, long firstBlockNumber,
      byte[] baselineHistoryDigest, StateArchiveCanonicalBlockMetaSource canonical) {
    this.root = Objects.requireNonNull(root, "root");
    if (firstBlockNumber < 0) {
      throw new IllegalArgumentException("State Archive V5 first block is negative");
    }
    this.firstBlockNumber = firstBlockNumber;
    this.baselineHistoryDigest = requireHash(baselineHistoryDigest,
        "baseline history digest");
    this.canonical = Objects.requireNonNull(canonical, "canonical");
  }

  @Override
  public synchronized long getHistoryStartBlock() {
    return firstBlockNumber;
  }

  synchronized void updatePublished(StateArchiveTailV5 replacement,
      CommonCheckpointTarget replacementTarget) throws IOException {
    StateArchiveTailV5 admittedTail = Objects.requireNonNull(replacement, "tail");
    CommonCheckpointTarget admittedTarget = Objects.requireNonNull(
        replacementTarget, "target");
    try {
      admittedTail.requireTarget(admittedTarget);
    } catch (IllegalArgumentException failure) {
      throw invalid("tail differs from published Common target", failure);
    }
    if (admittedTail.getFirstBlockNumber() != firstBlockNumber) {
      throw invalid("published history start differs");
    }
    if (target != null) {
      if (target.equals(admittedTarget)) {
        if (!Arrays.equals(tail.encode(), admittedTail.encode())) {
          throw invalid("same Common target has a different tail");
        }
        return;
      }
      if (admittedTarget.getFirstBlock().getBlockNumber()
          != target.getLastBlock().getBlockNumber() + 1
          || !Arrays.equals(admittedTarget.getFirstBlock().getParentHash(),
              target.getLastBlock().getBlockHash())) {
        throw invalid("published target is not the current successor");
      }
    }
    tail = admittedTail;
    target = admittedTarget;
    validated = false;
  }

  @Override
  public synchronized List<BlockReverseDiff> readCommittedDiffs(long fromExclusive,
      long through, long maxEncodedBytes) throws IOException {
    requirePublished();
    requireRange(fromExclusive, through);
    if (maxEncodedBytes < 0) {
      throw new IllegalArgumentException("State Archive V5 serving budget is negative");
    }
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.openCommittedPrefix(
        root, tail, target)) {
      requireBudget(view, fromExclusive + 1, through, maxEncodedBytes);
      if (!validated) {
        List<BlockReverseDiff> result = validateCompleteHistory(
            view, fromExclusive + 1, through);
        validated = true;
        return result;
      }
      if (fromExclusive == through) {
        return Collections.emptyList();
      }
      List<BlockReverseDiff> result = new ArrayList<>();
      try (ReplayHandles handles = new ReplayHandles()) {
        for (long block = fromExclusive + 1; block <= through; block++) {
          result.add(decodeBlock(view, handles, block, requireCanonical(block)).diff);
        }
      }
      return Collections.unmodifiableList(result);
    }
  }

  private List<BlockReverseDiff> validateCompleteHistory(StateArchiveCommittedViewV5 view,
      long requestedFirst, long requestedThrough) throws IOException {
    byte[] rolling = Arrays.copyOf(baselineHistoryDigest, baselineHistoryDigest.length);
    BlockSnapshotMeta previous = null;
    Map<Integer, Long> currentFiles = new HashMap<>();
    List<BlockReverseDiff> requested = new ArrayList<>();
    try (ReplayHandles handles = new ReplayHandles()) {
      for (long block = tail.getFirstBlockNumber(); block <= tail.getCommonBlockNumber(); block++) {
        BlockSnapshotMeta meta = requireCanonical(block);
        if (meta.getEpoch() != block || meta.getBlockNumber() != block
            || previous != null && !Arrays.equals(meta.getParentHash(), previous.getBlockHash())) {
          throw invalid("canonical metadata is not contiguous at block " + block);
        }
        DecodedBlock decoded = decodeBlock(view, handles, block, meta);
        for (PointLocation location : decoded.locations) {
          Long previousFile = currentFiles.put(location.getLaneId(), location.getFileId());
          if (previousFile == null || previousFile.longValue() != location.getFileId()) {
            requireSegmentStart(handles, location, block, rolling);
          }
        }
        rolling = StateArchiveGethFormatV5.nextHistoryDigest(rolling,
            StateArchiveGethFormatV5.blockHistoryDigest(meta, decoded.laneItemDigests));
        previous = meta;
        if (block >= requestedFirst && block <= requestedThrough) {
          requested.add(decoded.diff);
        }
      }
    }
    if (previous == null || !previous.equals(target.getLastBlock())
        || !Arrays.equals(rolling, tail.getResultHistoryDigest())) {
      throw invalid("canonical coverage or rolling tail differs");
    }
    BlockSnapshotMeta targetFirst = requireCanonical(target.getFirstBlock().getBlockNumber());
    if (!targetFirst.equals(target.getFirstBlock())) {
      throw invalid("Common first-block metadata differs from canonical source");
    }
    return Collections.unmodifiableList(requested);
  }

  private DecodedBlock decodeBlock(StateArchiveCommittedViewV5 view,
      ReplayHandles handles, long block, BlockSnapshotMeta meta) throws IOException {
    int[] laneIds = StateArchiveGethFormatV5.laneIds();
    List<DbGroup> groups = new ArrayList<>();
    List<byte[]> laneItemDigests = new ArrayList<>(laneIds.length);
    List<PointLocation> locations = new ArrayList<>(laneIds.length);
    for (int laneId : laneIds) {
      PointLocation location = view.capture(laneId, block);
      DecodedFrame frame;
      try {
        frame = codec.decode(handles.read(location));
      } catch (IllegalArgumentException failure) {
        throw invalid("frame decode failed at block " + block + " lane " + laneId, failure);
      }
      if (frame.getLaneId() != laneId || frame.getBlockNumber() != block
          || !Arrays.equals(frame.getBlockHash(), meta.getBlockHash())) {
        throw invalid("lane bundle identity differs at block " + block + " lane " + laneId);
      }
      groups.addAll(frame.getGroups());
      laneItemDigests.add(frame.getLaneItemDigest());
      locations.add(location);
    }
    return new DecodedBlock(new BlockReverseDiff(meta, groups), laneItemDigests, locations);
  }

  private void requireSegmentStart(ReplayHandles handles, PointLocation location,
      long block, byte[] rolling) throws IOException {
    StateArchiveSegmentHeaderV5 header;
    try {
      header = StateArchiveSegmentHeaderV5.decode(handles.read(location, 0,
          StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
    } catch (IllegalArgumentException failure) {
      throw invalid("segment header decode failed", failure);
    }
    if (header.getLaneId() != location.getLaneId()
        || header.getFileId() != location.getFileId()
        || header.getFirstBlockNumber() != block
        || !Arrays.equals(header.getStartHistoryDigest(), rolling)) {
      throw invalid("segment start history differs at block " + block
          + " lane " + location.getLaneId());
    }
  }

  private void requireBudget(StateArchiveCommittedViewV5 view, long first, long through,
      long budget) throws IOException {
    long bytes = 0;
    for (long block = first; block <= through; block++) {
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        PointLocation location = view.capture(laneId, block);
        long length = location.getEndOffset() - location.getStartOffset();
        if (length > budget - bytes) {
          throw new ReadBudgetException();
        }
        bytes += length;
      }
    }
  }

  private BlockSnapshotMeta requireCanonical(long block) throws IOException {
    BlockSnapshotMeta meta = canonical.load(block);
    if (meta == null || meta.getBlockNumber() != block) {
      throw invalid("canonical metadata is missing at block " + block);
    }
    return meta;
  }

  private void requireRange(long fromExclusive, long through) {
    long minimum = firstBlockNumber - 1;
    if (fromExclusive < minimum || through < fromExclusive
        || through > tail.getCommonBlockNumber()) {
      throw new IllegalArgumentException("State Archive V5 serving range is outside [B0,W]");
    }
  }

  private static byte[] readExact(FileChannel channel, long position, int length)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int read = channel.read(bytes, position + bytes.position());
      if (read < 0) {
        throw new EOFException("State Archive V5 serving data is truncated");
      }
      if (read == 0) {
        throw invalid("serving read made no progress");
      }
    }
    return bytes.array();
  }

  private static byte[] requireHash(byte[] value, String name) {
    if (value == null || value.length != StateArchiveFileFormatV3.HASH_LENGTH) {
      throw new IllegalArgumentException("State Archive V5 " + name + " length mismatch");
    }
    return Arrays.copyOf(value, value.length);
  }

  private static IOException invalid(String message) {
    return new IOException("State Archive V5 serving source " + message);
  }

  private static IOException invalid(String message, Exception cause) {
    return new IOException("State Archive V5 serving source " + message, cause);
  }

  private void requirePublished() throws IOException {
    if (tail == null || target == null) {
      throw invalid("has no published tail");
    }
  }

  private void updatePublishedUnchecked(StateArchiveTailV5 initial,
      CommonCheckpointTarget initialTarget) {
    try {
      updatePublished(initial, initialTarget);
    } catch (IOException failure) {
      throw new IllegalArgumentException(failure.getMessage(), failure);
    }
  }

  private static final class DecodedBlock {
    private final BlockReverseDiff diff;
    private final List<byte[]> laneItemDigests;
    private final List<PointLocation> locations;

    private DecodedBlock(BlockReverseDiff diff, List<byte[]> laneItemDigests,
        List<PointLocation> locations) {
      this.diff = diff;
      this.laneItemDigests = laneItemDigests;
      this.locations = locations;
    }
  }

  private static final class ReplayHandles implements AutoCloseable {
    private final Map<Integer, OpenData> open = new HashMap<>();

    private byte[] read(PointLocation location) throws IOException {
      long length = location.getEndOffset() - location.getStartOffset();
      if (length > Integer.MAX_VALUE) {
        throw invalid("frame length exceeds Java array");
      }
      return read(location, location.getStartOffset(), (int) length);
    }

    private byte[] read(PointLocation location, long position, int length)
        throws IOException {
      OpenData current = open.get(location.getLaneId());
      if (current == null || current.fileId != location.getFileId()) {
        if (current != null) {
          current.channel.close();
        }
        current = new OpenData(location.getFileId(), FileChannel.open(
            location.getDataPath(), StandardOpenOption.READ));
        open.put(location.getLaneId(), current);
      }
      return readExact(current.channel, position, length);
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (OpenData data : open.values()) {
        try {
          data.channel.close();
        } catch (IOException closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else {
            failure.addSuppressed(closeFailure);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static final class OpenData {
    private final long fileId;
    private final FileChannel channel;

    private OpenData(long fileId, FileChannel channel) {
      this.fileId = fileId;
      this.channel = channel;
    }
  }
}
