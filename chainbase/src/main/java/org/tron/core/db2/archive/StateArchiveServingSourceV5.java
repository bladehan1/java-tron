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
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.ServingFailure.Reason;
import org.tron.core.db2.archive.ServingFailure.ServingFaultException;
import org.tron.core.db2.archive.StateArchiveBlockFrameCodecV5.DecodedFrame;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.PointLocation;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/**
 * Five-lane V5 replay source and the committed History view of serving (ch04 §3.3.1 rule 4).
 *
 * <p>The published boundary is an immutable {@link Committed} behind a volatile reference: the
 * History participant replaces it in {@code afterCommit} (single writer, O(1)); the serving build
 * thread and readers take it without any lock (L3). Canonical parent hashes and timestamps come
 * from the Block Store; a missing block is SOURCE_UNAVAILABLE, a frame/canonical identity
 * mismatch or an undecodable committed frame is SOURCE_CORRUPT (ch04 §5.1).
 */
final class StateArchiveServingSourceV5 implements ServingHistorySource, StateArchiveServingSource {

  static final long MAX_REPLAY_DATA_HANDLES = 5;

  private final Path root;
  private final long firstBlockNumber;
  private final byte[] baselineParentHash;
  private final StateArchiveCanonicalBlockMetaSource canonical;
  private volatile Committed published;

  StateArchiveServingSourceV5(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, byte[] baselineParentHash,
      StateArchiveCanonicalBlockMetaSource canonical) {
    this(root, Objects.requireNonNull(tail, "tail").getFirstBlockNumber(),
        baselineParentHash, canonical);
    updatePublishedUnchecked(tail, target);
  }

  StateArchiveServingSourceV5(Path root, long firstBlockNumber,
      byte[] baselineParentHash, StateArchiveCanonicalBlockMetaSource canonical) {
    this.root = Objects.requireNonNull(root, "root");
    if (firstBlockNumber < 0) {
      throw new IllegalArgumentException("State Archive V5 first block is negative");
    }
    this.firstBlockNumber = firstBlockNumber;
    this.baselineParentHash = requireHash(baselineParentHash, "baseline parent hash");
    this.canonical = Objects.requireNonNull(canonical, "canonical");
    this.published = new Committed(firstBlockNumber, this.baselineParentHash, null, null);
  }

  public long getHistoryStartBlock() {
    return firstBlockNumber;
  }

  @Override
  public Committed committed() {
    return published;
  }

  /** Single writer (the History participant); readers never take this monitor. */
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
    Committed current = published;
    CommonCheckpointTarget target = current.getTarget();
    if (target != null) {
      if (target.equals(admittedTarget)) {
        if (!Arrays.equals(current.getTail().encode(), admittedTail.encode())) {
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
    published = new Committed(firstBlockNumber, baselineParentHash, admittedTail,
        admittedTarget);
  }

  /** Decodes {@code (fromExclusive, through]} within an encoded-byte budget (tests, tools). */
  public List<BlockReverseDiff> readCommittedDiffs(long fromExclusive,
      long through, long maxEncodedBytes) throws IOException {
    if (maxEncodedBytes < 0) {
      throw new IllegalArgumentException("State Archive V5 serving budget is negative");
    }
    Committed view = published;
    requireRange(view, fromExclusive, through);
    if (fromExclusive == through) {
      requirePublished(view);
      return Collections.emptyList();
    }
    long[] sizes = bundleBytes(view, fromExclusive + 1, through);
    long bytes = 0;
    for (long size : sizes) {
      if (size > maxEncodedBytes - bytes) {
        throw new ReadBudgetException();
      }
      bytes += size;
    }
    List<BlockReverseDiff> result = new ArrayList<>();
    for (CommittedBlock block : read(view, fromExclusive + 1, through)) {
      result.add(block.getDiff());
    }
    return Collections.unmodifiableList(result);
  }

  @Override
  public long[] bundleBytes(Committed view, long first, long last) throws IOException {
    requireRange(view, first - 1, last);
    long[] sizes = new long[StrictMathWrapper.toIntExact(last - first + 1)];
    try (StateArchiveCommittedViewV5 committed = open(view)) {
      for (long block = first; block <= last; block++) {
        long bytes = 0;
        for (int laneId : StateArchiveGethFormatV5.laneIds()) {
          PointLocation location = committed.capture(laneId, block);
          bytes += location.getEndOffset() - location.getStartOffset();
        }
        sizes[(int) (block - first)] = bytes;
      }
    } catch (ServingFaultException fault) {
      throw fault;
    } catch (IOException unavailable) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "History block index read failed", unavailable);
    }
    return sizes;
  }

  @Override
  public List<CommittedBlock> read(Committed view, long first, long last) throws IOException {
    requireRange(view, first - 1, last);
    List<CommittedBlock> result = new ArrayList<>();
    try (StateArchiveCommittedViewV5 committed = open(view);
        ReplayHandles handles = new ReplayHandles()) {
      for (long block = first; block <= last; block++) {
        result.add(decodeBlock(committed, handles, block, requireCanonical(block)));
      }
    } catch (ServingFaultException fault) {
      throw fault;
    } catch (IOException unavailable) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "History committed body read failed", unavailable);
    }
    return Collections.unmodifiableList(result);
  }

  @Override
  public byte[] blockHash(Committed view, long block) throws IOException {
    if (block == firstBlockNumber - 1) {
      return Arrays.copyOf(baselineParentHash, baselineParentHash.length);
    }
    requireRange(view, block - 1, block);
    try (StateArchiveCommittedViewV5 committed = open(view);
        ReplayHandles handles = new ReplayHandles()) {
      int smallest = -1;
      long smallestBytes = Long.MAX_VALUE;
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        PointLocation location = committed.capture(laneId, block);
        long bytes = location.getEndOffset() - location.getStartOffset();
        if (bytes < smallestBytes) {
          smallestBytes = bytes;
          smallest = laneId;
        }
      }
      DecodedFrame frame = decodeFrame(committed, handles, smallest, block);
      return frame.getBlockHash();
    } catch (ServingFaultException fault) {
      throw fault;
    } catch (IOException unavailable) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "History committed frame read failed", unavailable);
    }
  }

  private StateArchiveCommittedViewV5 open(Committed view) throws IOException {
    requirePublished(view);
    return StateArchiveCommittedViewV5.openServingPrefix(root, view.getTail(), view.getTarget());
  }

  private CommittedBlock decodeBlock(StateArchiveCommittedViewV5 view,
      ReplayHandles handles, long block, BlockSnapshotMeta meta) throws IOException {
    int[] laneIds = StateArchiveGethFormatV5.laneIds();
    List<DbGroup> groups = new ArrayList<>();
    List<byte[]> frameDigests = new ArrayList<>(laneIds.length);
    for (int laneId : laneIds) {
      DecodedFrame frame = decodeFrame(view, handles, laneId, block);
      if (!Arrays.equals(frame.getBlockHash(), meta.getBlockHash())) {
        throw corrupt("lane bundle identity differs from canonical metadata at block " + block
            + " lane " + laneId);
      }
      frameDigests.add(frame.getFrameDigest());
      groups.addAll(frame.getGroups());
    }
    return new CommittedBlock(new BlockReverseDiff(meta, groups),
        ServingIndexIncrementalPlan.stepDigest(meta, frameDigests));
  }

  private DecodedFrame decodeFrame(StateArchiveCommittedViewV5 view, ReplayHandles handles,
      int laneId, long block) throws IOException {
    PointLocation location = view.capture(laneId, block);
    DecodedFrame frame;
    try {
      frame = new StateArchiveBlockFrameCodecV5().decode(handles.read(location));
    } catch (IllegalArgumentException failure) {
      throw new ServingFaultException(Reason.SOURCE_CORRUPT,
          "State Archive V5 serving source frame decode failed at block " + block + " lane "
              + laneId, failure);
    }
    if (frame.getLaneId() != laneId || frame.getBlockNumber() != block) {
      throw corrupt("lane frame identity differs at block " + block + " lane " + laneId);
    }
    return frame;
  }

  private BlockSnapshotMeta requireCanonical(long block) throws IOException {
    BlockSnapshotMeta meta;
    try {
      meta = canonical.load(block);
    } catch (IOException | RuntimeException failure) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "canonical metadata is unavailable at block " + block, failure);
    }
    if (meta == null || meta.getBlockNumber() != block) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "State Archive V5 serving source canonical metadata is missing at block " + block);
    }
    return meta;
  }

  private void requireRange(Committed view, long fromExclusive, long through) {
    long minimum = firstBlockNumber - 1;
    if (fromExclusive < minimum || through < fromExclusive
        || through > view.getCommittedThrough()) {
      throw new IllegalArgumentException("State Archive V5 serving range is outside [B0,W]");
    }
  }

  private static ServingFaultException corrupt(String message) {
    return new ServingFaultException(Reason.SOURCE_CORRUPT,
        "State Archive V5 serving source " + message);
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

  private static void requirePublished(Committed view) throws IOException {
    if (view.getTail() == null || view.getTarget() == null) {
      throw new ServingFaultException(Reason.SOURCE_UNAVAILABLE,
          "State Archive V5 serving source has no published tail");
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

  /** The requested range exceeds the encoded-byte budget. */
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
