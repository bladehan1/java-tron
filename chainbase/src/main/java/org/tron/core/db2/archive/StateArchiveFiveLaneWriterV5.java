package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveBlockFrameCodecV5.EncodedFrame;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.ReadObserver;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Fresh-root five-lane V5 append writer before tail/Common publication is wired. */
final class StateArchiveFiveLaneWriterV5 implements Closeable {

  private static final ReadObserver NO_OP = (path, position, length) -> { };

  private final Path root;
  private final long firstBlockNumber;
  private final long rotationDecisionBytes;
  private final StateArchiveBlockFrameCodecV5 codec = new StateArchiveBlockFrameCodecV5();
  private final Map<Integer, LaneState> lanes = new LinkedHashMap<>();
  private byte[] resultHistoryDigest;
  private BlockSnapshotMeta appendHead;
  private boolean failed;
  private boolean closed;

  StateArchiveFiveLaneWriterV5(Path root, long firstBlockNumber,
      byte[] baselineHistoryDigest) throws IOException {
    this(root, firstBlockNumber, baselineHistoryDigest,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
  }

  StateArchiveFiveLaneWriterV5(Path root, long firstBlockNumber,
      byte[] baselineHistoryDigest, long rotationDecisionBytes) throws IOException {
    this(root, firstBlockNumber, baselineHistoryDigest, rotationDecisionBytes, null);
    initializeFresh();
  }

  private StateArchiveFiveLaneWriterV5(Path root, long firstBlockNumber,
      byte[] resultHistoryDigest, long rotationDecisionBytes, BlockSnapshotMeta appendHead) {
    this.root = Objects.requireNonNull(root, "root");
    if (firstBlockNumber < 0) {
      throw new IllegalArgumentException("State Archive V5 first block is negative");
    }
    if (rotationDecisionBytes <= StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
        || rotationDecisionBytes > StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES) {
      throw new IllegalArgumentException("State Archive V5 rotation decision is invalid");
    }
    this.firstBlockNumber = firstBlockNumber;
    this.rotationDecisionBytes = rotationDecisionBytes;
    this.resultHistoryDigest = requireHash(resultHistoryDigest, "result history digest");
    this.appendHead = appendHead;
  }

  static StateArchiveFiveLaneWriterV5 reopen(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, byte[] baselineHistoryDigest,
      long rotationDecisionBytes) throws IOException {
    return reopen(root, tail, target, baselineHistoryDigest, rotationDecisionBytes, NO_OP);
  }

  static StateArchiveFiveLaneWriterV5 reopen(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, byte[] baselineHistoryDigest,
      long rotationDecisionBytes, ReadObserver observer) throws IOException {
    Path admittedRoot = Objects.requireNonNull(root, "root");
    StateArchiveTailV5 admittedTail = Objects.requireNonNull(tail, "tail");
    CommonCheckpointTarget admittedTarget = Objects.requireNonNull(target, "target");
    byte[] admittedBaseline = requireHash(baselineHistoryDigest, "baseline history digest");
    ReadObserver admittedObserver = Objects.requireNonNull(observer, "observer");
    admittedTail.requireTarget(admittedTarget);
    try (StateArchiveCommittedViewV5 ignored = StateArchiveCommittedViewV5.open(
        admittedRoot, admittedTail, admittedTarget, admittedObserver)) {
      // Validate the exact committed physical prefix before opening writable handles.
    }
    StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(admittedRoot,
        admittedTail.getFirstBlockNumber(), admittedTail.getResultHistoryDigest(),
        rotationDecisionBytes, admittedTarget.getLastBlock());
    try {
      writer.initializeRecovered(admittedTail, admittedBaseline, admittedObserver);
      return writer;
    } catch (IOException | RuntimeException failure) {
      closeAll(new ArrayList<>(writer.lanes.values()), failure);
      throw failure;
    }
  }

  private void initializeFresh() throws IOException {
    List<LaneState> opened = new ArrayList<>();
    try {
      for (int laneId : StateArchiveGethFormatV5.laneIds()) {
        Path laneRoot = laneRoot(root, laneId);
        Files.createDirectories(laneRoot);
        StateArchiveLaneIndexV5 index = StateArchiveLaneIndexV5.create(
            laneRoot.resolve("blocks.idx"), laneId, firstBlockNumber);
        LaneState state;
        try {
          state = openFirst(laneId, index);
        } catch (IOException | RuntimeException failure) {
          index.close();
          throw failure;
        }
        lanes.put(laneId, state);
        opened.add(state);
      }
    } catch (IOException | RuntimeException failure) {
      closeAll(opened, failure);
      throw failure;
    }
  }

  private void initializeRecovered(StateArchiveTailV5 tail, byte[] baselineHistoryDigest,
      ReadObserver observer) throws IOException {
    long frameCount = StrictMathWrapper.addExact(
        StrictMathWrapper.subtractExact(tail.getCommonBlockNumber(), firstBlockNumber), 1);
    List<LaneState> opened = new ArrayList<>();
    try {
      for (LaneTerminal terminal : tail.getLanes()) {
        int laneId = terminal.getLaneId();
        Path laneRoot = laneRoot(root, laneId);
        StateArchiveLaneIndexV5 index = StateArchiveLaneIndexV5.openWritableCommitted(
            laneRoot.resolve("blocks.idx"), frameCount);
        FileChannel data = null;
        try {
          StateArchiveSegmentHeaderV5 firstHeader = readHeader(
              dataPath(root, laneId, 0), observer);
          if (!Arrays.equals(firstHeader.getStartHistoryDigest(), baselineHistoryDigest)) {
            throw new IOException("State Archive V5 baseline history digest differs");
          }
          Path terminalPath = dataPath(root, laneId, terminal.getTerminalFileId());
          data = FileChannel.open(terminalPath, StandardOpenOption.READ,
              StandardOpenOption.WRITE);
          StateArchiveSegmentHeaderV5 header = readHeader(data, terminalPath, observer);
          if (header.getLaneId() != laneId
              || header.getFileId() != terminal.getTerminalFileId()
              || !Arrays.equals(header.getIndexHeaderDigest(), index.getHeaderDigest())
              || !Arrays.equals(header.digest(), terminal.getTerminalSegmentHeaderDigest())
              || data.size() != terminal.getTerminalDataEndOffset()) {
            throw new IOException("State Archive V5 recovered lane terminal differs");
          }
          LaneState state = new LaneState(laneId, index, header, data);
          state.dataEndOffset = terminal.getTerminalDataEndOffset();
          state.recordCount = 1;
          state.lastBlockNumber = tail.getCommonBlockNumber();
          state.lastFrameDigest = terminal.getTerminalFrameDigest();
          lanes.put(laneId, state);
          opened.add(state);
          data = null;
        } catch (IOException | RuntimeException failure) {
          if (data != null) {
            data.close();
          }
          index.close();
          throw failure;
        }
      }
    } catch (IOException | RuntimeException failure) {
      closeAll(opened, failure);
      lanes.clear();
      throw failure;
    }
  }

  synchronized void append(BlockReverseDiff diff) throws IOException {
    requireUsable();
    BlockReverseDiff admitted = Objects.requireNonNull(diff, "diff");
    BlockSnapshotMeta meta = admitted.getMeta();
    validateNext(meta);
    List<EncodedFrame> frames = new ArrayList<>(5);
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      frames.add(codec.encode(admitted, laneId));
    }
    byte[] blockHistoryDigest = blockHistoryDigest(meta, frames);
    byte[] nextHistoryDigest = StateArchiveGethFormatV5.nextHistoryDigest(
        resultHistoryDigest, blockHistoryDigest);
    try {
      for (EncodedFrame frame : frames) {
        LaneState state = lanes.get(frame.getLaneId());
        if (state.recordCount > 0 && state.dataEndOffset >= rotationDecisionBytes) {
          state = rotate(state, meta.getBlockNumber(), resultHistoryDigest);
        }
        appendFrame(state, frame);
      }
      appendHead = meta;
      resultHistoryDigest = nextHistoryDigest;
    } catch (IOException | RuntimeException failure) {
      failed = true;
      throw failure;
    }
  }

  synchronized StateArchiveLaneIndexV5.FrameRange locate(int laneId, long blockNumber)
      throws IOException {
    requireUsable();
    StateArchiveGethFormatV5.requireLane(laneId);
    return lanes.get(laneId).index.locate(blockNumber);
  }

  synchronized byte[] getResultHistoryDigest() {
    return Arrays.copyOf(resultHistoryDigest, resultHistoryDigest.length);
  }

  synchronized BlockSnapshotMeta getAppendHead() {
    return appendHead;
  }

  synchronized StateArchiveTailV5 forceTailReady(CommonCheckpointTarget target)
      throws IOException {
    return forceTailReady(target, (stage, laneId) -> { });
  }

  synchronized StateArchiveTailV5 forceTailReady(CommonCheckpointTarget target,
      ForceObserver observer) throws IOException {
    requireUsable();
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    ForceObserver admittedObserver = Objects.requireNonNull(observer, "observer");
    if (appendHead == null
        || appendHead.getBlockNumber() != admitted.getLastBlock().getBlockNumber()
        || !Arrays.equals(appendHead.getBlockHash(), admitted.getLastBlock().getBlockHash())) {
      throw new IllegalArgumentException("State Archive V5 append head differs from target");
    }
    long expectedFrameCount = StrictMathWrapper.addExact(
        StrictMathWrapper.subtractExact(appendHead.getBlockNumber(), firstBlockNumber), 1);
    try {
      for (LaneState state : lanes.values()) {
        requireTerminal(state, expectedFrameCount);
        state.data.force(false);
        admittedObserver.forced(ForceStage.DATA, state.laneId);
      }
      for (LaneState state : lanes.values()) {
        state.index.force();
        admittedObserver.forced(ForceStage.LANE_INDEX, state.laneId);
      }
      List<LaneTerminal> terminals = new ArrayList<>(lanes.size());
      for (LaneState state : lanes.values()) {
        terminals.add(new LaneTerminal(state.laneId, StateArchiveTailV5.LANE_ACTIVE,
            state.header.getFileId(), StateArchiveLaneIndexV5.expectedLength(expectedFrameCount),
            state.dataEndOffset, state.lastFrameDigest, state.header.digest()));
      }
      StateArchiveTailV5 tail = StateArchiveTailV5.forTarget(firstBlockNumber, admitted,
          resultHistoryDigest, terminals);
      admittedObserver.forced(ForceStage.TAIL_READY, -1);
      return tail;
    } catch (IOException | RuntimeException failure) {
      failed = true;
      throw failure;
    }
  }

  static Path laneRoot(Path root, int laneId) {
    return root.resolve(laneName(laneId));
  }

  static Path dataPath(Path root, int laneId, long fileId) {
    if (fileId < 0 || fileId > 0xffffffffL) {
      throw new IllegalArgumentException("State Archive V5 file ID exceeds u32");
    }
    long shard = fileId / 1024;
    return laneRoot(root, laneId).resolve(String.format(Locale.ROOT, "shard-%07d", shard))
        .resolve(String.format(Locale.ROOT, "data-%010d.dat", fileId));
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    for (LaneState state : lanes.values()) {
      try {
        state.close();
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

  private LaneState openFirst(int laneId, StateArchiveLaneIndexV5 index)
      throws IOException {
    StateArchiveSegmentHeaderV5 header = StateArchiveSegmentHeaderV5.first(laneId,
        firstBlockNumber, index.getHeaderDigest(), resultHistoryDigest);
    return openSegment(index, header);
  }

  private LaneState rotate(LaneState previous, long firstBlock,
      byte[] startHistoryDigest) throws IOException {
    previous.data.force(false);
    previous.data.close();
    StateArchiveSegmentHeaderV5 header = StateArchiveSegmentHeaderV5.next(
        previous.header, previous.lastFrameDigest, firstBlock, startHistoryDigest);
    LaneState next = openSegment(previous.index, header);
    lanes.put(previous.laneId, next);
    return next;
  }

  private LaneState openSegment(StateArchiveLaneIndexV5 index,
      StateArchiveSegmentHeaderV5 supplied) throws IOException {
    byte[] encoded = supplied.encode();
    StateArchiveSegmentHeaderV5 header = StateArchiveSegmentHeaderV5.decode(encoded);
    Path dataPath = dataPath(root, header.getLaneId(), header.getFileId());
    Files.createDirectories(dataPath.getParent());
    FileChannel data = FileChannel.open(dataPath, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.READ, StandardOpenOption.WRITE);
    try {
      writeFully(data, ByteBuffer.wrap(encoded), 0);
      data.force(false);
      HistorySegmentStore.syncDirectory(dataPath.getParent());
      return new LaneState(header.getLaneId(), index, header, data);
    } catch (IOException | RuntimeException failure) {
      data.close();
      throw failure;
    }
  }

  private void appendFrame(LaneState state, EncodedFrame frame) throws IOException {
    byte[] bytes = frame.getBytes();
    long endOffset = StrictMathWrapper.addExact(state.dataEndOffset, bytes.length);
    if (endOffset > 0xffffffffL) {
      throw new IOException("State Archive V5 data offset exceeds u32");
    }
    writeFully(state.data, ByteBuffer.wrap(bytes), state.dataEndOffset);
    state.index.append(frame.getBlockNumber(), state.header.getFileId(), endOffset);
    state.dataEndOffset = endOffset;
    state.recordCount++;
    state.lastFrameDigest = frame.getFrameDigest();
    state.lastBlockNumber = frame.getBlockNumber();
  }

  private void requireTerminal(LaneState state, long expectedFrameCount) {
    if (state.index.getFrameCount() != expectedFrameCount
        || state.lastBlockNumber != appendHead.getBlockNumber()
        || state.lastFrameDigest == null
        || state.dataEndOffset <= StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH) {
      throw new IllegalStateException("State Archive V5 lane terminal differs from append head");
    }
  }

  private byte[] blockHistoryDigest(BlockSnapshotMeta meta, List<EncodedFrame> frames) {
    List<byte[]> laneItemDigests = new ArrayList<>(frames.size());
    int[] laneIds = StateArchiveGethFormatV5.laneIds();
    for (int index = 0; index < laneIds.length; index++) {
      EncodedFrame frame = frames.get(index);
      if (frame.getLaneId() != laneIds[index]
          || frame.getBlockNumber() != meta.getBlockNumber()
          || !Arrays.equals(frame.getBlockHash(), meta.getBlockHash())) {
        throw new IllegalArgumentException("State Archive V5 lane bundle identity mismatch");
      }
      laneItemDigests.add(frame.getLaneItemDigest());
    }
    return StateArchiveGethFormatV5.blockHistoryDigest(meta, laneItemDigests);
  }

  private void validateNext(BlockSnapshotMeta meta) {
    if (appendHead == null) {
      if (meta.getBlockNumber() != firstBlockNumber) {
        throw new IllegalArgumentException("State Archive V5 first block differs");
      }
      return;
    }
    if (meta.getBlockNumber() != appendHead.getBlockNumber() + 1
        || !Arrays.equals(meta.getParentHash(), appendHead.getBlockHash())) {
      throw new IllegalArgumentException("State Archive V5 block is not expected-next");
    }
  }

  private void requireUsable() {
    if (closed || failed) {
      throw new IllegalStateException("State Archive V5 writer is not usable");
    }
  }

  private static String laneName(int laneId) {
    switch (laneId) {
      case 0:
        return "shared";
      case 4:
        return "account";
      case 5:
        return "account-asset";
      case 13:
        return "delegation";
      case 22:
        return "storage-row";
      default:
        throw new IllegalArgumentException("Unknown State Archive V5 lane ID: " + laneId);
    }
  }

  private static byte[] requireHash(byte[] value, String name) {
    if (value == null || value.length != 32) {
      throw new IllegalArgumentException("State Archive V5 " + name + " length mismatch");
    }
    return Arrays.copyOf(value, value.length);
  }

  private static StateArchiveSegmentHeaderV5 readHeader(Path path, ReadObserver observer)
      throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      return readHeader(channel, path, observer);
    }
  }

  private static StateArchiveSegmentHeaderV5 readHeader(FileChannel channel, Path path,
      ReadObserver observer) throws IOException {
    observer.read(path, 0, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
    try {
      return StateArchiveSegmentHeaderV5.decode(readFully(channel, 0,
          StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
    } catch (RuntimeException failure) {
      throw new IOException("State Archive V5 recovered segment header is invalid", failure);
    }
  }

  private static byte[] readFully(FileChannel channel, long position, int length)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int read = channel.read(bytes, position + bytes.position());
      if (read < 0) {
        throw new EOFException("State Archive V5 data is truncated");
      }
      if (read == 0) {
        throw new IOException("State Archive V5 data read made no progress");
      }
    }
    return bytes.array();
  }

  private static void writeFully(FileChannel channel, ByteBuffer bytes, long position)
      throws IOException {
    while (bytes.hasRemaining()) {
      int written = channel.write(bytes, position + bytes.position());
      if (written == 0) {
        throw new IOException("State Archive V5 data write made no progress");
      }
    }
  }

  private static void closeAll(List<LaneState> states, Throwable failure) {
    for (LaneState state : states) {
      try {
        state.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  enum ForceStage {
    DATA,
    LANE_INDEX,
    TAIL_READY
  }

  interface ForceObserver {
    void forced(ForceStage stage, int laneId) throws IOException;
  }

  private static final class LaneState implements Closeable {
    private final int laneId;
    private final StateArchiveLaneIndexV5 index;
    private final StateArchiveSegmentHeaderV5 header;
    private final FileChannel data;
    private long dataEndOffset = StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH;
    private long recordCount;
    private long lastBlockNumber = -1;
    private byte[] lastFrameDigest;

    private LaneState(int laneId, StateArchiveLaneIndexV5 index,
        StateArchiveSegmentHeaderV5 header, FileChannel data) {
      this.laneId = laneId;
      this.index = index;
      this.header = header;
      this.data = data;
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      try {
        data.close();
      } catch (IOException closeFailure) {
        failure = closeFailure;
      }
      try {
        index.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }
}
