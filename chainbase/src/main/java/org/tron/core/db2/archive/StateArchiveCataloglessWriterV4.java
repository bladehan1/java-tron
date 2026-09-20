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
import java.util.Map;
import java.util.Objects;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveCataloglessFormatV4.FileDescriptor;
import org.tron.core.db2.archive.StateArchiveCataloglessMetadataV4.CommittedView;
import org.tron.core.db2.archive.StateArchiveCataloglessMetadataV4.LaneView;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.DecodedBundle;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.DecodedLane;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedLane;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.BlockIndexEntry;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.BlockIndexHeader;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.SegmentHeader;
import org.tron.core.db2.archive.StateArchiveSegmentNamesV4.SegmentPair;
import org.tron.core.db2.archive.StateArchiveTailV4.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Catalogless five-lane physical writer and committed reader for the v4 prototype. */
final class StateArchiveCataloglessWriterV4
    implements StateArchiveServingSource, StateArchivePointSource, Closeable {

  private static final int FRAME_ENTRY_COUNT_OFFSET = 304;
  private final Path segmentRoot;
  private final byte[] baselineHistoryDigest;
  private final short compressionId;
  private final long rotationTargetBytes;
  private final StateArchiveFiveLaneBlockCodecV3 codec =
      new StateArchiveFiveLaneBlockCodecV3();
  private final StateArchiveCataloglessMetadataV4 metadata;
  private final Map<Integer, LaneState> lanes = new LinkedHashMap<>();
  private BlockSnapshotMeta appendHead;
  private byte[] resultHistoryDigest;
  private StateArchiveTailV4 publishedTail;
  private CommittedView publishedView;
  private boolean failed;
  private boolean closed;

  StateArchiveCataloglessWriterV4(Path archiveRoot, byte[] baselineHistoryDigest,
      short compressionId, long rotationTargetBytes, CommonCheckpointTarget publishedTarget,
      StateArchiveTailV4 tail) throws IOException {
    Objects.requireNonNull(archiveRoot, "archiveRoot");
    Path legacyV3 = archiveRoot.resolveSibling("v3");
    if (Files.exists(legacyV3)) {
      throw new IOException("Legacy State Archive v3 root requires explicit rebuild");
    }
    if (Files.exists(archiveRoot.resolve("catalog"))) {
      throw new IOException("State Archive Catalog root cannot be opened as V4");
    }
    this.segmentRoot = archiveRoot.resolve("segments");
    this.baselineHistoryDigest = requireHash(baselineHistoryDigest, "baseline history digest");
    this.resultHistoryDigest = this.baselineHistoryDigest.clone();
    this.compressionId = compressionId;
    if (rotationTargetBytes <= StateArchiveFileFormatV3.PART_HEADER_LENGTH) {
      throw new IllegalArgumentException("Invalid State Archive V4 rotation target");
    }
    this.rotationTargetBytes = rotationTargetBytes;
    this.metadata = StateArchiveCataloglessMetadataV4.openOrCreate(archiveRoot);
    try {
      if ((publishedTarget == null) != (tail == null)) {
        throw new IOException("Archive V4 published target and tail must appear together");
      }
      if (tail != null) {
        tail.requireTarget(publishedTarget);
        recover(publishedTarget, tail);
      }
    } catch (IOException | RuntimeException failure) {
      closeAfterFailure(failure);
      throw failure;
    }
  }

  synchronized BlockSnapshotMeta getAppendHead() {
    return appendHead;
  }

  synchronized byte[] getResultHistoryDigest() {
    return resultHistoryDigest.clone();
  }

  synchronized void append(EncodedBundle bundle, boolean allowRotation) throws IOException {
    requireUsable();
    EncodedBundle admitted = Objects.requireNonNull(bundle, "bundle");
    BlockSnapshotMeta meta = admitted.getDiff().getMeta();
    validateNext(meta, admitted.getPreviousHistoryDigest());
    try {
      for (EncodedLane lane : admitted.getLanes()) {
        if (lane.getCompressionId() != compressionId) {
          throw new IOException("Archive V4 writer compression differs");
        }
        LaneState state = lanes.get(lane.getLaneId());
        if (allowRotation && state != null && StateArchiveSegmentFormatV3.shouldRotate(
            state.recordCount, state.dataEndOffset, rotationTargetBytes)) {
          seal(state);
          state = null;
        }
        if (state == null) {
          state = openCurrent(lane.getLaneId(), meta.getBlockNumber(),
              admitted.getPreviousHistoryDigest());
        }
        appendFrame(state, meta, lane);
      }
      appendHead = meta;
      resultHistoryDigest = admitted.getResultHistoryDigest();
    } catch (IOException | RuntimeException failure) {
      failed = true;
      throw failure;
    }
  }

  synchronized StateArchiveTailV4 force(CommonCheckpointTarget target) throws IOException {
    requireUsable();
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    if (appendHead == null || !appendHead.equals(admitted.getLastBlock())) {
      throw new IOException("Archive V4 append head differs from Common target");
    }
    List<LaneTerminal> terminals = new ArrayList<>();
    for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
      LaneState state = lanes.get(laneId);
      if (state == null || state.lastBlock != appendHead.getBlockNumber()) {
        throw new IOException("Archive V4 lane does not cover Common target");
      }
      state.data.force(false);
      state.index.force(false);
      terminals.add(new LaneTerminal(laneId, StateArchiveTailV4.TERMINAL_OPEN,
          state.fileId, state.firstBlock, state.lastBlock, state.dataEndOffset,
          metadata.committedLength(laneId)));
    }
    metadata.force();
    return StateArchiveTailV4.forTarget(admitted.getLastBlock().getBlockNumber(),
        admitted, terminals);
  }

  synchronized void publish(StateArchiveTailV4 tail) throws IOException {
    requireUsable();
    StateArchiveTailV4 admitted = Objects.requireNonNull(tail, "tail");
    publishedView = metadata.recover(admitted);
    publishedTail = admitted;
  }

  @Override
  public synchronized long getHistoryStartBlock() {
    long first = Long.MAX_VALUE;
    for (LaneState state : lanes.values()) {
      first = StrictMathWrapper.min(first, state.firstBlock);
    }
    if (publishedView != null) {
      for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
        LaneView lane = publishedView.lane(laneId);
        if (!lane.getSealed().isEmpty()) {
          first = StrictMathWrapper.min(first, lane.getSealed().get(0).getFirstRecordBlockNumber());
        }
      }
    }
    return first == Long.MAX_VALUE ? -1 : first;
  }

  @Override
  public List<BlockReverseDiff> readCommittedDiffs(long fromExclusive, long through,
      long maxEncodedBytes) throws IOException {
    ReadView view;
    synchronized (this) {
      requireUsable();
      if (publishedTail == null || fromExclusive < getHistoryStartBlock() - 1
          || through > publishedTail.getCommonBlockNumber() || through < fromExclusive
          || maxEncodedBytes <= 0) {
        throw new IOException("Archive V4 committed read range is invalid");
      }
      view = new ReadView(publishedView);
    }
    List<BlockReverseDiff> result = new ArrayList<>();
    long encodedBytes = 0;
    for (long block = fromExclusive + 1; block <= through; block++) {
      List<byte[]> frames = new ArrayList<>();
      for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
        byte[] frame = readFrame(view, laneId, block);
        encodedBytes = StrictMathWrapper.addExact(encodedBytes, frame.length);
        if (encodedBytes > maxEncodedBytes) {
          throw new StateArchiveServingSource.ReadBudgetException();
        }
        frames.add(frame);
      }
      result.add(codec.decode(frames).getDiff());
    }
    return result;
  }

  @Override
  public OldValue readCommittedOldValue(String dbName, byte[] rawKey, long blockNumber)
      throws IOException {
    String admittedDbName = Objects.requireNonNull(dbName, "dbName");
    byte[] admittedKey = Objects.requireNonNull(rawKey, "rawKey");
    int storeId = StateArchiveFileFormatV3.storeId(admittedDbName);
    int laneId = StateArchiveFileFormatV3.laneId(storeId);
    ReadView view;
    synchronized (this) {
      requireUsable();
      if (publishedTail == null || blockNumber < getHistoryStartBlock()
          || blockNumber > publishedTail.getCommonBlockNumber()) {
        throw new IOException("Archive V4 committed point block is invalid");
      }
      view = new ReadView(publishedView);
    }
    byte[] frame = readFrame(view, laneId, blockNumber);
    DecodedLane lane = codec.decodeLaneFrame(frame);
    if (lane.getLaneId() != laneId || lane.getMeta().getBlockNumber() != blockNumber) {
      throw new IOException("Archive V4 point frame identity differs");
    }
    for (BlockReverseDiff.DbGroup group : lane.getGroups()) {
      if (group.getDbName().equals(admittedDbName)) {
        for (BlockReverseDiff.Entry entry : group.getEntries()) {
          if (Arrays.equals(entry.getKey(), admittedKey)) {
            return entry.getOldValue();
          }
        }
        break;
      }
    }
    throw new IOException("Archive V4 point index references a missing old value");
  }

  private void recover(CommonCheckpointTarget target, StateArchiveTailV4 tail)
      throws IOException {
    CommittedView view = metadata.recover(tail);
    List<byte[]> finalFrames = new ArrayList<>();
    for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
      LaneView lane = view.lane(laneId);
      LaneTerminal terminal = lane.getTerminal();
      if (terminal.getFlags() != StateArchiveTailV4.TERMINAL_OPEN) {
        throw new IOException("Archive V4 prototype requires an open terminal per lane");
      }
      SegmentPair pair = StateArchiveSegmentNamesV4.current(segmentRoot,
          terminal.getFileId(), laneId, terminal.getFirstRecordBlockNumber());
      FileChannel data = FileChannel.open(pair.getData(), StandardOpenOption.READ,
          StandardOpenOption.WRITE);
      FileChannel index = null;
      try {
        index = FileChannel.open(pair.getIndex(), StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        long records = terminal.getEndBlockNumber()
            - terminal.getFirstRecordBlockNumber() + 1;
        long indexEnd = StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH
            + records * StateArchiveFileFormatV3.BLOCK_INDEX_ENTRY_LENGTH;
        if (data.size() < terminal.getDataEndOffset() || index.size() < indexEnd) {
          throw new IOException("Archive V4 committed current prefix is truncated");
        }
        boolean truncated = data.size() != terminal.getDataEndOffset()
            || index.size() != indexEnd;
        data.truncate(terminal.getDataEndOffset());
        index.truncate(indexEnd);
        if (truncated) {
          data.force(false);
          index.force(false);
          HistorySegmentStore.syncDirectory(pair.getData().getParent());
        }
        SegmentHeader header = readAndValidateHeaders(data, index, laneId,
            terminal.getFileId(), terminal.getFirstRecordBlockNumber());
        BlockIndexEntry last = readIndexEntry(index, records - 1);
        byte[] lastFrame = readFully(data, last.getFrameOffset(), last.getFrameLength());
        if (last.getBlockNumber() != terminal.getEndBlockNumber()
            || last.getEncodedFrameDigestPrefix() != encodedFrameDigestPrefix(lastFrame)) {
          throw new IOException("Archive V4 terminal index differs from data frame");
        }
        LaneState state = new LaneState(laneId, terminal.getFileId(),
            terminal.getFirstRecordBlockNumber(), terminal.getEndBlockNumber(), records,
            terminal.getDataEndOffset(), data, index, false);
        lanes.put(laneId, state);
        data = null;
        index = null;
        finalFrames.add(lastFrame);
      } finally {
        if (index != null) {
          index.close();
        }
        if (data != null) {
          data.close();
        }
      }
    }
    DecodedBundle decoded = codec.decode(finalFrames);
    if (!decoded.getDiff().getMeta().equals(target.getLastBlock())) {
      throw new IOException("Archive V4 terminal frames differ from Common target");
    }
    appendHead = decoded.getDiff().getMeta();
    resultHistoryDigest = decoded.getResultHistoryDigest();
    publishedTail = tail;
    publishedView = view;
  }

  private LaneState openCurrent(int laneId, long firstBlock, byte[] previousHistory)
      throws IOException {
    long fileId = metadata.committedLength(laneId) == StateArchiveFilesMetaV4.HEADER_LENGTH
        ? 0 : (metadata.committedLength(laneId) - StateArchiveFilesMetaV4.HEADER_LENGTH)
            / StateArchiveCataloglessFormatV4.FILE_DESCRIPTOR_LENGTH;
    byte[] previousSegment = previousSegmentDigest(laneId, fileId);
    SegmentHeader supplied = new SegmentHeader(laneId, fileId, firstBlock,
        previousSegment, previousHistory, compressionId);
    byte[] headerBytes = StateArchiveSegmentFormatV3.encodeHeader(supplied);
    SegmentHeader header = StateArchiveSegmentFormatV3.decodeHeader(headerBytes);
    SegmentPair pair = StateArchiveSegmentNamesV4.current(segmentRoot, fileId, laneId,
        firstBlock);
    Files.createDirectories(pair.getData().getParent());
    FileChannel data = FileChannel.open(pair.getData(), StandardOpenOption.CREATE_NEW,
        StandardOpenOption.READ, StandardOpenOption.WRITE);
    FileChannel index = null;
    try {
      writeFully(data, ByteBuffer.wrap(headerBytes));
      byte[] indexHeader = StateArchiveSegmentFormatV3.encodeBlockIndexHeader(
          new BlockIndexHeader(laneId, fileId, header.getHeaderDigest()));
      index = FileChannel.open(pair.getIndex(), StandardOpenOption.CREATE_NEW,
          StandardOpenOption.READ, StandardOpenOption.WRITE);
      writeFully(index, ByteBuffer.wrap(indexHeader));
      data.force(false);
      index.force(false);
      HistorySegmentStore.syncDirectory(pair.getData().getParent());
      LaneState state = new LaneState(laneId, fileId, firstBlock, firstBlock - 1, 0,
          StateArchiveFileFormatV3.PART_HEADER_LENGTH, data, index, true);
      lanes.put(laneId, state);
      return state;
    } catch (IOException | RuntimeException failure) {
      data.close();
      if (index != null) {
        index.close();
      }
      throw failure;
    }
  }

  private void appendFrame(LaneState state, BlockSnapshotMeta meta, EncodedLane lane)
      throws IOException {
    long offset = state.dataEndOffset;
    state.data.position(offset);
    writeFully(state.data, lane.frameView());
    state.index.position(state.index.size());
    writeFully(state.index, ByteBuffer.wrap(StateArchiveSegmentFormatV3.encodeBlockIndexEntry(
        new BlockIndexEntry(meta.getBlockNumber(), offset, lane.getFrameLength(),
            lane.getEncodedFrameDigestPrefix()))));
    state.lastBlock = meta.getBlockNumber();
    state.recordCount++;
    state.dataEndOffset += lane.getFrameLength();
    if (state.statsKnown) {
      if (lane.getEntryCount() > 0) {
        state.changedFrameCount++;
      }
      state.entryCount = StrictMathWrapper.addExact(state.entryCount, lane.getEntryCount());
    }
  }

  private void seal(LaneState state) throws IOException {
    if (!state.statsKnown) {
      recoverStats(state);
    }
    state.data.force(false);
    state.index.force(false);
    long indexBytes = state.index.size();
    state.close();
    StateArchiveSegmentNamesV4.seal(segmentRoot, state.fileId, state.laneId,
        state.firstBlock, state.lastBlock);
    FileDescriptor descriptor = new FileDescriptor(state.fileId, 0, state.firstBlock,
        state.lastBlock, state.dataEndOffset, state.recordCount,
        state.changedFrameCount, state.entryCount);
    metadata.appendSealed(state.laneId, descriptor);
    lanes.remove(state.laneId);
    long expectedIndex = StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH
        + state.recordCount * StateArchiveFileFormatV3.BLOCK_INDEX_ENTRY_LENGTH;
    if (indexBytes != expectedIndex) {
      throw new IOException("Archive V4 sealed index length differs");
    }
  }

  private void recoverStats(LaneState state) throws IOException {
    long changed = 0;
    long entries = 0;
    for (long ordinal = 0; ordinal < state.recordCount; ordinal++) {
      BlockIndexEntry index = readIndexEntry(state.index, ordinal);
      byte[] count = readFully(state.data,
          index.getFrameOffset() + FRAME_ENTRY_COUNT_OFFSET, Long.BYTES);
      long value = ByteBuffer.wrap(count).getLong();
      if (value < 0) {
        throw new IOException("Archive V4 frame entry count is invalid");
      }
      entries = StrictMathWrapper.addExact(entries, value);
      if (value > 0) {
        changed++;
      }
    }
    state.changedFrameCount = changed;
    state.entryCount = entries;
    state.statsKnown = true;
  }

  private byte[] previousSegmentDigest(int laneId, long fileId) throws IOException {
    if (fileId == 0) {
      return StateArchiveSegmentFormatV3.laneBaselineDigest(laneId);
    }
    List<FileDescriptor> sealed = metadataSnapshot(laneId);
    if (sealed.size() != fileId) {
      throw new IOException("Archive V4 sealed sequence differs from files.meta");
    }
    return StateArchiveFileFormatV3.sha256(StateArchiveFileFormatV3.SEGMENT_CHAIN_DOMAIN,
        StateArchiveCataloglessFormatV4.formatDigest(),
        StateArchiveCataloglessFormatV4.encodeFileDescriptor(sealed.get(sealed.size() - 1)));
  }

  private List<FileDescriptor> metadataSnapshot(int laneId) throws IOException {
    long length = metadata.committedLength(laneId);
    return metadata.snapshot(laneId, length);
  }

  private byte[] readFrame(ReadView view, int laneId, long block) throws IOException {
    LaneView lane = view.committed.lane(laneId);
    FileDescriptor sealed = lane.selectSealed(block);
    long fileId;
    long first;
    SegmentPair pair;
    if (sealed != null) {
      fileId = sealed.getFileId();
      first = sealed.getFirstRecordBlockNumber();
      pair = StateArchiveSegmentNamesV4.sealed(segmentRoot, fileId, laneId, first,
          sealed.getEndBlockNumber());
    } else {
      LaneTerminal terminal = lane.getTerminal();
      if (block < terminal.getFirstRecordBlockNumber()
          || block > terminal.getEndBlockNumber()) {
        throw new IOException("Archive V4 block has no segment");
      }
      fileId = terminal.getFileId();
      first = terminal.getFirstRecordBlockNumber();
      pair = StateArchiveSegmentNamesV4.current(segmentRoot, fileId, laneId, first);
    }
    try (FileChannel index = FileChannel.open(pair.getIndex(), StandardOpenOption.READ);
        FileChannel data = FileChannel.open(pair.getData(), StandardOpenOption.READ)) {
      readAndValidateHeaders(data, index, laneId, fileId, first);
      BlockIndexEntry entry = readIndexEntry(index, block - first);
      if (entry.getBlockNumber() != block) {
        throw new IOException("Archive V4 block index identity differs");
      }
      byte[] frame = readFully(data, entry.getFrameOffset(), entry.getFrameLength());
      if (entry.getEncodedFrameDigestPrefix() != encodedFrameDigestPrefix(frame)) {
        throw new IOException("Archive V4 block index digest differs");
      }
      return frame;
    }
  }

  private static SegmentHeader readAndValidateHeaders(FileChannel data, FileChannel index,
      int laneId, long fileId, long firstBlock) throws IOException {
    SegmentHeader header = StateArchiveSegmentFormatV3.decodeHeader(readFully(data, 0,
        StateArchiveFileFormatV3.PART_HEADER_LENGTH));
    BlockIndexHeader indexHeader = StateArchiveSegmentFormatV3.decodeBlockIndexHeader(
        readFully(index, 0, StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH));
    if (header.getLaneId() != laneId || header.getSegmentSeq() != fileId
        || header.getActualFirstBlock() != firstBlock || indexHeader.getLaneId() != laneId
        || indexHeader.getSegmentSeq() != fileId
        || !Arrays.equals(indexHeader.getDataSegmentHeaderDigest(), header.getHeaderDigest())) {
      throw new IOException("Archive V4 segment header identity differs");
    }
    return header;
  }

  private static BlockIndexEntry readIndexEntry(FileChannel index, long ordinal)
      throws IOException {
    long offset = StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH
        + StrictMathWrapper.multiplyExact(ordinal, StateArchiveFileFormatV3.BLOCK_INDEX_ENTRY_LENGTH);
    return StateArchiveSegmentFormatV3.decodeBlockIndexEntry(readFully(index, offset,
        StateArchiveFileFormatV3.BLOCK_INDEX_ENTRY_LENGTH));
  }

  private static long encodedFrameDigestPrefix(byte[] frame) throws IOException {
    if (frame.length < StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH) {
      throw new IOException("Archive V4 frame is too short");
    }
    return ByteBuffer.wrap(frame, frame.length - StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH,
        Long.BYTES).getLong();
  }

  private void validateNext(BlockSnapshotMeta meta, byte[] previousHistory) {
    if (appendHead == null) {
      if (!Arrays.equals(previousHistory, baselineHistoryDigest)) {
        throw new IllegalArgumentException("Archive V4 first history digest differs");
      }
      return;
    }
    if (meta.getBlockNumber() != appendHead.getBlockNumber() + 1
        || !Arrays.equals(meta.getParentHash(), appendHead.getBlockHash())
        || !Arrays.equals(previousHistory, resultHistoryDigest)) {
      throw new IllegalArgumentException("Archive V4 bundle is not expected-next");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      IOException failure = null;
      for (LaneState state : lanes.values()) {
        try {
          state.close();
        } catch (IOException closeFailure) {
          failure = closeFailure;
        }
      }
      try {
        metadata.close();
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

  private void closeAfterFailure(Throwable failure) {
    try {
      close();
    } catch (IOException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private void requireUsable() throws IOException {
    if (closed || failed) {
      throw new IOException("Archive V4 writer is unavailable");
    }
  }

  private static byte[] requireHash(byte[] value, String name) {
    byte[] result = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (result.length != StateArchiveFileFormatV3.HASH_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return result;
  }

  private static byte[] readFully(FileChannel channel, long offset, int length)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int read = channel.read(bytes, offset + bytes.position());
      if (read < 0) {
        throw new EOFException("Archive V4 file is truncated");
      }
    }
    return bytes.array();
  }

  private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining()) {
      channel.write(bytes);
    }
  }

  private static final class ReadView {
    private final CommittedView committed;

    private ReadView(CommittedView committed) {
      this.committed = committed;
    }
  }

  private static final class LaneState implements Closeable {
    private final int laneId;
    private final long fileId;
    private final long firstBlock;
    private final FileChannel data;
    private final FileChannel index;
    private long lastBlock;
    private long recordCount;
    private long dataEndOffset;
    private long changedFrameCount;
    private long entryCount;
    private boolean statsKnown;

    private LaneState(int laneId, long fileId, long firstBlock, long lastBlock,
        long recordCount, long dataEndOffset, FileChannel data, FileChannel index,
        boolean statsKnown) {
      this.laneId = laneId;
      this.fileId = fileId;
      this.firstBlock = firstBlock;
      this.lastBlock = lastBlock;
      this.recordCount = recordCount;
      this.dataEndOffset = dataEndOffset;
      this.data = data;
      this.index = index;
      this.statsKnown = statsKnown;
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      try {
        index.close();
      } catch (IOException closeFailure) {
        failure = closeFailure;
      }
      try {
        data.close();
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
