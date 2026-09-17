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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ProcessFdAdmission;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.Boundary;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.FrameRange;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Strict read-only view of exactly the V5 prefix bound by one committed tail and Common. */
final class StateArchiveCommittedViewV5 implements Closeable {

  private static final ReadObserver NO_OP = (path, position, length) -> { };

  private final Path root;
  private final long firstBlockNumber;
  private final long committedBlockNumber;
  private final Map<Integer, StateArchiveLaneIndexV5> indexes;
  private final List<DataFile> dataFiles;
  private final boolean preopenAdmitted;
  private boolean closed;

  private StateArchiveCommittedViewV5(Path root, long firstBlockNumber,
      long committedBlockNumber, Map<Integer, StateArchiveLaneIndexV5> indexes,
      List<DataFile> dataFiles, boolean preopenAdmitted) {
    this.root = root;
    this.firstBlockNumber = firstBlockNumber;
    this.committedBlockNumber = committedBlockNumber;
    this.indexes = indexes;
    this.dataFiles = Collections.unmodifiableList(new ArrayList<>(dataFiles));
    this.preopenAdmitted = preopenAdmitted;
  }

  static StateArchiveCommittedViewV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target) throws IOException {
    return open(root, tail, target, null, NO_OP);
  }

  static StateArchiveCommittedViewV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ReadObserver observer) throws IOException {
    return open(root, tail, target, null, Objects.requireNonNull(observer, "observer"));
  }

  static StateArchiveCommittedViewV5 openForPreopen(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission) throws IOException {
    return open(root, tail, target, Objects.requireNonNull(admission, "admission"), NO_OP);
  }

  static StateArchiveCommittedViewV5 openForPreopen(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission, ReadObserver observer)
      throws IOException {
    return open(root, tail, target, Objects.requireNonNull(admission, "admission"),
        Objects.requireNonNull(observer, "observer"));
  }

  private static StateArchiveCommittedViewV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission, ReadObserver observer)
      throws IOException {
    Path admittedRoot = Objects.requireNonNull(root, "root");
    StateArchiveTailV5 admittedTail = Objects.requireNonNull(tail, "tail");
    admittedTail.requireTarget(Objects.requireNonNull(target, "target"));
    long first = admittedTail.getFirstBlockNumber();
    long last = admittedTail.getCommonBlockNumber();
    long frameCount;
    try {
      frameCount = Math.addExact(Math.subtractExact(last, first), 1);
    } catch (ArithmeticException failure) {
      throw new IOException("State Archive V5 committed range overflow", failure);
    }
    Map<Integer, StateArchiveLaneIndexV5> opened = new LinkedHashMap<>();
    try {
      List<LaneTerminal> terminals = admittedTail.getLanes();
      long requiredDataHandles = requiredDataHandles(terminals);
      StateArchiveDataHandlePoolV5.requireApplicationCap(requiredDataHandles);
      if (admission != null) {
        admission.require(requiredDataHandles,
            StateArchiveDataHandlePoolV5.PROCESS_FD_RESERVE);
      }
      int[] laneIds = StateArchiveGethFormatV5.laneIds();
      List<DataFile> dataFiles = committedDataFiles(admittedRoot, terminals);
      for (int index = 0; index < laneIds.length; index++) {
        int laneId = laneIds[index];
        LaneTerminal terminal = terminals.get(index);
        Path indexPath = StateArchiveFiveLaneWriterV5.laneRoot(admittedRoot, laneId)
            .resolve("blocks.idx");
        StateArchiveLaneIndexV5 laneIndex = StateArchiveLaneIndexV5.openCommitted(
            indexPath, frameCount);
        opened.put(laneId, laneIndex);
        validateLane(admittedRoot, laneIndex, terminal, first, last, observer);
      }
      return new StateArchiveCommittedViewV5(admittedRoot, first, last, opened, dataFiles,
          admission != null);
    } catch (IOException | RuntimeException failure) {
      closeAll(opened.values(), failure);
      if (failure instanceof IOException) {
        throw (IOException) failure;
      }
      throw new IOException("State Archive V5 committed reopen failed", failure);
    }
  }

  synchronized FrameRange locate(int laneId, long blockNumber) throws IOException {
    requireOpen();
    StateArchiveGethFormatV5.requireLane(laneId);
    if (blockNumber < firstBlockNumber || blockNumber > committedBlockNumber) {
      throw new IllegalArgumentException("State Archive V5 block is outside committed view");
    }
    return indexes.get(laneId).locate(blockNumber);
  }

  synchronized PointLocation capture(int laneId, long blockNumber) throws IOException {
    FrameRange range = locate(laneId, blockNumber);
    return new PointLocation(laneId,
        StateArchiveFiveLaneWriterV5.dataPath(root, laneId, range.getFileId()),
        range.getFileId(), range.getStartOffset(), range.getEndOffset());
  }

  synchronized List<DataFile> committedDataFiles() {
    requireOpen();
    if (!preopenAdmitted) {
      throw new IllegalStateException(
          "State Archive V5 committed view lacks process FD admission");
    }
    return dataFiles;
  }

  long getFirstBlockNumber() {
    return firstBlockNumber;
  }

  long getCommittedBlockNumber() {
    return committedBlockNumber;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    for (StateArchiveLaneIndexV5 index : indexes.values()) {
      try {
        index.close();
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

  private static void validateLane(Path root, StateArchiveLaneIndexV5 laneIndex,
      LaneTerminal terminal, long firstBlock, long committedBlock, ReadObserver observer)
      throws IOException {
    int laneId = laneIndex.getLaneId();
    if (terminal.getLaneId() != laneId || terminal.getFlags() != StateArchiveTailV5.LANE_ACTIVE
        || laneIndex.getFirstBlockNumber() != firstBlock) {
      throw invalid("lane identity mismatch");
    }
    Boundary indexTerminal = laneIndex.getLastBoundary();
    if (indexTerminal.getFileId() != terminal.getTerminalFileId()
        || indexTerminal.getEndOffset() != terminal.getTerminalDataEndOffset()) {
      throw invalid("terminal boundary mismatch");
    }

    StateArchiveSegmentHeaderV5 previousHeader = null;
    Path previousPath = null;
    Set<Path> expectedPaths = new HashSet<>();
    for (long fileId = 0; fileId <= terminal.getTerminalFileId(); fileId++) {
      Path dataPath = StateArchiveFiveLaneWriterV5.dataPath(root, laneId, fileId);
      expectedPaths.add(normalize(dataPath));
      StateArchiveSegmentHeaderV5 header = readHeader(dataPath, observer);
      if (header.getLaneId() != laneId || header.getFileId() != fileId) {
        throw invalid("segment header path identity mismatch");
      }
      if (fileId == 0) {
        if (header.getFirstBlockNumber() != firstBlock
            || !Arrays.equals(header.getIndexHeaderDigest(), laneIndex.getHeaderDigest())) {
          throw invalid("first segment binding mismatch");
        }
      } else {
        long segmentFirst = header.getFirstBlockNumber();
        if (segmentFirst <= firstBlock || segmentFirst > committedBlock) {
          throw invalid("segment first block mismatch");
        }
        FrameRange currentFirst = laneIndex.locate(segmentFirst);
        FrameRange previousLast = laneIndex.locate(segmentFirst - 1);
        if (currentFirst.getFileId() != fileId
            || currentFirst.getStartOffset() != StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
            || previousLast.getFileId() != fileId - 1
            || Files.size(previousPath) != previousLast.getEndOffset()) {
          throw invalid("segment index transition mismatch");
        }
        byte[] previousFrameDigest = readStoredFrameDigest(previousPath, previousLast, observer);
        header.requireSuccessorOf(previousHeader, previousFrameDigest, segmentFirst,
            header.getStartHistoryDigest());
      }
      previousHeader = header;
      previousPath = dataPath;
    }

    FrameRange terminalRange = laneIndex.locate(committedBlock);
    if (terminalRange.getFileId() != terminal.getTerminalFileId()
        || terminalRange.getEndOffset() != terminal.getTerminalDataEndOffset()
        || Files.size(previousPath) != terminalRange.getEndOffset()
        || !Arrays.equals(previousHeader.digest(), terminal.getTerminalSegmentHeaderDigest())
        || !Arrays.equals(readStoredFrameDigest(previousPath, terminalRange, observer),
            terminal.getTerminalFrameDigest())) {
      throw invalid("terminal segment mismatch");
    }
    rejectUnexpectedDataFiles(StateArchiveFiveLaneWriterV5.laneRoot(root, laneId),
        expectedPaths);
  }

  private static StateArchiveSegmentHeaderV5 readHeader(Path path, ReadObserver observer)
      throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      observer.read(path, 0, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
      return StateArchiveSegmentHeaderV5.decode(readFully(channel, 0,
          StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
    } catch (RuntimeException failure) {
      throw new IOException("State Archive V5 segment header is invalid: " + path, failure);
    }
  }

  private static byte[] readStoredFrameDigest(Path path, FrameRange range,
      ReadObserver observer) throws IOException {
    long length = range.getEndOffset() - range.getStartOffset();
    if (length < StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
        + StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH
        || length > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
      throw invalid("terminal frame length mismatch");
    }
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      long position = range.getEndOffset() - StateArchiveGethFormatV5.FRAME_TRAILER_LENGTH;
      observer.read(path, position, StateArchiveFileFormatV3.HASH_LENGTH);
      return readFully(channel, position, StateArchiveFileFormatV3.HASH_LENGTH);
    } catch (RuntimeException failure) {
      throw new IOException("State Archive V5 terminal frame metadata is invalid: " + path,
          failure);
    }
  }

  private static void rejectUnexpectedDataFiles(Path laneRoot, Set<Path> expected)
      throws IOException {
    try (Stream<Path> paths = Files.walk(laneRoot)) {
      Path unexpected = paths.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".dat"))
          .map(StateArchiveCommittedViewV5::normalize)
          .filter(path -> !expected.contains(path)).findFirst().orElse(null);
      if (unexpected != null) {
        throw invalid("unexpected data segment: " + unexpected);
      }
    }
  }

  private static byte[] readFully(FileChannel channel, long position, int length)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    while (bytes.hasRemaining()) {
      int read = channel.read(bytes, position + bytes.position());
      if (read < 0) {
        throw new EOFException("State Archive V5 committed file is truncated");
      }
      if (read == 0) {
        throw new IOException("State Archive V5 committed read made no progress");
      }
    }
    return bytes.array();
  }

  private synchronized void requireOpen() {
    if (closed) {
      throw new IllegalStateException("State Archive V5 committed view is closed");
    }
  }

  private static Path normalize(Path path) {
    return path.toAbsolutePath().normalize();
  }

  private static IOException invalid(String message) {
    return new IOException("State Archive V5 " + message);
  }

  interface ReadObserver {
    void read(Path path, long position, int length) throws IOException;
  }

  private static void closeAll(Iterable<StateArchiveLaneIndexV5> indexes,
      Throwable failure) {
    for (StateArchiveLaneIndexV5 index : indexes) {
      try {
        index.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static List<DataFile> committedDataFiles(Path root, List<LaneTerminal> terminals) {
    List<DataFile> files = new ArrayList<>();
    for (LaneTerminal terminal : terminals) {
      for (long fileId = 0; fileId <= terminal.getTerminalFileId(); fileId++) {
        files.add(new DataFile(terminal.getLaneId(), fileId,
            StateArchiveFiveLaneWriterV5.dataPath(root, terminal.getLaneId(), fileId)));
      }
    }
    return files;
  }

  private static long requiredDataHandles(List<LaneTerminal> terminals) throws IOException {
    long result = 0;
    try {
      for (LaneTerminal terminal : terminals) {
        result = Math.addExact(result, Math.addExact(terminal.getTerminalFileId(), 1));
      }
      return result;
    } catch (ArithmeticException failure) {
      throw new IOException("State Archive V5 data handle requirement overflow", failure);
    }
  }

  static final class PointLocation {
    private final int laneId;
    private final Path dataPath;
    private final long fileId;
    private final long startOffset;
    private final long endOffset;

    private PointLocation(int laneId, Path dataPath, long fileId, long startOffset,
        long endOffset) {
      this.laneId = laneId;
      this.dataPath = dataPath;
      this.fileId = fileId;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    int getLaneId() {
      return laneId;
    }

    Path getDataPath() {
      return dataPath;
    }

    long getFileId() {
      return fileId;
    }

    long getStartOffset() {
      return startOffset;
    }

    long getEndOffset() {
      return endOffset;
    }
  }

  static final class DataFile {
    private final int laneId;
    private final long fileId;
    private final Path path;

    private DataFile(int laneId, long fileId, Path path) {
      this.laneId = laneId;
      this.fileId = fileId;
      this.path = path;
    }

    int getLaneId() {
      return laneId;
    }

    long getFileId() {
      return fileId;
    }

    Path getPath() {
      return path;
    }
  }
}
