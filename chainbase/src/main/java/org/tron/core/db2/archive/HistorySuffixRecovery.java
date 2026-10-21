package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/** Trims only physical bytes/files after the boundary proved by a durable Common commit. */
final class HistorySuffixRecovery {
  static void trim(Path root, StateArchiveTailV5 tail, CommonCheckpointTarget target)
      throws IOException {
    List<Trim> plan = new ArrayList<>();
    // Validate all visited committed headers/boundaries and terminal frames before any mutation.
    try (StateArchiveCommittedViewV5 view =
        StateArchiveCommittedViewV5.openCommittedPrefix(root, tail, target)) {
      for (StateArchiveTailV5.LaneTerminal lane : tail.getLanes()) {
        StateArchiveLaneIndexV5.FrameRange range = view.locate(
            lane.getLaneId(), tail.getCommonBlockNumber());
        Path terminal = StateArchiveFiveLaneWriterV5.dataPath(
            root, lane.getLaneId(), lane.getTerminalFileId());
        try (FileChannel file = FileChannel.open(terminal, StandardOpenOption.READ)) {
          long length = range.getEndOffset() - range.getStartOffset();
          if (length <= 0 || length > StateArchiveGethFormatV5.MAX_FRAME_BYTES) {
            throw new IOException("committed terminal frame length is invalid");
          }
          ByteBuffer bytes = ByteBuffer.allocate((int) length);
          long offset = range.getStartOffset();
          while (bytes.hasRemaining()) {
            int read = file.read(bytes, offset);
            if (read <= 0) {
              throw new IOException("committed terminal frame is truncated");
            }
            offset += read;
          }
          StateArchiveBlockFrameCodecV5.DecodedFrame decoded;
          try {
            decoded = new StateArchiveBlockFrameCodecV5().decode(bytes.array());
          } catch (IllegalArgumentException corrupt) {
            throw new IOException("committed terminal frame is corrupt; no trimming", corrupt);
          }
          if (decoded.getLaneId() != lane.getLaneId()
              || decoded.getBlockNumber() != tail.getCommonBlockNumber()
              || !Arrays.equals(decoded.getBlockHash(), tail.getCommonBlockHash())
              || !Arrays.equals(decoded.getFrameDigest(), lane.getTerminalFrameDigest())) {
            throw new IOException("committed terminal frame identity differs; no trimming");
          }
        }
        plan.add(new Trim(StateArchiveFiveLaneWriterV5.laneRoot(root, lane.getLaneId())
            .resolve("blocks.idx"), lane.getCommittedIndexBytes()));
        plan.add(new Trim(terminal, lane.getTerminalDataEndOffset()));
        futureFiles(root, lane.getLaneId(), lane.getTerminalFileId(), plan);
      }
    }
    apply(plan);
  }

  /** Requires an explicit caller proof that this directory has no committed Common generation. */
  static void resetUncommittedBaseline(Path root, long firstBlock, byte[] baselineDigest)
      throws IOException {
    List<Trim> plan = new ArrayList<>();
    for (int lane : StateArchiveGethFormatV5.laneIds()) {
      Path laneRoot = StateArchiveFiveLaneWriterV5.laneRoot(root, lane);
      Path index = laneRoot.resolve("blocks.idx");
      try (StateArchiveLaneIndexV5 opened = StateArchiveLaneIndexV5.openCommittedPrefix(index, 0)) {
        if (opened.getFirstBlockNumber() != firstBlock || opened.getLaneId() != lane) {
          throw new IOException("uncommitted baseline index identity differs");
        }
      }
      Path initial = StateArchiveFiveLaneWriterV5.dataPath(root, lane, 0);
      try (FileChannel file = FileChannel.open(initial, StandardOpenOption.READ)) {
        ByteBuffer bytes = ByteBuffer.allocate(StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH);
        while (bytes.hasRemaining()) {
          if (file.read(bytes) <= 0) {
            throw new IOException("uncommitted initial header is truncated");
          }
        }
        StateArchiveSegmentHeaderV5 header = StateArchiveSegmentHeaderV5.decode(bytes.array());
        if (header.getLaneId() != lane || header.getFileId() != 0
            || header.getFirstBlockNumber() != firstBlock
            || !Arrays.equals(header.getStartHistoryDigest(), baselineDigest)) {
          throw new IOException("uncommitted initial header identity differs");
        }
      }
      plan.add(new Trim(index, StateArchiveLaneIndexV5.expectedLength(0)));
      plan.add(new Trim(initial, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH));
      futureFiles(root, lane, 0, plan);
    }
    apply(plan);
  }

  private static void futureFiles(Path root, int lane, long terminal, List<Trim> plan)
      throws IOException {
    Path laneRoot = StateArchiveFiveLaneWriterV5.laneRoot(root, lane);
    try (Stream<Path> files = Files.walk(laneRoot)) {
      for (Path path : (Iterable<Path>) files::iterator) {
        if (Files.isSymbolicLink(path)) {
          throw new IOException("History recovery does not follow symbolic links");
        }
        String name = path.getFileName().toString();
        if (!name.endsWith(".dat")) {
          continue;
        }
        if (!name.matches("data-[0-9]{10}\\.dat")) {
          throw new IOException("unknown History data file; refusing recovery: " + name);
        }
        long id = Long.parseLong(name.substring(5, 15));
        if (!path.equals(StateArchiveFiveLaneWriterV5.dataPath(root, lane, id))) {
          throw new IOException("History data path identity differs");
        }
        if (id > terminal) {
          plan.add(new Trim(path, -1));
        }
      }
    }
  }

  private static void apply(List<Trim> plan) throws IOException {
    for (Trim trim : plan) {
      if (trim.length < 0) {
        Files.deleteIfExists(trim.path);
        HistorySegmentStore.syncDirectory(trim.path.getParent());
      } else {
        try (FileChannel file = FileChannel.open(trim.path, StandardOpenOption.WRITE)) {
          if (file.size() < trim.length) {
            throw new IOException("committed History prefix is missing; no repair");
          }
          file.truncate(trim.length);
          file.force(false);
        }
      }
    }
  }

  private static final class Trim {
    private final Path path;
    private final long length;

    private Trim(Path path, long length) {
      this.path = path;
      this.length = length;
    }
  }
}
