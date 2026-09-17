package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/** Deterministic current and sealed data/index names for catalogless v4 segments. */
final class StateArchiveSegmentNamesV4 {

  private static final String CURRENT = "current";

  private StateArchiveSegmentNamesV4() {
  }

  static SegmentPair current(Path segmentRoot, long fileId, int laneId,
      long firstBlockNumber) {
    requireBlock(firstBlockNumber, "first block");
    return pair(segmentRoot, fileId, laneId,
        String.format("%s_%020d-%s", laneName(laneId), firstBlockNumber, CURRENT));
  }

  static SegmentPair sealed(Path segmentRoot, long fileId, int laneId,
      long firstBlockNumber, long endBlockNumber) {
    requireBlock(firstBlockNumber, "first block");
    requireBlock(endBlockNumber, "end block");
    if (endBlockNumber < firstBlockNumber) {
      throw new IllegalArgumentException("State Archive segment range is invalid");
    }
    return pair(segmentRoot, fileId, laneId,
        String.format("%s_%020d-%020d", laneName(laneId), firstBlockNumber,
            endBlockNumber));
  }

  /** Renames both members of one current pair; any partial result remains fail-closed. */
  static SegmentPair seal(Path segmentRoot, long fileId, int laneId,
      long firstBlockNumber, long endBlockNumber) throws IOException {
    SegmentPair current = current(segmentRoot, fileId, laneId, firstBlockNumber);
    SegmentPair sealed = sealed(segmentRoot, fileId, laneId, firstBlockNumber,
        endBlockNumber);
    requireCurrentPair(current);
    if (Files.exists(sealed.data) || Files.exists(sealed.index)) {
      throw new IOException("State Archive sealed segment pair already exists");
    }
    try {
      Files.move(current.index, sealed.index, StandardCopyOption.ATOMIC_MOVE);
      Files.move(current.data, sealed.data, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new IOException("State Archive segment pair requires atomic member renames",
          unsupported);
    }
    HistorySegmentStore.syncDirectory(sealed.data.getParent());
    return sealed;
  }

  static String laneName(int laneId) {
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
        throw new IllegalArgumentException("Unknown State Archive lane ID: " + laneId);
    }
  }

  private static SegmentPair pair(Path segmentRoot, long fileId, int laneId,
      String baseName) {
    Objects.requireNonNull(segmentRoot, "segmentRoot");
    if (fileId < 0 || fileId > 0xffff_ffffL) {
      throw new IllegalArgumentException("State Archive file ID is invalid");
    }
    laneName(laneId);
    Path shard = segmentRoot.resolve(String.format("shard-%06d",
        fileId / StateArchiveFileFormatV3.SHARD_MAX_SEGMENTS));
    return new SegmentPair(shard.resolve(baseName + ".dat"),
        shard.resolve(baseName + ".bidx"));
  }

  private static void requireBlock(long blockNumber, String field) {
    if (blockNumber < 0) {
      throw new IllegalArgumentException("State Archive " + field + " is invalid");
    }
  }

  private static void requireCurrentPair(SegmentPair current) throws IOException {
    boolean data = Files.isRegularFile(current.data);
    boolean index = Files.isRegularFile(current.index);
    if (!data || !index) {
      throw new IOException("State Archive current segment pair is incomplete");
    }
  }

  static final class SegmentPair {
    private final Path data;
    private final Path index;

    private SegmentPair(Path data, Path index) {
      this.data = data;
      this.index = index;
    }

    Path getData() {
      return data;
    }

    Path getIndex() {
      return index;
    }
  }
}
