package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveCataloglessFormatV4.FileDescriptor;
import org.tron.core.db2.archive.StateArchiveTailV4.LaneTerminal;

/** Catalogless structural authority assembled from five files.meta committed prefixes. */
final class StateArchiveCataloglessMetadataV4 implements Closeable {

  private final Map<Integer, StateArchiveFilesMetaV4> filesByLane;

  private StateArchiveCataloglessMetadataV4(
      Map<Integer, StateArchiveFilesMetaV4> filesByLane) {
    this.filesByLane = filesByLane;
  }

  static StateArchiveCataloglessMetadataV4 openOrCreate(Path root) throws IOException {
    Objects.requireNonNull(root, "root");
    Map<Integer, StateArchiveFilesMetaV4> opened = new LinkedHashMap<>();
    try {
      for (int laneId : StateArchiveCataloglessFormatV4.laneIds()) {
        opened.put(laneId, StateArchiveFilesMetaV4.openOrCreate(root, laneId));
      }
      return new StateArchiveCataloglessMetadataV4(opened);
    } catch (IOException | RuntimeException failure) {
      closeAfterFailure(opened.values(), failure);
      throw failure;
    }
  }

  synchronized void appendSealed(int laneId, FileDescriptor descriptor) throws IOException {
    lane(laneId).append(Objects.requireNonNull(descriptor, "descriptor"));
  }

  synchronized void force() throws IOException {
    for (StateArchiveFilesMetaV4 files : filesByLane.values()) {
      files.force();
    }
  }

  synchronized long committedLength(int laneId) {
    return lane(laneId).committedLength();
  }

  synchronized List<FileDescriptor> snapshot(int laneId, long committedBytes)
      throws IOException {
    return lane(laneId).readCommitted(committedBytes);
  }

  synchronized CommittedView recover(StateArchiveTailV4 tail) throws IOException {
    StateArchiveTailV4 admitted = Objects.requireNonNull(tail, "tail");
    Map<Integer, LaneView> lanes = new LinkedHashMap<>();
    for (LaneTerminal terminal : admitted.getLanes()) {
      StateArchiveFilesMetaV4 files = lane(terminal.getLaneId());
      require(files.committedLength() == terminal.getFilesMetaCommittedBytes(),
          "Archive files.meta has an unpublished suffix");
      List<FileDescriptor> sealed = files.readCommitted(
          terminal.getFilesMetaCommittedBytes());
      validateTerminal(terminal, sealed);
      lanes.put(terminal.getLaneId(), new LaneView(terminal, sealed));
    }
    require(lanes.size() == StateArchiveCataloglessFormatV4.laneIds().length,
        "Archive committed view has incomplete lanes");
    return new CommittedView(admitted.getCommonBlockNumber(), lanes);
  }

  private StateArchiveFilesMetaV4 lane(int laneId) {
    StateArchiveFilesMetaV4 files = filesByLane.get(laneId);
    if (files == null) {
      throw new IllegalArgumentException("Unknown catalogless Archive lane: " + laneId);
    }
    return files;
  }

  private static void validateTerminal(LaneTerminal terminal,
      List<FileDescriptor> sealed) {
    int expectedCount = terminal.getFlags() == StateArchiveTailV4.TERMINAL_SEALED
        ? StrictMathWrapper.toIntExact(terminal.getFileId() + 1) : StrictMathWrapper.toIntExact(terminal.getFileId());
    require(sealed.size() == expectedCount,
        "Archive terminal differs from files.meta committed prefix");
    if (terminal.getFlags() == StateArchiveTailV4.TERMINAL_SEALED) {
      FileDescriptor descriptor = sealed.get(sealed.size() - 1);
      require(descriptor.getFileId() == terminal.getFileId()
              && descriptor.getFirstRecordBlockNumber()
                  == terminal.getFirstRecordBlockNumber()
              && descriptor.getEndBlockNumber() == terminal.getEndBlockNumber()
              && descriptor.getDataFileBytes() == terminal.getDataEndOffset(),
          "Archive sealed terminal identity mismatch");
      return;
    }
    if (!sealed.isEmpty()) {
      FileDescriptor previous = sealed.get(sealed.size() - 1);
      require(previous.getFileId() + 1 == terminal.getFileId()
              && previous.getEndBlockNumber() + 1
                  == terminal.getFirstRecordBlockNumber(),
          "Archive open terminal does not extend files.meta");
    } else {
      require(terminal.getFileId() == 0,
          "Archive first open terminal file id mismatch");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    IOException failure = null;
    for (StateArchiveFilesMetaV4 files : filesByLane.values()) {
      try {
        files.close();
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

  private static void closeAfterFailure(Iterable<StateArchiveFilesMetaV4> opened,
      Throwable failure) {
    for (StateArchiveFilesMetaV4 files : opened) {
      try {
        files.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  static final class CommittedView {
    private final long commonBlockNumber;
    private final Map<Integer, LaneView> lanes;

    private CommittedView(long commonBlockNumber, Map<Integer, LaneView> lanes) {
      this.commonBlockNumber = commonBlockNumber;
      this.lanes = Collections.unmodifiableMap(new LinkedHashMap<>(lanes));
    }

    long getCommonBlockNumber() {
      return commonBlockNumber;
    }

    LaneView lane(int laneId) {
      LaneView lane = lanes.get(laneId);
      if (lane == null) {
        throw new IllegalArgumentException("Unknown committed Archive lane: " + laneId);
      }
      return lane;
    }
  }

  static final class LaneView {
    private final LaneTerminal terminal;
    private final List<FileDescriptor> sealed;

    private LaneView(LaneTerminal terminal, List<FileDescriptor> sealed) {
      this.terminal = terminal;
      this.sealed = Collections.unmodifiableList(new ArrayList<>(sealed));
    }

    LaneTerminal getTerminal() {
      return terminal;
    }

    List<FileDescriptor> getSealed() {
      return sealed;
    }

    FileDescriptor selectSealed(long blockNumber) {
      int low = 0;
      int high = sealed.size();
      while (low < high) {
        int middle = (low + high) >>> 1;
        if (sealed.get(middle).getEndBlockNumber() < blockNumber) {
          low = middle + 1;
        } else {
          high = middle;
        }
      }
      if (low >= sealed.size()) {
        return null;
      }
      FileDescriptor selected = sealed.get(low);
      return selected.getFirstRecordBlockNumber() <= blockNumber ? selected : null;
    }
  }
}
