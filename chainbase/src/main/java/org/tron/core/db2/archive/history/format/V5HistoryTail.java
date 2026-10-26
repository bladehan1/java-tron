package org.tron.core.db2.archive.history.format;

import java.util.List;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/** Immutable publication identity shared by history and serving; no file handles or writes. */
public interface V5HistoryTail {
  int LANE_ACTIVE = StateArchiveTailV5.LANE_ACTIVE;

  byte[] encode();

  void requireTarget(CommonCheckpointTarget target);

  byte[] getCommonBlockHash();

  byte[] getCommonTargetDigest();

  long getFirstBlockNumber();

  long getCommonBlockNumber();

  byte[] getResultHistoryDigest();

  List<LaneTerminal> getLanes();

  static V5HistoryTail decode(byte[] encoded) {
    return StateArchiveTailV5.decode(encoded);
  }

  static byte[] key() {
    return StateArchiveTailV5.key();
  }

  static V5HistoryTail forTarget(
      long first,
      CommonCheckpointTarget target,
      byte[] digest,
      List<? extends LaneTerminal> lanes) {
    return StateArchiveTailV5.forTarget(first, target, digest, TailViews.lanes(lanes));
  }

  static V5HistoryTail create(
      long first,
      long block,
      byte[] hash,
      byte[] targetDigest,
      byte[] historyDigest,
      List<? extends LaneTerminal> lanes) {
    return new StateArchiveTailV5(
        first, block, hash, targetDigest, historyDigest, TailViews.lanes(lanes));
  }

  static LaneTerminal laneTerminal(
      int lane,
      int flags,
      long file,
      long indexBytes,
      long dataEnd,
      byte[] frameDigest,
      byte[] headerDigest) {
    return new StateArchiveTailV5.LaneTerminal(
        lane, flags, file, indexBytes, dataEnd, frameDigest, headerDigest);
  }

  interface LaneTerminal {
    int getLaneId();

    int getFlags();

    long getTerminalFileId();

    long getCommittedIndexBytes();

    long getTerminalDataEndOffset();

    byte[] getTerminalFrameDigest();

    byte[] getTerminalSegmentHeaderDigest();
  }
}
