package org.tron.core.db2.archive.history.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Keeps the codec's concrete immutable values private; normal internal values need no copies. */
final class TailViews {
  private TailViews() {}

  @SuppressWarnings("unchecked")
  static List<StateArchiveTailV5.LaneTerminal> lanes(
      List<? extends V5HistoryTail.LaneTerminal> supplied) {
    Objects.requireNonNull(supplied, "lanes");
    boolean internal = true;
    for (V5HistoryTail.LaneTerminal lane : supplied) {
      internal &= lane instanceof StateArchiveTailV5.LaneTerminal;
    }
    if (internal) {
      return (List<StateArchiveTailV5.LaneTerminal>) (List<?>) supplied;
    }
    List<StateArchiveTailV5.LaneTerminal> converted = new ArrayList<>(supplied.size());
    for (V5HistoryTail.LaneTerminal lane : supplied) {
      converted.add(
          lane == null
              ? null
              : new StateArchiveTailV5.LaneTerminal(
                  lane.getLaneId(),
                  lane.getFlags(),
                  lane.getTerminalFileId(),
                  lane.getCommittedIndexBytes(),
                  lane.getTerminalDataEndOffset(),
                  lane.getTerminalFrameDigest(),
                  lane.getTerminalSegmentHeaderDigest()));
    }
    return converted;
  }
}
