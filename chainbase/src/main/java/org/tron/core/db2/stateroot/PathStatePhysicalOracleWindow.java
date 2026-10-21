package org.tron.core.db2.stateroot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Validated, read-only input for one fixed physical-store oracle window.
 *
 * <p>The window is ordered from the current child toward its oldest retained parent. It does not
 * expose a historical root lookup API and does not mutate {@code F/N/M}, {@code INTENT},
 * {@code CURRENT}, or reverse journals. A caller must open an offline coherent checkpoint before
 * loading the window; this class only proves that the supplied CURRENT and journals form one exact
 * direct-parent chain.
 */
final class PathStatePhysicalOracleWindow {

  private final byte[] currentTarget;
  private final byte[] oldestTarget;
  private final List<byte[]> encodedJournals;

  PathStatePhysicalOracleWindow(byte[] currentTarget,
      List<PathStatePhysicalReverseJournal> journals) {
    this.currentTarget = owned(currentTarget, "currentTarget");
    List<PathStatePhysicalReverseJournal> supplied = new ArrayList<>(
        Objects.requireNonNull(journals, "journals"));
    if (supplied.isEmpty()) {
      throw new IllegalArgumentException("physical oracle window must contain at least one block");
    }
    PathStatePhysicalGlobalIntent cursor = PathStatePhysicalGlobalIntent.decode(
        this.currentTarget);
    List<byte[]> encoded = new ArrayList<>(supplied.size());
    for (PathStatePhysicalReverseJournal journal : supplied) {
      PathStatePhysicalReverseJournal present = Objects.requireNonNull(journal, "journal");
      if (!Arrays.equals(cursor.encode(), present.getChildTarget())) {
        throw new IllegalArgumentException(
            "physical oracle window journal does not extend its current child");
      }
      byte[] journalBytes = present.encode();
      encoded.add(Arrays.copyOf(journalBytes, journalBytes.length));
      cursor = PathStatePhysicalGlobalIntent.decode(present.getParentTarget());
    }
    this.oldestTarget = cursor.encode();
    this.encodedJournals = Collections.unmodifiableList(encoded);
  }

  int getBlockCount() {
    return encodedJournals.size();
  }

  PathStateRootMetadata getCurrentMetadata() {
    return PathStatePhysicalGlobalIntent.decode(currentTarget).getMetadata();
  }

  PathStateRootMetadata getOldestMetadata() {
    return PathStatePhysicalGlobalIntent.decode(oldestTarget).getMetadata();
  }

  List<PathStatePhysicalReverseJournal> journals() {
    List<PathStatePhysicalReverseJournal> copies = new ArrayList<>(encodedJournals.size());
    for (byte[] encoded : encodedJournals) {
      copies.add(PathStatePhysicalReverseJournal.decode(encoded));
    }
    return Collections.unmodifiableList(copies);
  }

  List<PathStatePhysicalGlobalIntent> targets() {
    List<PathStatePhysicalGlobalIntent> targets = new ArrayList<>(encodedJournals.size() + 1);
    PathStatePhysicalGlobalIntent cursor = PathStatePhysicalGlobalIntent.decode(currentTarget);
    targets.add(cursor);
    for (byte[] encoded : encodedJournals) {
      PathStatePhysicalReverseJournal journal = PathStatePhysicalReverseJournal.decode(encoded);
      cursor = PathStatePhysicalGlobalIntent.decode(journal.getParentTarget());
      targets.add(cursor);
    }
    return Collections.unmodifiableList(targets);
  }

  private static byte[] owned(byte[] value, String name) {
    byte[] supplied = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    PathStatePhysicalGlobalIntent.decode(supplied);
    return supplied;
  }
}
