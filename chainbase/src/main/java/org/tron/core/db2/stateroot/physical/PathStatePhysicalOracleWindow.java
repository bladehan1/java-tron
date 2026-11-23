package org.tron.core.db2.stateroot.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.stateroot.format.PathStateRootMetadata;
import org.tron.core.db2.stateroot.physical.PathStatePhysicalRecords.GlobalIntent;
import org.tron.core.db2.stateroot.physical.PathStatePhysicalRecords.ReverseJournal;

/**
 * Validated, read-only input for one fixed physical-store oracle window.
 *
 * <p>The window is ordered from the current child toward its oldest retained parent. It does not
 * expose a historical root lookup API and does not mutate {@code F/N/M}, {@code INTENT},
 * {@code CURRENT}, or reverse journals. A caller must open an offline coherent checkpoint before
 * loading the window; this class only proves that the supplied CURRENT and journals form one exact
 * direct-parent chain.
 */
final class PathStatePhysicalOracleWindow implements PathStatePhysicalRecords.OracleWindow {

  private final byte[] currentTarget;
  private final byte[] oldestTarget;
  private final List<byte[]> encodedJournals;

  PathStatePhysicalOracleWindow(byte[] currentTarget,
      List<ReverseJournal> journals) {
    this.currentTarget = owned(currentTarget, "currentTarget");
    List<ReverseJournal> supplied = new ArrayList<>(
        Objects.requireNonNull(journals, "journals"));
    if (supplied.isEmpty()) {
      throw new IllegalArgumentException("physical oracle window must contain at least one block");
    }
    GlobalIntent cursor = GlobalIntent.decode(
        this.currentTarget);
    List<byte[]> encoded = new ArrayList<>(supplied.size());
    for (ReverseJournal journal : supplied) {
      ReverseJournal present = Objects.requireNonNull(journal, "journal");
      if (!Arrays.equals(cursor.encode(), present.getChildTarget())) {
        throw new IllegalArgumentException(
            "physical oracle window journal does not extend its current child");
      }
      byte[] journalBytes = present.encode();
      encoded.add(Arrays.copyOf(journalBytes, journalBytes.length));
      cursor = GlobalIntent.decode(present.getParentTarget());
    }
    this.oldestTarget = cursor.encode();
    this.encodedJournals = Collections.unmodifiableList(encoded);
  }

  @Override
  public int getBlockCount() {
    return encodedJournals.size();
  }

  @Override
  public PathStateRootMetadata getCurrentMetadata() {
    return GlobalIntent.decode(currentTarget).getMetadata();
  }

  @Override
  public PathStateRootMetadata getOldestMetadata() {
    return GlobalIntent.decode(oldestTarget).getMetadata();
  }

  @Override
  public List<ReverseJournal> journals() {
    List<ReverseJournal> copies = new ArrayList<>(encodedJournals.size());
    for (byte[] encoded : encodedJournals) {
      copies.add(ReverseJournal.decode(encoded));
    }
    return Collections.unmodifiableList(copies);
  }

  @Override
  public List<GlobalIntent> targets() {
    List<GlobalIntent> targets = new ArrayList<>(encodedJournals.size() + 1);
    GlobalIntent cursor = GlobalIntent.decode(currentTarget);
    targets.add(cursor);
    for (byte[] encoded : encodedJournals) {
      ReverseJournal journal = ReverseJournal.decode(encoded);
      cursor = GlobalIntent.decode(journal.getParentTarget());
      targets.add(cursor);
    }
    return Collections.unmodifiableList(targets);
  }

  private static byte[] owned(byte[] value, String name) {
    byte[] supplied = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    GlobalIntent.decode(supplied);
    return supplied;
  }
}
