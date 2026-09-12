package org.tron.core.db2.archive;

import java.util.Objects;

/** Borrowed archive runtime collaborators installed into SnapshotManager as one unit. */
public final class ArchiveRuntimeAttachment {

  private final OldValueCollector collector;
  private final DurableBlockReverseDiffSink sink;
  private final ArchiveCommittedPrefixPublisher committedPrefixPublisher;
  private final ArchiveCommittedPrefixPublisher readableStatePublisher;

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      DurableBlockReverseDiffSink sink) {
    this(collector, sink, null, null);
  }

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      DurableBlockReverseDiffSink sink,
      ArchiveCommittedPrefixPublisher committedPrefixPublisher) {
    this(collector, sink, committedPrefixPublisher, null);
  }

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      DurableBlockReverseDiffSink sink,
      ArchiveCommittedPrefixPublisher committedPrefixPublisher,
      ArchiveCommittedPrefixPublisher readableStatePublisher) {
    this.collector = Objects.requireNonNull(collector, "collector");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.committedPrefixPublisher = committedPrefixPublisher;
    this.readableStatePublisher = readableStatePublisher;
  }

  public OldValueCollector getCollector() {
    return collector;
  }

  public DurableBlockReverseDiffSink getSink() {
    return sink;
  }

  public void publishCommittedPrefix(BlockSnapshotMeta target) throws java.io.IOException {
    if (committedPrefixPublisher != null) {
      committedPrefixPublisher.publish(Objects.requireNonNull(target, "target"));
    }
  }

  public void publishReadableState(BlockSnapshotMeta target) throws java.io.IOException {
    if (readableStatePublisher != null) {
      readableStatePublisher.publish(Objects.requireNonNull(target, "target"));
    }
  }
}
