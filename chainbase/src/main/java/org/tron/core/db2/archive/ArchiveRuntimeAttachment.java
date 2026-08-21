package org.tron.core.db2.archive;

import java.util.Objects;

/** Borrowed archive runtime collaborators installed into SnapshotManager as one unit. */
public final class ArchiveRuntimeAttachment {

  private final OldValueCollector collector;
  private final ArchiveBlockProjectionPreparer projectionPreparer;
  private final DurableBlockReverseDiffSink sink;

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      ArchiveBlockProjectionPreparer projectionPreparer, DurableBlockReverseDiffSink sink) {
    this.collector = Objects.requireNonNull(collector, "collector");
    this.projectionPreparer = Objects.requireNonNull(projectionPreparer, "projectionPreparer");
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  public OldValueCollector getCollector() {
    return collector;
  }

  public ArchiveBlockProjectionPreparer getProjectionPreparer() {
    return projectionPreparer;
  }

  public DurableBlockReverseDiffSink getSink() {
    return sink;
  }
}
