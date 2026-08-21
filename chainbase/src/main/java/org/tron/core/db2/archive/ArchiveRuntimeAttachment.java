package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Borrowed archive runtime collaborators installed into SnapshotManager as one unit. */
public final class ArchiveRuntimeAttachment {

  private final OldValueCollector collector;
  private final ArchiveBlockProjectionPreparer projectionPreparer;
  private final DurableBlockReverseDiffSink sink;
  private final ForwardFlushPublisher forwardFlushPublisher;

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      ArchiveBlockProjectionPreparer projectionPreparer, DurableBlockReverseDiffSink sink) {
    this(collector, projectionPreparer, sink, null);
  }

  public ArchiveRuntimeAttachment(OldValueCollector collector,
      ArchiveBlockProjectionPreparer projectionPreparer, DurableBlockReverseDiffSink sink,
      ForwardFlushPublisher forwardFlushPublisher) {
    this.collector = Objects.requireNonNull(collector, "collector");
    this.projectionPreparer = Objects.requireNonNull(projectionPreparer, "projectionPreparer");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.forwardFlushPublisher = forwardFlushPublisher;
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

  public boolean hasForwardFlushPublisher() {
    return forwardFlushPublisher != null;
  }

  public void publishForwardFlush(List<ArchiveBlockForwardPayload> payloads,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    if (forwardFlushPublisher == null) {
      throw new IllegalStateException("Archive runtime has no forward flush publisher");
    }
    forwardFlushPublisher.publish(payloads, refresh);
  }

  /** Publishes one frozen normal-flush target through C/D, refresh and R. */
  @FunctionalInterface
  public interface ForwardFlushPublisher {

    void publish(List<ArchiveBlockForwardPayload> payloads,
        ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException;
  }
}
