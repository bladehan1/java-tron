package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Objects;
import org.tron.core.db2.archive.BlockChangeView;

/** Independent non-consensus runtime installed at the metadata-aware block commit boundary. */
public final class PathStateRuntimeAttachment {

  private final PathStateTransitionCollector collector;
  private final TransitionSink sink;
  private final BaseFlushSink baseFlushSink;
  private Throwable failure;

  public PathStateRuntimeAttachment(PathStateTransitionCollector collector, TransitionSink sink) {
    this(collector, sink, (blockNumber, blockHash) -> { });
  }

  public PathStateRuntimeAttachment(PathStateTransitionCollector collector, TransitionSink sink,
      BaseFlushSink baseFlushSink) {
    this.collector = Objects.requireNonNull(collector, "collector");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.baseFlushSink = Objects.requireNonNull(baseFlushSink, "baseFlushSink");
  }

  /** Capture failures fail only this shadow runtime and never reject the canonical block. */
  public synchronized PathStateBlockTransition capture(BlockChangeView view) {
    if (failure != null) {
      return null;
    }
    try {
      return Objects.requireNonNull(collector.collect(view),
          "path-state collector returned null");
    } catch (IOException | RuntimeException currentFailure) {
      failure = currentFailure;
      return null;
    }
  }

  /** Durable publication failures are retained as observable fail-stop state. */
  public synchronized void publish(PathStateBlockTransition transition) {
    if (failure != null || transition == null) {
      return;
    }
    try {
      sink.accept(transition);
    } catch (IOException | RuntimeException currentFailure) {
      failure = currentFailure;
    }
  }

  /** Compacts only after Chainbase has durably refreshed the matching prefix. */
  public synchronized void flushBaseThrough(long blockNumber, byte[] blockHash) {
    if (failure != null) {
      return;
    }
    try {
      baseFlushSink.accept(blockNumber, blockHash);
    } catch (IOException | RuntimeException currentFailure) {
      failure = currentFailure;
    }
  }

  public synchronized boolean isFailed() {
    return failure != null;
  }

  public synchronized Throwable getFailure() {
    return failure;
  }

  /** Marks an externally coordinated lifecycle operation as failed without replacing first cause. */
  public synchronized void fail(Throwable currentFailure) {
    if (failure == null) {
      failure = Objects.requireNonNull(currentFailure, "currentFailure");
    }
  }

  @FunctionalInterface
  public interface TransitionSink {

    void accept(PathStateBlockTransition transition) throws IOException;
  }

  @FunctionalInterface
  public interface BaseFlushSink {

    void accept(long blockNumber, byte[] blockHash) throws IOException;
  }
}
