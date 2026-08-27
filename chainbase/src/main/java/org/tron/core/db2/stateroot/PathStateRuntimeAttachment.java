package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Objects;
import org.tron.core.db2.archive.BlockChangeView;

/** Independent non-consensus runtime installed at the metadata-aware block commit boundary. */
public final class PathStateRuntimeAttachment {

  private final PathStateTransitionCollector collector;
  private final TransitionSink sink;
  private Throwable failure;

  public PathStateRuntimeAttachment(PathStateTransitionCollector collector, TransitionSink sink) {
    this.collector = Objects.requireNonNull(collector, "collector");
    this.sink = Objects.requireNonNull(sink, "sink");
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

  public synchronized boolean isFailed() {
    return failure != null;
  }

  public synchronized Throwable getFailure() {
    return failure;
  }

  @FunctionalInterface
  public interface TransitionSink {

    void accept(PathStateBlockTransition transition) throws IOException;
  }
}
