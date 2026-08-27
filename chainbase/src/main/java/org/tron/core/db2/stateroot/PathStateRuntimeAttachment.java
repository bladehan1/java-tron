package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.db2.archive.BlockChangeView;

/** Independent non-consensus runtime installed at the metadata-aware block commit boundary. */
@Slf4j(topic = "DB")
public final class PathStateRuntimeAttachment {

  private final PathStateTransitionCollector collector;
  private final TransitionSink sink;
  private final BaseFlushSink baseFlushSink;
  private Throwable failure;
  private FailureStage failureStage;
  private long readyBlockNumber = -1;
  private byte[] readyBlockHash;
  private long observedBlockNumber = -1;
  private byte[] observedBlockHash;
  private PathStateBlockTransition pending;

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
    BlockChangeView admitted = Objects.requireNonNull(view, "view");
    observe(admitted);
    if (failure != null) {
      return null;
    }
    try {
      PathStateBlockTransition transition = Objects.requireNonNull(collector.collect(admitted),
          "path-state collector returned null");
      if (transition.getBlockNumber() != admitted.getMeta().getBlockNumber()
          || !Arrays.equals(transition.getBlockHash(), admitted.getMeta().getBlockHash())
          || !Arrays.equals(transition.getParentHash(), admitted.getMeta().getParentHash())
          || transition.getTimestamp() != admitted.getMeta().getTimestamp()) {
        throw new IOException("path-state collector changed the captured block identity");
      }
      pending = transition;
      return transition;
    } catch (IOException | RuntimeException currentFailure) {
      fail(FailureStage.CAPTURE, currentFailure);
      return null;
    }
  }

  /** Durable publication failures are retained as observable fail-stop state. */
  public synchronized void publish(PathStateBlockTransition transition) {
    if (failure != null || transition == null) {
      return;
    }
    try {
      if (pending != transition) {
        throw new IOException("path-state publication differs from captured transition");
      }
      sink.accept(transition);
      readyBlockNumber = transition.getBlockNumber();
      readyBlockHash = transition.getBlockHash();
      pending = null;
    } catch (IOException | RuntimeException currentFailure) {
      fail(FailureStage.PUBLISH, currentFailure);
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
      fail(FailureStage.BASE_FLUSH, currentFailure);
    }
  }

  public synchronized boolean isFailed() {
    return failure != null;
  }

  public synchronized Throwable getFailure() {
    return failure;
  }

  /** Returns a copy-only diagnostic snapshot; it never repairs or guesses a root. */
  public synchronized Status status() {
    State state = failure != null ? State.FAILED
        : pending == null && readyBlockNumber >= 0
        && readyBlockNumber == observedBlockNumber
        && Arrays.equals(readyBlockHash, observedBlockHash) ? State.READY : State.NOT_READY;
    return new Status(state, readyBlockNumber, readyBlockHash, observedBlockNumber,
        observedBlockHash, failureStage, classify(failure), failure);
  }

  /** Seeds or rewinds the exact verified durable head; failed runtimes cannot be reset. */
  public synchronized void synchronizeReadyHead(PathStateRootMetadata metadata) {
    if (failure != null) {
      throw new IllegalStateException("failed path-state runtime cannot become ready");
    }
    if (pending != null) {
      throw new IllegalStateException("captured path-state transition is not published");
    }
    PathStateRootMetadata admitted = Objects.requireNonNull(metadata, "metadata");
    readyBlockNumber = admitted.getBlockNumber();
    readyBlockHash = admitted.getBlockHash();
    observedBlockNumber = readyBlockNumber;
    observedBlockHash = copy(readyBlockHash);
  }

  /** Marks an externally coordinated lifecycle operation as failed without replacing first cause. */
  public synchronized void fail(Throwable currentFailure) {
    fail(FailureStage.EXTERNAL, currentFailure);
  }

  public synchronized void fail(FailureStage stage, Throwable currentFailure) {
    if (failure == null) {
      failure = Objects.requireNonNull(currentFailure, "currentFailure");
      failureStage = Objects.requireNonNull(stage, "stage");
      logger.error(
          "Path-state runtime fail-stop: stage={}, kind={}, readyBlock={}, observedBlock={}, "
              + "rootLag={}",
          failureStage, classify(failure), readyBlockNumber, observedBlockNumber,
          lag(readyBlockNumber, observedBlockNumber),
          failure);
    }
  }

  /** Records the exact canonical target of a failed external lifecycle operation. */
  public synchronized void failAt(FailureStage stage, long blockNumber, byte[] blockHash,
      Throwable currentFailure) {
    observedBlockNumber = blockNumber;
    observedBlockHash = copy(Objects.requireNonNull(blockHash, "blockHash"));
    fail(stage, currentFailure);
  }

  private void observe(BlockChangeView view) {
    long number = view.getMeta().getBlockNumber();
    byte[] hash = view.getMeta().getBlockHash();
    byte[] parentHash = view.getMeta().getParentHash();
    if (failure == null) {
      if (pending != null) {
        fail(FailureStage.CAPTURE_GAP,
            new IOException("path-state previous capture is not published"));
      }
      long expectedParentNumber = observedBlockNumber >= 0
          ? observedBlockNumber : readyBlockNumber;
      byte[] expectedParentHash = observedBlockHash != null
          ? observedBlockHash : readyBlockHash;
      if (expectedParentNumber >= 0 && (number != expectedParentNumber + 1
          || !Arrays.equals(parentHash, expectedParentHash))) {
        fail(FailureStage.CAPTURE_GAP,
            new IOException("path-state block-final capture is not continuous"));
      }
    }
    observedBlockNumber = number;
    observedBlockHash = copy(hash);
  }

  private static FailureKind classify(Throwable failure) {
    if (failure == null) {
      return FailureKind.NONE;
    }
    StringBuilder messages = new StringBuilder();
    Throwable current = failure;
    boolean io = false;
    while (current != null) {
      io |= current instanceof IOException;
      if (current.getMessage() != null) {
        messages.append(' ').append(current.getMessage().toLowerCase(Locale.ROOT));
      }
      current = current.getCause();
    }
    String text = messages.toString();
    if (text.contains("no space left") || text.contains("disk full")
        || text.contains("out of space")) {
      return FailureKind.STORAGE_FULL;
    }
    if (text.contains("corrupt") || text.contains("checksum") || text.contains("mismatch")
        || text.contains("orphan") || text.contains("native progress")) {
      return FailureKind.CORRUPTION;
    }
    return io ? FailureKind.IO : FailureKind.RUNTIME;
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  private static long lag(long readyBlockNumber, long observedBlockNumber) {
    if (readyBlockNumber < 0 || observedBlockNumber < 0) {
      return -1;
    }
    return observedBlockNumber >= readyBlockNumber
        ? observedBlockNumber - readyBlockNumber : readyBlockNumber - observedBlockNumber;
  }

  public enum State {
    NOT_READY,
    READY,
    FAILED
  }

  public enum FailureStage {
    CAPTURE_GAP,
    CAPTURE,
    PUBLISH,
    BASE_FLUSH,
    REORG,
    EXTERNAL
  }

  public enum FailureKind {
    NONE,
    STORAGE_FULL,
    CORRUPTION,
    IO,
    RUNTIME
  }

  public static final class Status {

    private final State state;
    private final long readyBlockNumber;
    private final byte[] readyBlockHash;
    private final long observedBlockNumber;
    private final byte[] observedBlockHash;
    private final FailureStage failureStage;
    private final FailureKind failureKind;
    private final String failureType;
    private final String failureMessage;

    private Status(State state, long readyBlockNumber, byte[] readyBlockHash,
        long observedBlockNumber, byte[] observedBlockHash, FailureStage failureStage,
        FailureKind failureKind, Throwable failure) {
      this.state = state;
      this.readyBlockNumber = readyBlockNumber;
      this.readyBlockHash = copy(readyBlockHash);
      this.observedBlockNumber = observedBlockNumber;
      this.observedBlockHash = copy(observedBlockHash);
      this.failureStage = failureStage;
      this.failureKind = failureKind;
      this.failureType = failure == null ? null : failure.getClass().getName();
      this.failureMessage = failure == null ? null : failure.getMessage();
    }

    public State getState() {
      return state;
    }

    public long getReadyBlockNumber() {
      return readyBlockNumber;
    }

    public byte[] getReadyBlockHash() {
      return copy(readyBlockHash);
    }

    public long getObservedBlockNumber() {
      return observedBlockNumber;
    }

    public byte[] getObservedBlockHash() {
      return copy(observedBlockHash);
    }

    public long getRootLag() {
      return lag(readyBlockNumber, observedBlockNumber);
    }

    public FailureStage getFailureStage() {
      return failureStage;
    }

    public FailureKind getFailureKind() {
      return failureKind;
    }

    public String getFailureType() {
      return failureType;
    }

    public String getFailureMessage() {
      return failureMessage;
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
