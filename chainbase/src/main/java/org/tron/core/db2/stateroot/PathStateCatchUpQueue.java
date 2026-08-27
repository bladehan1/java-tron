package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;

/**
 * Bounded handoff from a fixed rebuild snapshot to normal block-final path-state processing.
 *
 * <p>Transitions captured while BASE(P0) is rebuilding remain in memory and must form one exact
 * block/hash chain above P0. BASE publication and the switch to draining share one synchronized
 * gate, so capture cannot pass through that boundary unnoticed. Once the queue becomes READY,
 * callers must send later transitions through the normal direct-apply path.
 */
public final class PathStateCatchUpQueue {

  private final SnapshotIdentity snapshot;
  private final int maxTransitions;
  private final long maxMutations;
  private final long maxBytes;
  private final DrainHook drainHook;
  private final Deque<PathStateBlockTransition> transitions = new ArrayDeque<>();
  private State state = State.CAPTURING;
  private long queuedMutations;
  private long queuedBytes;
  private PathStateRootMetadata readyHead;

  public PathStateCatchUpQueue(SnapshotIdentity snapshot, int maxTransitions,
      long maxMutations, long maxBytes) {
    this(snapshot, maxTransitions, maxMutations, maxBytes, transition -> { });
  }

  PathStateCatchUpQueue(SnapshotIdentity snapshot, int maxTransitions,
      long maxMutations, long maxBytes, DrainHook drainHook) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    if (maxTransitions <= 0 || maxMutations <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("path-state catch-up limits must be positive");
    }
    this.maxTransitions = maxTransitions;
    this.maxMutations = maxMutations;
    this.maxBytes = maxBytes;
    this.drainHook = Objects.requireNonNull(drainHook, "drainHook");
  }

  /** Queues a rebuilding transition, or explicitly returns it to the post-READY direct path. */
  public synchronized CaptureDisposition capture(PathStateBlockTransition transition)
      throws IOException {
    PathStateBlockTransition admitted = Objects.requireNonNull(transition, "transition");
    if (state == State.FAILED) {
      throw new IOException("path-state catch-up queue has failed");
    }
    if (state == State.READY) {
      return CaptureDisposition.DIRECT_APPLY;
    }
    requireNext(admitted);
    long nextMutations;
    long nextBytes;
    try {
      nextMutations = Math.addExact(queuedMutations, admitted.getMutations().size());
      nextBytes = Math.addExact(queuedBytes, logicalBytes(admitted));
    } catch (ArithmeticException overflow) {
      return failOverflow(overflow);
    }
    if (transitions.size() >= maxTransitions || nextMutations > maxMutations
        || nextBytes > maxBytes) {
      return failOverflow(null);
    }
    transitions.addLast(admitted);
    queuedMutations = nextMutations;
    queuedBytes = nextBytes;
    return CaptureDisposition.QUEUED;
  }

  public synchronized State getState() {
    return state;
  }

  public synchronized int getQueuedTransitions() {
    return transitions.size();
  }

  public synchronized long getQueuedMutations() {
    return queuedMutations;
  }

  public synchronized long getQueuedBytes() {
    return queuedBytes;
  }

  public synchronized PathStateRootMetadata getReadyHead() {
    return readyHead;
  }

  synchronized void admitSnapshot(SnapshotIdentity identity) throws IOException {
    requireCapturing();
    if (!snapshot.sameAs(Objects.requireNonNull(identity, "identity"))) {
      state = State.FAILED;
      throw new IOException("path-state catch-up snapshot identity mismatch");
    }
  }

  synchronized PathStateRootMetadata publishBase(SnapshotIdentity identity,
      PathStateRootMetadata base, BasePublisher publisher) throws IOException {
    requireCapturing();
    requireBase(identity, base);
    try {
      PathStateRootMetadata published = Objects.requireNonNull(
          publisher.publish(), "published BASE");
      if (!Arrays.equals(base.encode(), published.encode())) {
        throw new IOException("path-state catch-up published BASE identity mismatch");
      }
      state = State.DRAINING;
      readyHead = published;
      return published;
    } catch (IOException | RuntimeException failure) {
      state = State.FAILED;
      throw failure;
    }
  }

  PathStateRootMetadata drain(PathStateStoreManifest manifest, PathStateLayerLimits limits)
      throws IOException {
    PathStateStoreManifest admittedManifest = Objects.requireNonNull(manifest, "manifest");
    PathStateLayerLimits admittedLimits = Objects.requireNonNull(limits, "limits");
    while (true) {
      PathStateBlockTransition transition;
      PathStateRootMetadata parent;
      synchronized (this) {
        if (state == State.FAILED) {
          throw new IOException("path-state catch-up queue has failed");
        }
        if (state == State.READY) {
          return readyHead;
        }
        if (state != State.DRAINING) {
          throw new IOException("path-state catch-up BASE is not published");
        }
        transition = transitions.peekFirst();
        if (transition == null) {
          state = State.READY;
          return readyHead;
        }
        parent = readyHead;
      }
      try {
        PathStateRootMetadata committed;
        try (PathStateLayer layer = PathStateLayer.begin(admittedManifest, parent,
            transition.getBlockNumber(), transition.getBlockHash(), transition.getParentHash(),
            transition.getTimestamp(), transition.getPhase(), transition.getPayloadDigest(),
            admittedLimits)) {
          layer.apply(transition.getMutations());
          committed = layer.commit();
        }
        synchronized (this) {
          if (transitions.peekFirst() != transition || state != State.DRAINING) {
            throw new IOException("path-state catch-up queue changed during drain");
          }
          transitions.removeFirst();
          queuedMutations -= transition.getMutations().size();
          queuedBytes -= logicalBytes(transition);
          readyHead = committed;
        }
        drainHook.afterCommit(transition);
      } catch (IOException | RuntimeException failure) {
        synchronized (this) {
          state = State.FAILED;
        }
        throw failure;
      }
    }
  }

  private void requireNext(PathStateBlockTransition transition) throws IOException {
    PathStateBlockTransition tail = transitions.peekLast();
    long parentNumber;
    byte[] parentHash;
    if (tail != null) {
      parentNumber = tail.getBlockNumber();
      parentHash = tail.getBlockHash();
    } else if (state == State.DRAINING) {
      parentNumber = readyHead.getBlockNumber();
      parentHash = readyHead.getBlockHash();
    } else {
      parentNumber = snapshot.getBlockNumber();
      parentHash = snapshot.getBlockHash();
    }
    if (transition.getBlockNumber() != parentNumber + 1
        || !Arrays.equals(transition.getParentHash(), parentHash)) {
      state = State.FAILED;
      throw new IOException("path-state catch-up transition is not block/hash continuous");
    }
  }

  private void requireCapturing() throws IOException {
    if (state != State.CAPTURING) {
      throw new IOException("path-state catch-up BASE publication is not admissible");
    }
  }

  private void requireBase(SnapshotIdentity identity, PathStateRootMetadata base)
      throws IOException {
    SnapshotIdentity admittedIdentity = Objects.requireNonNull(identity, "identity");
    PathStateRootMetadata admittedBase = Objects.requireNonNull(base, "base");
    if (!snapshot.sameAs(admittedIdentity)
        || admittedBase.getKind() != Kind.BASE
        || admittedBase.getBlockNumber() != snapshot.getBlockNumber()
        || admittedBase.getTimestamp() != snapshot.getTimestamp()
        || admittedBase.getPhase() != snapshot.getPhase()
        || !Arrays.equals(admittedBase.getBlockHash(), snapshot.getBlockHash())
        || !Arrays.equals(admittedBase.getParentHash(), snapshot.getParentHash())) {
      state = State.FAILED;
      throw new IOException("path-state catch-up snapshot and BASE identity mismatch");
    }
  }

  private CaptureDisposition failOverflow(ArithmeticException cause) throws IOException {
    state = State.FAILED;
    IOException failure = new IOException("path-state catch-up queue limit exceeded");
    if (cause != null) {
      failure.initCause(cause);
    }
    throw failure;
  }

  private static long logicalBytes(PathStateBlockTransition transition) {
    long bytes = Long.BYTES * 2L + PathStateBlockTransition.HASH_LENGTH * 2L
        + Integer.BYTES;
    for (PathStateMutation mutation : transition.getMutations()) {
      bytes = Math.addExact(bytes,
          mutation.getDbName().getBytes(StandardCharsets.UTF_8).length);
      bytes = Math.addExact(bytes, mutation.getCanonicalKey().length);
      byte[] value = mutation.getCanonicalValue();
      if (value != null) {
        bytes = Math.addExact(bytes, value.length);
      }
      bytes = Math.addExact(bytes, Integer.BYTES * 3L + 1L);
    }
    return bytes;
  }

  public enum CaptureDisposition {
    QUEUED,
    DIRECT_APPLY
  }

  public enum State {
    CAPTURING,
    DRAINING,
    READY,
    FAILED
  }

  @FunctionalInterface
  interface BasePublisher {

    PathStateRootMetadata publish() throws IOException;
  }

  @FunctionalInterface
  interface DrainHook {

    void afterCommit(PathStateBlockTransition transition) throws IOException;
  }

}
