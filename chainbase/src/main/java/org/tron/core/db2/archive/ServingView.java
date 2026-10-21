package org.tron.core.db2.archive;

import java.util.Arrays;

/**
 * Immutable serving status published through a volatile reference (ch04 §3.4.1, L5). Query
 * threads reach the write handle only through the {@code ServingView} they read (§3.10.1).
 */
public final class ServingView {

  private final ServingState state;
  private final long stateSinceMillis;
  private final long indexedFrom;
  private final long indexedThrough;
  private final byte[] headHash;
  private final long committedThrough;
  private final ServingFailure lastFailure;
  private final boolean rebuildInProgress;
  private final long handleSeq;
  private final long backlogBytes;
  private final long oldestBacklogAgeMillis;
  private final ServingIndexStore store;

  ServingView(ServingState state, long stateSinceMillis, long indexedFrom, long indexedThrough,
      byte[] headHash, long committedThrough, ServingFailure lastFailure,
      boolean rebuildInProgress, long handleSeq, long backlogBytes,
      long oldestBacklogAgeMillis, ServingIndexStore store) {
    this.state = state;
    this.stateSinceMillis = stateSinceMillis;
    this.indexedFrom = indexedFrom;
    this.indexedThrough = indexedThrough;
    this.headHash = headHash == null ? null : Arrays.copyOf(headHash, headHash.length);
    this.committedThrough = committedThrough;
    this.lastFailure = lastFailure;
    this.rebuildInProgress = rebuildInProgress;
    this.handleSeq = handleSeq;
    this.backlogBytes = backlogBytes;
    this.oldestBacklogAgeMillis = oldestBacklogAgeMillis;
    this.store = store;
  }

  public ServingState getState() {
    return state;
  }

  public long getStateSinceMillis() {
    return stateSinceMillis;
  }

  /** {@code B0 - 1}, or -1 before the index is open. */
  public long getIndexedFrom() {
    return indexedFrom;
  }

  /** Published {@code I}, or -1 before the index is open. */
  public long getIndexedThrough() {
    return indexedThrough;
  }

  public byte[] getHeadHash() {
    return headHash == null ? null : Arrays.copyOf(headHash, headHash.length);
  }

  /** Latest observed History boundary {@code W}, or -1 before the first observation. */
  public long getCommittedThrough() {
    return committedThrough;
  }

  /** {@code W - I}; 0 when unknown. */
  public long getBacklogBlocks() {
    return indexedThrough < 0 || committedThrough < indexedThrough ? 0
        : committedThrough - indexedThrough;
  }

  public long getBacklogBytes() {
    return backlogBytes;
  }

  public long getOldestBacklogAgeMillis() {
    return oldestBacklogAgeMillis;
  }

  public ServingFailure getLastFailure() {
    return lastFailure;
  }

  public boolean isRebuildInProgress() {
    return rebuildInProgress;
  }

  public long getHandleSeq() {
    return handleSeq;
  }

  ServingIndexStore store() {
    return store;
  }

  @Override
  public String toString() {
    return "ServingView{" + state + ", I=" + indexedThrough + ", W=" + committedThrough
        + ", from=" + indexedFrom + ", handle=" + handleSeq
        + (lastFailure == null ? "" : ", failure=" + lastFailure) + '}';
  }
}
