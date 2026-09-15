package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Same-DB index snapshot with bounded, indexed reads of authoritative five-lane bodies. */
final class StateArchiveAppendReadAdapterV3 implements CheckpointPointHistory {

  private final PersistentServingKeyIndexGeneration index;
  private final StateArchiveFiveLaneSegmentWriterV3 source;
  private boolean closed;

  /** Takes ownership of the index, including on admission failure. Caller holds Common lease. */
  StateArchiveAppendReadAdapterV3(PersistentServingKeyIndexGeneration index,
      StateArchiveFiveLaneSegmentWriterV3 source, CommonCheckpointTarget target)
      throws IOException {
    this.index = index;
    this.source = source;
    try {
      if (index.getIndexedThrough() != target.getLastBlock().getBlockNumber()
          || !Arrays.equals(index.getHeadHash(), target.getLastBlock().getBlockHash())
          || !Arrays.equals(index.getLatestSourceIdentityDigest(), target.getPayloadDigest())
          || index.getIndexedFrom() != source.getHistoryStartBlock() - 1
          || !new java.util.HashSet<>(index.getParticipatingDatabases())
              .equals(ArchiveStoreScope.getStateDatabases())) {
        throw new IOException("Append query index differs from Common identity or exact coverage");
      }
    } catch (IOException | RuntimeException failure) {
      try {
        index.close();
      } catch (IOException | RuntimeException closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  @Override
  public synchronized Optional<OldValue> findOldValueAfter(String dbName, byte[] rawKey,
      long targetBlock) throws IOException {
    if (closed) {
      throw new IllegalStateException("Append history reader is closed");
    }
    OptionalLong first = index.firstChangeAfter(dbName, rawKey, targetBlock,
        index.getIndexedThrough());
    if (!first.isPresent()) {
      return Optional.empty();
    }
    long block = first.getAsLong();
    // One indexed block only, using the existing bounded serving decoder. Never scan history
    // or interpret a missing/corrupt indexed value as permission to use latest.
    List<BlockReverseDiff> diffs = source.readCommittedDiffs(block - 1, block,
        StateArchiveServingWorkerV3.MAX_SOURCE_BYTES);
    if (diffs.size() != 1 || diffs.get(0).getMeta().getBlockNumber() != block) {
      throw new IOException("Append point body differs from indexed block");
    }
    for (BlockReverseDiff.DbGroup group : diffs.get(0).getGroups()) {
      if (group.getDbName().equals(dbName)) {
        for (BlockReverseDiff.Entry entry : group.getEntries()) {
          if (Arrays.equals(entry.getKey(), rawKey)) {
            return Optional.of(entry.getOldValue());
          }
        }
      }
    }
    throw new IOException("Append point index references a missing authoritative old value");
  }

  @Override
  public long getIndexedFrom() {
    return index.getIndexedFrom();
  }

  @Override
  public long getIndexedThrough() {
    return index.getIndexedThrough();
  }

  @Override
  public byte[] getHeadHash() {
    return index.getHeadHash();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      index.close();
    }
  }
}
