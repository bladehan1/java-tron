package org.tron.core.db2.archive;

/** Archive sink whose committed history can gate checkpoint and disk-layer advancement. */
public interface DurableBlockReverseDiffSink extends BlockReverseDiffSink {

  void awaitCommitted(long epoch);

  void releaseThrough(long epoch);
}
