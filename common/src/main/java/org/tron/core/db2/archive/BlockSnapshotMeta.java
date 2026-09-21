package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable identity of a successfully applied canonical block snapshot.
 */
public final class BlockSnapshotMeta {

  private static final int HASH_LENGTH = 32;

  private final long epoch;
  private final long blockNumber;
  private final byte[] blockHash;
  private final byte[] parentHash;
  private final long timestamp;

  public BlockSnapshotMeta(long epoch, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp) {
    if (epoch < 0) {
      throw new IllegalArgumentException("epoch must not be negative");
    }
    if (blockNumber < 0) {
      throw new IllegalArgumentException("blockNumber must not be negative");
    }
    this.epoch = epoch;
    this.blockNumber = blockNumber;
    this.blockHash = copyHash(blockHash, "blockHash");
    this.parentHash = copyHash(parentHash, "parentHash");
    this.timestamp = timestamp;
  }

  public static BlockSnapshotMeta forBlock(long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp) {
    return new BlockSnapshotMeta(blockNumber, blockNumber, blockHash, parentHash, timestamp);
  }

  private static byte[] copyHash(byte[] hash, String name) {
    Objects.requireNonNull(hash, name);
    if (hash.length != HASH_LENGTH) {
      throw new IllegalArgumentException(name + " must be exactly " + HASH_LENGTH + " bytes");
    }
    return Arrays.copyOf(hash, hash.length);
  }

  public long getEpoch() {
    return epoch;
  }

  public long getBlockNumber() {
    return blockNumber;
  }

  public byte[] getBlockHash() {
    return Arrays.copyOf(blockHash, blockHash.length);
  }

  public byte[] getParentHash() {
    return Arrays.copyOf(parentHash, parentHash.length);
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BlockSnapshotMeta)) {
      return false;
    }
    BlockSnapshotMeta that = (BlockSnapshotMeta) object;
    return epoch == that.epoch
        && blockNumber == that.blockNumber
        && timestamp == that.timestamp
        && Arrays.equals(blockHash, that.blockHash)
        && Arrays.equals(parentHash, that.parentHash);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(epoch, blockNumber, timestamp);
    result = 31 * result + Arrays.hashCode(blockHash);
    result = 31 * result + Arrays.hashCode(parentHash);
    return result;
  }

  @Override
  public String toString() {
    return "BlockSnapshotMeta{"
        + "epoch=" + epoch
        + ", blockNumber=" + blockNumber
        + ", timestamp=" + timestamp
        + '}';
  }
}
