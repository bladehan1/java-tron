package org.tron.core.db2.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Durable parent admitted before the first next-format common checkpoint. */
public final class CommonCheckpointBaseline {

  private final byte[] formatIdentity;
  private final BlockSnapshotMeta head;
  private final byte[] stateRoot;

  public CommonCheckpointBaseline(byte[] formatIdentity, BlockSnapshotMeta head,
      byte[] stateRoot) {
    this.formatIdentity = copy32(formatIdentity, "formatIdentity");
    this.head = Objects.requireNonNull(head, "head");
    this.stateRoot = copy32(stateRoot, "stateRoot");
  }

  public byte[] getFormatIdentity() {
    return Arrays.copyOf(formatIdentity, formatIdentity.length);
  }

  public BlockSnapshotMeta getHead() {
    return head;
  }

  public byte[] getStateRoot() {
    return Arrays.copyOf(stateRoot, stateRoot.length);
  }

  public void requireParent(CommonCheckpointTarget target, String authority) throws IOException {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    BlockSnapshotMeta first = admitted.getFirstBlock();
    if (!Arrays.equals(formatIdentity, admitted.getFormatIdentity())
        || head.getEpoch() + 1 != first.getEpoch()
        || head.getBlockNumber() + 1 != first.getBlockNumber()
        || !Arrays.equals(head.getBlockHash(), first.getParentHash())
        || !Arrays.equals(stateRoot, admitted.getParentStateRoot())) {
      throw new IOException(authority + " checkpoint does not extend the admitted baseline");
    }
  }

  private static byte[] copy32(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != 32) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return copy;
  }
}
