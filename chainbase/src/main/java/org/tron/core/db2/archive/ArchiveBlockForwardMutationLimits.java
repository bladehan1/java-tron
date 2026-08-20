package org.tron.core.db2.archive;

/** Explicit standalone bounds for one block's complete forward mutation capture. */
public final class ArchiveBlockForwardMutationLimits {

  private final int maxAccounts;
  private final int maxAssetMutations;
  private final int maxKeyBytes;
  private final int maxValueBytes;
  private final long maxTotalPayloadBytes;

  public ArchiveBlockForwardMutationLimits(int maxAccounts, int maxAssetMutations,
      int maxKeyBytes, int maxValueBytes, long maxTotalPayloadBytes) {
    if (maxAccounts < 0 || maxAssetMutations < 0 || maxKeyBytes < 0
        || maxValueBytes < 0 || maxTotalPayloadBytes < 0) {
      throw new IllegalArgumentException("Block forward mutation limits must not be negative");
    }
    this.maxAccounts = maxAccounts;
    this.maxAssetMutations = maxAssetMutations;
    this.maxKeyBytes = maxKeyBytes;
    this.maxValueBytes = maxValueBytes;
    this.maxTotalPayloadBytes = maxTotalPayloadBytes;
  }

  int getMaxAccounts() {
    return maxAccounts;
  }

  int getMaxAssetMutations() {
    return maxAssetMutations;
  }

  int getMaxKeyBytes() {
    return maxKeyBytes;
  }

  int getMaxValueBytes() {
    return maxValueBytes;
  }

  long getMaxTotalPayloadBytes() {
    return maxTotalPayloadBytes;
  }
}
