package org.tron.core.db2.archive;

import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.common.WrappedByteArray;

/** Commit-time source for one changed account's old physical account-asset rows. */
@FunctionalInterface
public interface AccountAssetOldPhysicalAssetsSource {

  /** Returns the complete old physical rows for {@code accountKey}. */
  Map<WrappedByteArray, byte[]> capture(byte[] accountKey);

  static Map<WrappedByteArray, byte[]> captureRequired(
      AccountAssetOldPhysicalAssetsSource source, byte[] accountKey) {
    try {
      Map<WrappedByteArray, byte[]> captured = Objects.requireNonNull(
          Objects.requireNonNull(source, "source").capture(accountKey),
          "old physical AccountAsset input");
      return captured;
    } catch (ArchivePersistenceException failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw new ArchivePersistenceException(
          "Failed to capture old physical AccountAsset input", failure);
    }
  }
}
