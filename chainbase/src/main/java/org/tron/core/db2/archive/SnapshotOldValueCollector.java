package org.tron.core.db2.archive;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Scheme 2 collector: read old values from the completed block layer's previous view. */
public final class SnapshotOldValueCollector implements OldValueCollector {

  @Override
  public BlockReverseDiff collect(BlockChangeView view) {
    return BlockReverseDiff.collect(Objects.requireNonNull(view, "view"));
  }

  /** Resolves proposal 66 from the same immutable physical target block view. */
  public static boolean resolveTargetAssetOptimization(BlockChangeView view) {
    Objects.requireNonNull(view, "view");
    byte[] propertyKey = HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey();
    for (BlockChangeView.DatabaseChanges database : view.getDatabases()) {
      if (!HistoricalAccountAssetBalanceResolver.PROPERTIES_DATABASE.equals(
          database.getDbName())) {
        continue;
      }
      byte[] targetValue = database.getPrevious(propertyKey);
      for (BlockChangeView.Change change : database.getChanges()) {
        if (Arrays.equals(propertyKey, change.getKey())) {
          targetValue = change.getPostValue().isPresent()
              ? change.getPostValue().getValue() : null;
        }
      }
      if (targetValue == null || targetValue.length != Long.BYTES) {
        throw new ArchivePersistenceException(
            "Target proposal-66 property must be exactly eight bytes");
      }
      long enabled = ByteBuffer.wrap(targetValue).getLong();
      if (enabled != 0L && enabled != 1L) {
        throw new ArchivePersistenceException("Target proposal-66 property must be 0 or 1");
      }
      return enabled == 1L;
    }
    throw new ArchivePersistenceException("Target proposal-66 properties Store is absent");
  }

}
