package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.tron.core.db2.common.WrappedByteArray;

/** Scheme 2 collector: read old values from the completed block layer's previous view. */
public final class SnapshotOldValueCollector implements OldValueCollector {

  private final AccountAssetArchiveProjector accountAssetProjector;
  private final AccountAssetOldPhysicalAssetsSource oldPhysicalAssetsSource;
  private final BooleanSupplier optimizationEnabled;

  public SnapshotOldValueCollector() {
    this(null, null, null);
  }

  public SnapshotOldValueCollector(AccountAssetArchiveProjector accountAssetProjector,
      AccountAssetOldPhysicalAssetsSource oldPhysicalAssetsSource,
      BooleanSupplier optimizationEnabled) {
    this.accountAssetProjector = accountAssetProjector;
    this.oldPhysicalAssetsSource = oldPhysicalAssetsSource;
    this.optimizationEnabled = optimizationEnabled;
    if (accountAssetProjector != null) {
      Objects.requireNonNull(oldPhysicalAssetsSource, "oldPhysicalAssetsSource");
      Objects.requireNonNull(optimizationEnabled, "optimizationEnabled");
    }
  }

  @Override
  public BlockReverseDiff collect(BlockChangeView view) {
    List<BlockReverseDiff.DbGroup> groups = new ArrayList<>();
    List<BlockReverseDiff.Entry> accountAssetEntries = new ArrayList<>();
    boolean targetAssetOptimizationEnabled = accountAssetProjector != null
        && optimizationEnabled.getAsBoolean();
    for (BlockChangeView.DatabaseChanges database : view.getDatabases()) {
      List<BlockReverseDiff.Entry> entries = new ArrayList<>();
      for (BlockChangeView.Change change : database.getChanges()) {
        byte[] key = change.getKey();
        OldValue oldValue = OldValue.fromNullable(database.getPrevious(key));
        BlockChangeView.PostValue postValue = change.getPostValue();
        if (accountAssetProjector != null
            && AccountAssetArchiveProjector.ACCOUNT_DB.equals(database.getDbName())) {
          Map<WrappedByteArray, byte[]> oldPhysicalAssets = Collections.emptyMap();
          if (accountAssetProjector.requiresOldPhysicalAssets(
              oldValue.isPresent() ? oldValue.getValue() : null, postValue)) {
            oldPhysicalAssets = AccountAssetOldPhysicalAssetsSource.captureRequired(
                oldPhysicalAssetsSource, key);
          }
          AccountAssetArchiveProjector.Projection projection = accountAssetProjector.project(
              key, oldValue.isPresent() ? oldValue.getValue() : null, postValue,
              targetAssetOptimizationEnabled, oldPhysicalAssets);
          oldValue = projection.oldAccount;
          postValue = projection.postAccount;
          accountAssetEntries.addAll(projection.reverseAssets);
        }
        if (!sameLogicalValue(oldValue, postValue)) {
          entries.add(new BlockReverseDiff.Entry(key, oldValue));
        }
      }
      if (!entries.isEmpty()) {
        groups.add(new BlockReverseDiff.DbGroup(database.getDbName(), entries));
      }
    }
    if (!accountAssetEntries.isEmpty()) {
      groups.add(new BlockReverseDiff.DbGroup(
          AccountAssetArchiveProjector.ACCOUNT_ASSET_DB, accountAssetEntries));
    }
    return new BlockReverseDiff(view.getMeta(), groups);
  }

  private boolean sameLogicalValue(OldValue oldValue, BlockChangeView.PostValue postValue) {
    if (oldValue.isPresent() != postValue.isPresent()) {
      return false;
    }
    return !oldValue.isPresent() || Arrays.equals(oldValue.getValue(), postValue.getValue());
  }
}
