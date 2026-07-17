package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Scheme 2 collector: read old values from the completed block layer's previous view. */
public final class SnapshotOldValueCollector implements OldValueCollector {

  private final AccountAssetArchiveProjector accountAssetProjector;

  public SnapshotOldValueCollector() {
    this(null);
  }

  public SnapshotOldValueCollector(AccountAssetArchiveProjector accountAssetProjector) {
    this.accountAssetProjector = accountAssetProjector;
  }

  @Override
  public BlockReverseDiff collect(BlockChangeView view) {
    List<BlockReverseDiff.DbGroup> groups = new ArrayList<>();
    List<BlockReverseDiff.Entry> accountAssetEntries = new ArrayList<>();
    for (BlockChangeView.DatabaseChanges database : view.getDatabases()) {
      List<BlockReverseDiff.Entry> entries = new ArrayList<>();
      for (BlockChangeView.Change change : database.getChanges()) {
        byte[] key = change.getKey();
        OldValue oldValue = OldValue.fromNullable(database.getPrevious(key));
        BlockChangeView.PostValue postValue = change.getPostValue();
        if (accountAssetProjector != null
            && AccountAssetArchiveProjector.ACCOUNT_DB.equals(database.getDbName())) {
          AccountAssetArchiveProjector.Projection projection = accountAssetProjector.project(
              key, oldValue.isPresent() ? oldValue.getValue() : null, postValue);
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
