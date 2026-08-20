package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.AccountAssetForwardProjector.Projection;
import org.tron.core.db2.archive.ArchiveParticipantMutationBatch.Mutation;
import org.tron.core.db2.archive.BlockChangeView.Change;
import org.tron.core.db2.archive.BlockChangeView.DatabaseChanges;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

/** Converts one immutable block post-state view into a target-bound physical mutation batch. */
public final class ArchiveParticipantMutationBatchCollector {

  private final AccountAssetForwardProjector accountAssetProjector;
  private final String accountAssetFormatId;
  private final Phase targetPhase;
  private final List<String> participants;

  public ArchiveParticipantMutationBatchCollector(Phase targetPhase) {
    this(P66AccountAssetCodec.FORMAT_ID, targetPhase, null);
  }

  public ArchiveParticipantMutationBatchCollector(
      AccountAssetForwardMutationManifest manifest) {
    this(Objects.requireNonNull(manifest, "manifest").getFormatId(), manifest.getTargetPhase(),
        manifest);
  }

  public ArchiveParticipantMutationBatchCollector(Phase targetPhase,
      AccountAssetForwardProjector accountAssetProjector) {
    this(P66AccountAssetCodec.FORMAT_ID, targetPhase, accountAssetProjector);
  }

  private ArchiveParticipantMutationBatchCollector(String accountAssetFormatId,
      Phase targetPhase, AccountAssetForwardProjector accountAssetProjector) {
    this.accountAssetFormatId = Objects.requireNonNull(accountAssetFormatId,
        "accountAssetFormatId");
    this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
    this.accountAssetProjector = accountAssetProjector;
    participants = ArchiveParticipantDescriptor.current().getParticipants();
  }

  public ArchiveParticipantMutationBatch collect(HistoryCommitMarker committedTarget,
      BlockChangeView view) {
    HistoryCommitMarker target = Objects.requireNonNull(committedTarget, "committedTarget");
    BlockChangeView input = Objects.requireNonNull(view, "view");
    if (!target.getMeta().equals(input.getMeta())) {
      throw new ArchivePersistenceException("Block mutation view target identity mismatch");
    }
    requireExactCoverage(target, input);
    List<byte[]> changedAccountKeys = changedAccountKeys(input);
    if (!changedAccountKeys.isEmpty() && accountAssetProjector == null) {
      throw new ArchivePersistenceException(
          "Account mutation requires an explicit AccountAsset forward projector");
    }
    if (accountAssetProjector != null) {
      accountAssetProjector.begin(target, changedAccountKeys);
    }
    List<Mutation> mutations = new ArrayList<>();
    for (DatabaseChanges database : input.getDatabases()) {
      for (Change change : database.getChanges()) {
        if (AccountAssetArchiveProjector.ACCOUNT_DB.equals(database.getDbName())) {
          collectAccount(change, mutations);
        } else {
          mutations.add(toMutation(database.getDbName(), change.getKey(),
              change.getPostValue()));
        }
      }
    }
    if (accountAssetProjector != null) {
      accountAssetProjector.complete();
    }
    return new ArchiveParticipantMutationBatch(target, accountAssetFormatId, targetPhase,
        mutations);
  }

  private void collectAccount(Change change, List<Mutation> mutations) {
    if (accountAssetProjector == null) {
      throw new ArchivePersistenceException(
          "Account mutation requires an explicit AccountAsset forward projector");
    }
    byte[] accountKey = change.getKey();
    Projection projection = accountAssetProjector.project(accountKey, change.getPostValue());
    if (projection == null) {
      throw new ArchivePersistenceException("AccountAsset forward projection is missing");
    }
    mutations.add(toMutation(AccountAssetArchiveProjector.ACCOUNT_DB, accountKey,
        projection.getAccountPostValue()));
    for (AssetMutation asset : projection.getAssetMutations()) {
      mutations.add(toMutation(AccountAssetArchiveProjector.ACCOUNT_ASSET_DB,
          asset.getPhysicalRawKey(), asset.getPostValue()));
    }
  }

  private void requireExactCoverage(HistoryCommitMarker target, BlockChangeView view) {
    List<String> actual = new ArrayList<>();
    for (DatabaseChanges database : view.getDatabases()) {
      actual.add(database.getDbName());
    }
    Collections.sort(actual);
    if (!actual.equals(participants) || !target.getDatabases().equals(participants)) {
      throw new ArchivePersistenceException(
          "Block mutation view does not cover the exact VERSIONED_STATE set");
    }
  }

  private static List<byte[]> changedAccountKeys(BlockChangeView view) {
    List<byte[]> keys = new ArrayList<>();
    for (DatabaseChanges database : view.getDatabases()) {
      if (AccountAssetArchiveProjector.ACCOUNT_DB.equals(database.getDbName())) {
        for (Change change : database.getChanges()) {
          keys.add(change.getKey());
        }
      }
    }
    return keys;
  }

  private static Mutation toMutation(String dbName, byte[] key, PostValue postValue) {
    return postValue.isPresent()
        ? Mutation.put(dbName, key, postValue.getValue())
        : Mutation.delete(dbName, key);
  }
}
