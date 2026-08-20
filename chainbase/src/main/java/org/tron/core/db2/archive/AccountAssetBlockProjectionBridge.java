package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.AccountAssetForwardMutationManifest.Entry;
import org.tron.core.db2.archive.BlockChangeView.Change;
import org.tron.core.db2.archive.BlockChangeView.DatabaseChanges;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.common.WrappedByteArray;

/**
 * Standalone bridge which prepares reverse and forward projections before a durable history marker
 * exists, then seals the forward projection against that marker exactly once.
 */
public final class AccountAssetBlockProjectionBridge {

  private final AccountAssetArchiveProjector projector;
  private final AccountAssetOldPhysicalAssetsSource oldPhysicalAssetsSource;
  private final List<String> participants;

  public AccountAssetBlockProjectionBridge(AccountAssetArchiveProjector projector,
      AccountAssetOldPhysicalAssetsSource oldPhysicalAssetsSource) {
    this.projector = Objects.requireNonNull(projector, "projector");
    this.oldPhysicalAssetsSource = Objects.requireNonNull(oldPhysicalAssetsSource,
        "oldPhysicalAssetsSource");
    participants = ArchiveParticipantDescriptor.current().getParticipants();
  }

  public PreparedBlockProjection prepare(BlockChangeView view,
      TargetAssetOptimization activation) {
    BlockChangeView input = Objects.requireNonNull(view, "view");
    TargetAssetOptimization targetActivation = Objects.requireNonNull(activation, "activation");
    validateBeforeProjection(input, targetActivation);

    List<BlockReverseDiff.DbGroup> groups = new ArrayList<>();
    List<BlockReverseDiff.Entry> accountAssetEntries = new ArrayList<>();
    List<Entry> forwardEntries = new ArrayList<>();
    for (DatabaseChanges database : input.getDatabases()) {
      List<BlockReverseDiff.Entry> entries = new ArrayList<>();
      for (Change change : database.getChanges()) {
        byte[] key = change.getKey();
        OldValue oldValue = OldValue.fromNullable(database.getPrevious(key));
        PostValue postValue = change.getPostValue();
        if (AccountAssetArchiveProjector.ACCOUNT_DB.equals(database.getDbName())) {
          Map<WrappedByteArray, byte[]> oldPhysicalAssets = Collections.emptyMap();
          if (projector.requiresOldPhysicalAssets(
              oldValue.isPresent() ? oldValue.getValue() : null, postValue)) {
            oldPhysicalAssets = AccountAssetOldPhysicalAssetsSource.captureRequired(
                oldPhysicalAssetsSource, key);
          }
          AccountAssetArchiveProjector.Projection projection = projector.project(key,
              oldValue.isPresent() ? oldValue.getValue() : null, postValue,
              targetActivation.isEnabled(), oldPhysicalAssets);
          oldValue = projection.oldAccount;
          postValue = projection.postAccount;
          accountAssetEntries.addAll(projection.reverseAssets);
          forwardEntries.add(new Entry(key, change.getPostValue(), projection.postAccount,
              projection.forwardAssets));
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
      groups.add(new BlockReverseDiff.DbGroup(AccountAssetArchiveProjector.ACCOUNT_ASSET_DB,
          accountAssetEntries));
    }

    BlockReverseDiff reverse = new BlockReverseDiff(input.getMeta(), groups);
    return new PreparedBlockProjection(input, participants, reverse, forwardEntries,
        targetActivation.getPhase());
  }

  private void validateBeforeProjection(BlockChangeView view,
      TargetAssetOptimization activation) {
    if (!view.getMeta().equals(activation.getMeta())) {
      throw new ArchivePersistenceException("Block projection activation identity mismatch");
    }
    List<String> actual = new ArrayList<>();
    for (DatabaseChanges database : view.getDatabases()) {
      actual.add(database.getDbName());
      if (AccountAssetArchiveProjector.ACCOUNT_ASSET_DB.equals(database.getDbName())
          && !database.getChanges().isEmpty()) {
        throw new ArchivePersistenceException(
            "AccountAsset block projection requires one derived physical mutation source");
      }
    }
    Collections.sort(actual);
    if (!actual.equals(participants)) {
      throw new ArchivePersistenceException(
          "Block projection does not cover the exact VERSIONED_STATE set");
    }
  }

  private static boolean sameLogicalValue(OldValue oldValue, PostValue postValue) {
    return oldValue.isPresent() == postValue.isPresent()
        && (!oldValue.isPresent() || Arrays.equals(oldValue.getValue(), postValue.getValue()));
  }

  /** Target-bound proposal-66 state; mismatched identity is rejected before any Store read. */
  public static final class TargetAssetOptimization {
    private final BlockSnapshotMeta meta;
    private final Phase phase;

    private TargetAssetOptimization(BlockSnapshotMeta meta, Phase phase) {
      this.meta = Objects.requireNonNull(meta, "meta");
      this.phase = Objects.requireNonNull(phase, "phase");
    }

    public static TargetAssetOptimization forTarget(BlockSnapshotMeta meta, boolean enabled) {
      return forTarget(meta, enabled ? Phase.P66_ON : Phase.P66_OFF);
    }

    static TargetAssetOptimization forTarget(BlockSnapshotMeta meta, Phase phase) {
      return new TargetAssetOptimization(meta, phase);
    }

    BlockSnapshotMeta getMeta() {
      return meta;
    }

    boolean isEnabled() {
      return phase != Phase.P66_OFF;
    }

    Phase getPhase() {
      return phase;
    }
  }

  /** Meta-bound projection owner which can be sealed or aborted exactly once. */
  public static final class PreparedBlockProjection {
    private final BlockSnapshotMeta meta;
    private final List<String> participants;
    private BlockChangeView view;
    private BlockReverseDiff reverseDiff;
    private List<Entry> forwardEntries;
    private final Phase targetPhase;
    private State state = State.PREPARED;

    private PreparedBlockProjection(BlockChangeView view, List<String> participants,
        BlockReverseDiff reverseDiff, List<Entry> forwardEntries, Phase targetPhase) {
      this.view = Objects.requireNonNull(view, "view");
      this.meta = view.getMeta();
      this.participants = Collections.unmodifiableList(new ArrayList<>(participants));
      this.reverseDiff = Objects.requireNonNull(reverseDiff, "reverseDiff");
      this.forwardEntries = Collections.unmodifiableList(new ArrayList<>(forwardEntries));
      this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
    }

    public synchronized BlockReverseDiff getReverseDiff() {
      if (state == State.ABORTED) {
        throw new ArchivePersistenceException("Prepared block projection was aborted");
      }
      return reverseDiff;
    }

    synchronized BlockSnapshotMeta getMeta() {
      return meta;
    }

    synchronized void requirePreparedOwnership() {
      requirePrepared();
    }

    public synchronized AccountAssetForwardMutationManifest seal(HistoryCommitMarker marker) {
      AccountAssetForwardMutationManifest manifest = previewSeal(marker);
      completeSeal();
      return manifest;
    }

    public synchronized ArchiveBlockForwardPayload sealPayload(HistoryCommitMarker marker) {
      ArchiveBlockForwardPayload payload = previewSealPayload(marker);
      completeSeal();
      return payload;
    }

    synchronized AccountAssetForwardMutationManifest previewSeal(HistoryCommitMarker marker) {
      HistoryCommitMarker target = validateMarker(marker);
      return new AccountAssetForwardMutationManifest(target, targetPhase, forwardEntries);
    }

    synchronized ArchiveBlockForwardPayload previewSealPayload(HistoryCommitMarker marker) {
      HistoryCommitMarker target = validateMarker(marker);
      return new ArchiveBlockForwardPayload(target, view,
          new AccountAssetForwardMutationManifest(target, targetPhase, forwardEntries));
    }

    synchronized HistoryCommitMarker validateMarker(HistoryCommitMarker marker) {
      requirePrepared();
      HistoryCommitMarker target = Objects.requireNonNull(marker, "marker");
      if (!meta.equals(target.getMeta())) {
        throw new ArchivePersistenceException("Prepared block projection target mismatch");
      }
      if (!participants.equals(target.getDatabases())) {
        throw new ArchivePersistenceException(
            "Prepared block projection participant set mismatch");
      }
      return target;
    }

    synchronized void completeSeal() {
      requirePrepared();
      view = null;
      forwardEntries = Collections.emptyList();
      state = State.SEALED;
    }

    synchronized boolean retainsCapturedView() {
      return view != null;
    }

    public synchronized void abort() {
      requirePrepared();
      view = null;
      reverseDiff = null;
      forwardEntries = Collections.emptyList();
      state = State.ABORTED;
    }

    private void requirePrepared() {
      if (state != State.PREPARED) {
        throw new ArchivePersistenceException("Prepared block projection is terminal");
      }
    }

    private enum State {
      PREPARED,
      SEALED,
      ABORTED
    }
  }
}
