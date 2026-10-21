package org.tron.core.db2.core;

import lombok.extern.slf4j.Slf4j;
import org.tron.core.db.RevokingDatabase;
import org.tron.core.store.AccountAssetStore;

/**
 * Startup install decision for the P66 AccountAsset lane (design 07 §3.9.3, D3, 07-B, 07-C).
 *
 * <pre>
 *   form ON                                    -> INSTALLED (recovering until the caller ends it)
 *   form OFF, legacy redo has account-asset    -> RECOVERY_ONLY: replay with synchronous merges,
 *                                                 retire all legacy redo, uninstall the lane
 *   form OFF otherwise                         -> NOT_INSTALLED: upstream path, untouched
 * </pre>
 *
 * <p>RECOVERY_ONLY only exists for data directories written by PR head {@code b76ddef2e4}
 * without Common, which registered the lane unconditionally and therefore wrote account-asset
 * rows into the legacy checkpoint. Upstream {@code recover()} skips that unknown database while
 * the checkpoint's Accounts are already stripped of their asset maps, so ignoring the rows would
 * lose TRC10 balances (review X1-01).
 */
@Slf4j(topic = "DB")
public final class P66LaneStartup {

  public static final String ACCOUNT_ASSET = "account-asset";

  public enum Mode {
    NOT_INSTALLED,
    INSTALLED,
    RECOVERY_ONLY
  }

  private P66LaneStartup() {
  }

  /** Runs after {@code revokingStore.disable()} and before {@code revokingStore.check()}. */
  public static Mode beforeLegacyCheck(RevokingDatabase revoking, AccountAssetStore assets,
      boolean archiveFeaturesEnabled) {
    if (archiveFeaturesEnabled) {
      if (!(revoking instanceof SnapshotManager)) {
        throw new IllegalStateException("AccountAsset Snapshot requires SnapshotManager");
      }
      // The lane survives Manager re-creation inside the same context lifecycle.
      if (!assets.isSnapshotsAttached()) {
        assets.enableSnapshots((SnapshotManager) revoking, true);
      }
      return Mode.INSTALLED;
    }
    if (assets.isSnapshotsAttached()) {
      throw new IllegalStateException(
          "AccountAsset Snapshot lane is attached while every archive feature is off");
    }
    if (!(revoking instanceof SnapshotManager)) {
      return Mode.NOT_INSTALLED;
    }
    SnapshotManager manager = (SnapshotManager) revoking;
    if (!manager.legacyCheckpointContains(ACCOUNT_ASSET)) {
      return Mode.NOT_INSTALLED;
    }
    logger.warn("Legacy checkpoint contains {} redo written by a lane-enabled build; replaying it "
        + "once with the recovery-only P66 lane before returning to the upstream path",
        ACCOUNT_ASSET);
    assets.enableSnapshots(manager, true);
    manager.setSynchronousLegacyRecovery(true);
    return Mode.RECOVERY_ONLY;
  }

  /**
   * Runs right after {@code revokingStore.check()}. Common finishes its recovering phase only
   * after Common redo (02 §3.10), so that case is left to the Common startup path.
   */
  public static void afterLegacyCheck(Mode mode, RevokingDatabase revoking,
      AccountAssetStore assets, boolean commonCheckpointEnabled) {
    switch (mode) {
      case INSTALLED:
        if (!commonCheckpointEnabled) {
          assets.finishSnapshotRecovery((SnapshotManager) revoking);
        }
        return;
      case RECOVERY_ONLY:
        SnapshotManager manager = (SnapshotManager) revoking;
        manager.setSynchronousLegacyRecovery(false);
        manager.retireLegacyRedo();
        assets.detachSnapshots(manager);
        logger.warn("Recovery-only P66 lane finished: legacy redo retired, lane uninstalled. A "
            + "directory that ran a lane-enabled build must still pass the 07 §3.9.8 repair "
            + "audit before it is treated as upstream-equivalent");
        return;
      default:
        return;
    }
  }
}
