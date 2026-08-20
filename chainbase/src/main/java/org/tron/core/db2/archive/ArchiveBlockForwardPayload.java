package org.tron.core.db2.archive;

import java.util.Objects;

/** Immutable handoff of one committed target's exact view and AccountAsset projection. */
public final class ArchiveBlockForwardPayload {

  private final HistoryCommitMarker marker;
  private final BlockChangeView view;
  private final AccountAssetForwardMutationManifest accountAssetManifest;

  ArchiveBlockForwardPayload(HistoryCommitMarker marker, BlockChangeView view,
      AccountAssetForwardMutationManifest accountAssetManifest) {
    this.marker = Objects.requireNonNull(marker, "marker");
    this.view = Objects.requireNonNull(view, "view");
    this.accountAssetManifest = Objects.requireNonNull(accountAssetManifest,
        "accountAssetManifest");
    if (!marker.getMeta().equals(view.getMeta())) {
      throw new ArchivePersistenceException("Forward payload view target mismatch");
    }
  }

  public BlockSnapshotMeta getMeta() {
    return marker.getMeta();
  }

  public HistoryCommitMarker getMarker() {
    return marker;
  }

  public BlockChangeView getView() {
    return view;
  }

  public AccountAssetForwardMutationManifest getAccountAssetManifest() {
    return accountAssetManifest;
  }
}
