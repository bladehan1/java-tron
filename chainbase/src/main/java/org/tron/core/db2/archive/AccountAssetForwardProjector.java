package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockChangeView.PostValue;

/** Explicit no-scan contract for canonical account and physical account-asset post mutations. */
@FunctionalInterface
public interface AccountAssetForwardProjector {

  /** Opens one target-bound projection pass and declares its exact changed-account keys. */
  default void begin(HistoryCommitMarker target, List<byte[]> changedAccountPhysicalKeys) {
  }

  Projection project(byte[] accountPhysicalKey, PostValue rawAccountPostValue);

  /** Finalizes one projection pass after every declared account has been consumed. */
  default void complete() {
  }

  /** One canonical account post value plus exact physical account-asset post mutations. */
  final class Projection {
    private final PostValue accountPostValue;
    private final List<AssetMutation> assetMutations;

    public Projection(PostValue accountPostValue, List<AssetMutation> assetMutations) {
      this.accountPostValue = Objects.requireNonNull(accountPostValue, "accountPostValue");
      List<AssetMutation> copy = new ArrayList<>(
          Objects.requireNonNull(assetMutations, "assetMutations"));
      if (copy.contains(null)) {
        throw new IllegalArgumentException("AccountAsset projection contains null mutation");
      }
      this.assetMutations = Collections.unmodifiableList(copy);
    }

    PostValue getAccountPostValue() {
      return accountPostValue;
    }

    List<AssetMutation> getAssetMutations() {
      return assetMutations;
    }
  }

  /** Exact physical account-asset key and its present/absent post state. */
  final class AssetMutation {
    private final byte[] physicalRawKey;
    private final PostValue postValue;

    public AssetMutation(byte[] physicalRawKey, PostValue postValue) {
      this.physicalRawKey = Arrays.copyOf(
          Objects.requireNonNull(physicalRawKey, "physicalRawKey"), physicalRawKey.length);
      this.postValue = Objects.requireNonNull(postValue, "postValue");
    }

    byte[] getPhysicalRawKey() {
      return Arrays.copyOf(physicalRawKey, physicalRawKey.length);
    }

    PostValue getPostValue() {
      return postValue;
    }
  }
}
