package org.tron.core.db2.archive;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.store.AccountAssetStore;
import org.tron.protos.Protocol.Account;

/**
 * Projects the non-Chainbase {@code account-asset} mutations which SnapshotRoot otherwise creates
 * implicitly while merging account snapshots.
 */
public final class AccountAssetArchiveProjector {

  public static final String ACCOUNT_DB = "account";
  public static final String ACCOUNT_ASSET_DB = "account-asset";

  private final AccountAssetStore assetStore;
  private final BooleanSupplier optimizationEnabled;

  public AccountAssetArchiveProjector(AccountAssetStore assetStore,
      BooleanSupplier optimizationEnabled) {
    this.assetStore = assetStore;
    this.optimizationEnabled = optimizationEnabled;
  }

  Projection project(byte[] accountKey, byte[] rawOld, BlockChangeView.PostValue rawPost) {
    Account oldAccount = parse(rawOld);
    Account postAccount = rawPost.isPresent() ? parse(rawPost.getValue()) : null;
    boolean projectPost = postAccount != null
        && (postAccount.getAssetOptimized() || optimizationEnabled.getAsBoolean());

    Map<WrappedByteArray, byte[]> oldAssets = physicalAssets(accountKey, oldAccount,
        oldAccount != null && oldAccount.getAssetOptimized());
    Map<WrappedByteArray, byte[]> postAssets = physicalAssets(accountKey, postAccount, projectPost);

    Set<WrappedByteArray> assetKeys = new HashSet<>(oldAssets.keySet());
    assetKeys.addAll(postAssets.keySet());
    List<BlockReverseDiff.Entry> reverseAssets = new ArrayList<>();
    for (WrappedByteArray assetKey : assetKeys) {
      byte[] oldValue = oldAssets.get(assetKey);
      byte[] postValue = postAssets.get(assetKey);
      if (!Arrays.equals(oldValue, postValue)) {
        reverseAssets.add(new BlockReverseDiff.Entry(assetKey.getBytes(),
            OldValue.fromNullable(oldValue)));
      }
    }

    OldValue canonicalOld = oldAccount == null ? OldValue.absent()
        : OldValue.present(canonicalAccount(oldAccount, oldAccount.getAssetOptimized()));
    BlockChangeView.PostValue canonicalPost = postAccount == null
        ? BlockChangeView.PostValue.absent()
        : BlockChangeView.PostValue.present(canonicalAccount(postAccount, projectPost));
    return new Projection(canonicalOld, canonicalPost, reverseAssets);
  }

  private Map<WrappedByteArray, byte[]> physicalAssets(byte[] accountKey, Account account,
      boolean projected) {
    Map<WrappedByteArray, byte[]> result = new HashMap<>();
    if (account == null || !projected) {
      return result;
    }
    if (account.getAssetOptimized()) {
      assetStore.prefixQuery(accountKey).forEach((key, value) -> result.put(
          WrappedByteArray.copyOf(key.getBytes()), Arrays.copyOf(value, value.length)));
    }
    account.getAssetV2Map().forEach((token, balance) -> {
      WrappedByteArray key = WrappedByteArray.copyOf(Bytes.concat(accountKey,
          token.getBytes(StandardCharsets.UTF_8)));
      if (balance == 0) {
        result.remove(key);
      } else {
        result.put(key, Longs.toByteArray(balance));
      }
    });
    return result;
  }

  private byte[] canonicalAccount(Account account, boolean projected) {
    if (!projected) {
      return account.toByteArray();
    }
    return account.toBuilder()
        .setAssetOptimized(true)
        .clearAsset()
        .clearAssetV2()
        .build()
        .toByteArray();
  }

  private Account parse(byte[] value) {
    if (value == null) {
      return null;
    }
    try {
      return Account.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Invalid account value while projecting archive state", e);
    }
  }

  static final class Projection {
    final OldValue oldAccount;
    final BlockChangeView.PostValue postAccount;
    final List<BlockReverseDiff.Entry> reverseAssets;

    private Projection(OldValue oldAccount, BlockChangeView.PostValue postAccount,
        List<BlockReverseDiff.Entry> reverseAssets) {
      this.oldAccount = oldAccount;
      this.postAccount = postAccount;
      this.reverseAssets = reverseAssets;
    }
  }
}
