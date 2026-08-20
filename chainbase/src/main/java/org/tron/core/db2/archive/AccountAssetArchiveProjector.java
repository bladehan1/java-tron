package org.tron.core.db2.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.P66AccountAssetCodec.AssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.protos.Protocol.Account;

/**
 * Projects the non-Chainbase {@code account-asset} mutations which SnapshotRoot otherwise creates
 * implicitly while merging account snapshots.
 */
public final class AccountAssetArchiveProjector {

  public static final String ACCOUNT_DB = "account";
  public static final String ACCOUNT_ASSET_DB = "account-asset";
  private final P66AccountAssetCodec codec = new P66AccountAssetCodec();

  Projection project(byte[] accountKey, byte[] rawOld, BlockChangeView.PostValue rawPost,
      boolean targetAssetOptimizationEnabled,
      Map<WrappedByteArray, byte[]> oldPhysicalAssetsForAddress) {
    Account oldAccount = parse(rawOld);
    Account postAccount = rawPost.isPresent() ? parse(rawPost.getValue()) : null;
    boolean projectPost = postAccount != null
        && (postAccount.getAssetOptimized() || targetAssetOptimizationEnabled);

    boolean requiresOldPhysicalAssets = requiresOldPhysicalAssets(oldAccount, postAccount);
    if (requiresOldPhysicalAssets && oldPhysicalAssetsForAddress == null) {
      throw new ArchivePersistenceException(
          "Optimized Account projection requires explicit old physical assets");
    }
    Map<WrappedByteArray, byte[]> physicalSnapshot = copyPhysicalAssets(accountKey,
        oldPhysicalAssetsForAddress == null ? Collections.emptyMap()
            : oldPhysicalAssetsForAddress);
    if (!physicalSnapshot.isEmpty()
        && (oldAccount == null || !oldAccount.getAssetOptimized())) {
      throw new ArchivePersistenceException(
          "Unoptimized Account must not have old physical AccountAsset rows");
    }

    Map<WrappedByteArray, byte[]> oldAssets = physicalAssets(accountKey, oldAccount,
        oldAccount != null && oldAccount.getAssetOptimized(), physicalSnapshot);
    Map<WrappedByteArray, byte[]> postAssets = physicalAssets(accountKey, postAccount, projectPost,
        physicalSnapshot);

    Set<WrappedByteArray> assetKeys = new TreeSet<>((left, right) ->
        BlockReverseDiff.compareUnsigned(left.getBytes(), right.getBytes()));
    assetKeys.addAll(oldAssets.keySet());
    assetKeys.addAll(postAssets.keySet());
    List<BlockReverseDiff.Entry> reverseAssets = new ArrayList<>();
    List<AssetMutation> forwardAssets = new ArrayList<>();
    for (WrappedByteArray assetKey : assetKeys) {
      byte[] oldValue = oldAssets.get(assetKey);
      byte[] postValue = postAssets.get(assetKey);
      if (!Arrays.equals(oldValue, postValue)) {
        reverseAssets.add(new BlockReverseDiff.Entry(assetKey.getBytes(),
            OldValue.fromNullable(oldValue)));
        forwardAssets.add(new AssetMutation(assetKey.getBytes(), postValue == null
            ? BlockChangeView.PostValue.absent()
            : BlockChangeView.PostValue.present(postValue)));
      }
    }

    OldValue canonicalOld = oldAccount == null ? OldValue.absent()
        : OldValue.present(canonicalAccount(accountKey, oldAccount,
            oldAccount.getAssetOptimized()));
    BlockChangeView.PostValue canonicalPost = postAccount == null
        ? BlockChangeView.PostValue.absent()
        : BlockChangeView.PostValue.present(canonicalAccount(accountKey, postAccount, projectPost));
    if (canonicalPost.isPresent()) {
      List<AssetRow> rows = new ArrayList<>(forwardAssets.size());
      for (AssetMutation mutation : forwardAssets) {
        rows.add(new AssetRow(mutation.getPhysicalRawKey(), mutation.getPostValue()));
      }
      codec.requireCanonicalLayout(phase(postAccount, projectPost),
          accountKey, canonicalPost.getValue(), rows);
    }
    return new Projection(canonicalOld, canonicalPost, reverseAssets, forwardAssets);
  }

  boolean requiresOldPhysicalAssets(byte[] rawOld, BlockChangeView.PostValue rawPost) {
    return requiresOldPhysicalAssets(parse(rawOld),
        rawPost.isPresent() ? parse(rawPost.getValue()) : null);
  }

  private boolean requiresOldPhysicalAssets(Account oldAccount, Account postAccount) {
    return oldAccount != null && oldAccount.getAssetOptimized()
        || postAccount != null && postAccount.getAssetOptimized();
  }

  private Map<WrappedByteArray, byte[]> copyPhysicalAssets(byte[] accountKey,
      Map<WrappedByteArray, byte[]> oldPhysicalAssetsForAddress) {
    Map<WrappedByteArray, byte[]> copy = new HashMap<>();
    oldPhysicalAssetsForAddress.forEach((key, value) -> {
      if (key == null || value == null) {
        throw new ArchivePersistenceException("Old physical AccountAsset input contains null");
      }
      byte[] physicalKey = key.getBytes();
      DecodedAssetRow decoded = codec.decodePresentAssetRow(physicalKey, value);
      if (!Arrays.equals(decoded.getAccountAddress(), accountKey)) {
        throw new ArchivePersistenceException(
            "Old physical AccountAsset input does not belong to changed Account");
      }
      copy.put(WrappedByteArray.copyOf(physicalKey), Arrays.copyOf(value, value.length));
    });
    return copy;
  }

  private Map<WrappedByteArray, byte[]> physicalAssets(byte[] accountKey, Account account,
      boolean projected, Map<WrappedByteArray, byte[]> physicalSnapshot) {
    Map<WrappedByteArray, byte[]> result = new HashMap<>();
    if (account == null || !projected) {
      return result;
    }
    if (account.getAssetOptimized()) {
      physicalSnapshot.forEach((key, value) -> result.put(
          WrappedByteArray.copyOf(key.getBytes()), Arrays.copyOf(value, value.length)));
    }
    account.getAssetV2Map().forEach((token, balance) -> {
      AssetRow encoded = codec.encodeAssetRow(account.getAssetOptimized()
              ? Phase.P66_ON : Phase.P66_ACTIVATION,
          accountKey, token, balance);
      WrappedByteArray key = WrappedByteArray.copyOf(encoded.getPhysicalRawKey());
      if (!encoded.getPostValue().isPresent()) {
        result.remove(key);
      } else {
        result.put(key, encoded.getPostValue().getValue());
      }
    });
    return result;
  }

  private byte[] canonicalAccount(byte[] accountKey, Account account, boolean projected) {
    return codec.canonicalizeAccount(phase(account, projected), accountKey,
        account.toByteArray());
  }

  private Phase phase(Account account, boolean projected) {
    if (!projected) {
      return Phase.P66_OFF;
    }
    return account.getAssetOptimized() ? Phase.P66_ON : Phase.P66_ACTIVATION;
  }

  private Account parse(byte[] value) {
    if (value == null) {
      return null;
    }
    try {
      return Account.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new ArchivePersistenceException(
          "Invalid account value while projecting archive state", e);
    }
  }

  static final class Projection {
    final OldValue oldAccount;
    final BlockChangeView.PostValue postAccount;
    final List<BlockReverseDiff.Entry> reverseAssets;
    final List<AssetMutation> forwardAssets;

    private Projection(OldValue oldAccount, BlockChangeView.PostValue postAccount,
        List<BlockReverseDiff.Entry> reverseAssets, List<AssetMutation> forwardAssets) {
      this.oldAccount = oldAccount;
      this.postAccount = postAccount;
      this.reverseAssets = Collections.unmodifiableList(new ArrayList<>(reverseAssets));
      this.forwardAssets = Collections.unmodifiableList(new ArrayList<>(forwardAssets));
    }
  }
}
