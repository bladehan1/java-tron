package org.tron.core.capsule.utils;

import com.google.common.primitives.Bytes;
import java.util.HashMap;
import java.util.Map;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.HistoricalExecutionScope;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.Protocol.Account;

public class AssetUtil {

  /**
   * The static stores are the process-wide latest state; a historical execution must resolve TRC10
   * balances through its session instead (chapter 05 section 3.11 point 4). Inside a
   * historical scope every entry point fails closed as a marked CAPABILITY failure.
   */
  private static void trapHistorical(String entryPoint) {
    if (HistoricalExecutionScope.isActive()) {
      throw HistoricalExecutionScope.trap(entryPoint);
    }
  }

  private static AccountAssetStore accountAssetStore;

  private static DynamicPropertiesStore dynamicPropertiesStore;

  public static boolean hasAssetV2(Account account, byte[] key) {
    trapHistorical("AssetUtil.hasAssetV2");
    if (!account.getAssetV2Map().isEmpty()
        && account.getAssetV2Map().containsKey(ByteArray.toStr(key))) {
      return true;
    }
    if (!isAllowAssetOptimization()) {
      return false;
    }
    byte[] dbKey = Bytes.concat(account.getAddress().toByteArray(), key);
    return accountAssetStore.get(dbKey) != null;
  }

  public static Account importAsset(Account account, byte[] key) {
    trapHistorical("AssetUtil.importAsset");
    if (!isAllowAssetOptimization()) {
      return account;
    }

    String sKey = ByteArray.toStr(key);
    if (account.getAssetV2Map().containsKey(sKey)) {
      return account;
    }

    long balance = accountAssetStore.getBalance(account, key);
    Map<String, Long> map = new HashMap<>();
    map.putAll(account.getAssetV2Map());
    map.put(sKey, balance);
    return account.toBuilder().clearAssetV2().putAllAssetV2(map).build();
  }

  public static Account importAllAsset(Account account) {
    trapHistorical("AssetUtil.importAllAsset");
    if (!isAllowAssetOptimization()) {
      return account;
    }
    Map<String, Long> map = accountAssetStore.getAllAssets(account);
    return account.toBuilder().clearAssetV2().putAllAssetV2(map).build();
  }

  public static void setAccountAssetStore(
          AccountAssetStore accountAssetStore) {
    trapHistorical("AssetUtil.setAccountAssetStore");
    AssetUtil.accountAssetStore = accountAssetStore;
  }

  public static void setDynamicPropertiesStore(DynamicPropertiesStore dynamicPropertiesStore) {
    trapHistorical("AssetUtil.setDynamicPropertiesStore");
    AssetUtil.dynamicPropertiesStore = dynamicPropertiesStore;
  }

  public static boolean isAllowAssetOptimization() {
    trapHistorical("AssetUtil.isAllowAssetOptimization");
    return dynamicPropertiesStore.supportAllowAssetOptimization();
  }

}
