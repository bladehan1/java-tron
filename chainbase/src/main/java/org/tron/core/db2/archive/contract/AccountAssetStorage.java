package org.tron.core.db2.archive.contract;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Shared physical Store identities used by historical readers and block capture. */
public final class AccountAssetStorage {

  public static final String PROPERTIES_DATABASE = "properties";
  public static final String ACCOUNT_DATABASE = "account";
  public static final String ACCOUNT_ASSET_DATABASE = "account-asset";
  private static final byte[] PROPOSAL_66_PHYSICAL_KEY =
      "ALLOW_ASSET_OPTIMIZATION".getBytes(StandardCharsets.UTF_8);

  private AccountAssetStorage() {
  }

  public static byte[] proposal66PhysicalKey() {
    return Arrays.copyOf(PROPOSAL_66_PHYSICAL_KEY, PROPOSAL_66_PHYSICAL_KEY.length);
  }
}
