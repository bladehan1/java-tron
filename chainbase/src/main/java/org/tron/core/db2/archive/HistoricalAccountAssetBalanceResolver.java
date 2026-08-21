package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.AssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;

/** Resolves one historical TRC10 balance from a single pinned archive generation. */
public final class HistoricalAccountAssetBalanceResolver {

  static final String PROPERTIES_DATABASE = "properties";
  static final String ACCOUNT_DATABASE = "account";
  static final String ACCOUNT_ASSET_DATABASE = "account-asset";

  private static final byte[] PROPOSAL_66_PHYSICAL_KEY =
      "ALLOW_ASSET_OPTIMIZATION".getBytes(StandardCharsets.UTF_8);

  private final P66AccountAssetCodec codec = new P66AccountAssetCodec();

  public Result resolve(ArchiveReadSnapshot snapshot, byte[] address, String tokenId)
      throws IOException {
    ArchiveReadSnapshot pinned = Objects.requireNonNull(snapshot, "snapshot");
    requireScopedDatabases();
    pinned.requirePinnedIdentity();

    byte[] directKey = codec.assetPhysicalKey(address, tokenId);
    OldValue propertyValue = pinned.get(PROPERTIES_DATABASE, PROPOSAL_66_PHYSICAL_KEY);
    OldValue accountValue = pinned.get(ACCOUNT_DATABASE, address);
    OldValue directValue = pinned.get(ACCOUNT_ASSET_DATABASE, directKey);

    pinned.requirePinnedIdentity();
    Phase phase = decodeTargetPhase(propertyValue);
    if (!accountValue.isPresent()) {
      if (directValue.isPresent()) {
        throw new ArchivePersistenceException(
            "Historical AccountAsset row has no owning Account");
      }
      return Result.absent(pinned.getTargetBlock(), address, tokenId, phase);
    }

    Account account = codec.decodeCanonicalAccount(phase, address, accountValue.getValue());
    if (phase == Phase.P66_OFF) {
      if (directValue.isPresent()) {
        throw new ArchivePersistenceException("P66-off historical layout contains a direct row");
      }
      Long balance = account.getAssetV2Map().get(tokenId);
      return Result.present(pinned.getTargetBlock(), address, tokenId, phase,
          accountValue.getValue(), balance == null ? 0L : balance);
    }

    long balance = 0L;
    if (directValue.isPresent()) {
      AssetRow row = new AssetRow(directKey, PostValue.present(directValue.getValue()));
      codec.requireCanonicalLayout(phase, address, accountValue.getValue(),
          Collections.singletonList(row));
      DecodedAssetRow decoded = codec.decodePresentAssetRow(directKey, directValue.getValue());
      if (!Arrays.equals(address, decoded.getAccountAddress())
          || !tokenId.equals(decoded.getTokenId())) {
        throw new ArchivePersistenceException("Historical AccountAsset identity mismatch");
      }
      balance = decoded.getBalance();
    }
    return Result.present(pinned.getTargetBlock(), address, tokenId, phase,
        accountValue.getValue(), balance);
  }

  static byte[] proposal66PhysicalKey() {
    return Arrays.copyOf(PROPOSAL_66_PHYSICAL_KEY, PROPOSAL_66_PHYSICAL_KEY.length);
  }

  static Phase decodeTargetPhase(OldValue propertyValue) {
    if (!propertyValue.isPresent()) {
      throw new ArchivePersistenceException("Historical proposal-66 property is absent");
    }
    byte[] value = propertyValue.getValue();
    if (value.length != Long.BYTES) {
      throw new ArchivePersistenceException(
          "Historical proposal-66 property must be exactly eight bytes");
    }
    long enabled = ByteArray.toLong(value);
    if (enabled != 0L && enabled != 1L) {
      throw new ArchivePersistenceException("Historical proposal-66 property must be 0 or 1");
    }
    return enabled == 0L ? Phase.P66_OFF : Phase.P66_ON;
  }

  static void requireScopedDatabases() {
    if (!ArchiveStoreScope.isStateDatabase(PROPERTIES_DATABASE)
        || !ArchiveStoreScope.isStateDatabase(ACCOUNT_DATABASE)
        || !ArchiveStoreScope.isStateDatabase(ACCOUNT_ASSET_DATABASE)) {
      throw new IllegalStateException("Historical AccountAsset resolver Store scope mismatch");
    }
  }

  public static final class Result {
    private final long blockNumber;
    private final byte[] address;
    private final String tokenId;
    private final Phase phase;
    private final boolean accountPresent;
    private final byte[] accountValue;
    private final long balance;

    private Result(long blockNumber, byte[] address, String tokenId, Phase phase,
        boolean accountPresent, byte[] accountValue, long balance) {
      this.blockNumber = blockNumber;
      this.address = Arrays.copyOf(address, address.length);
      this.tokenId = tokenId;
      this.phase = phase;
      this.accountPresent = accountPresent;
      this.accountValue = accountValue == null ? null
          : Arrays.copyOf(accountValue, accountValue.length);
      this.balance = balance;
    }

    private static Result absent(long blockNumber, byte[] address, String tokenId, Phase phase) {
      return new Result(blockNumber, address, tokenId, phase, false, null, 0L);
    }

    private static Result present(long blockNumber, byte[] address, String tokenId, Phase phase,
        byte[] accountValue, long balance) {
      return new Result(blockNumber, address, tokenId, phase, true, accountValue, balance);
    }

    public long getBlockNumber() {
      return blockNumber;
    }

    public byte[] getAddress() {
      return Arrays.copyOf(address, address.length);
    }

    public String getTokenId() {
      return tokenId;
    }

    /** P66_ON also represents the activation target because both use the direct layout. */
    public Phase getPhase() {
      return phase;
    }

    public boolean isAccountPresent() {
      return accountPresent;
    }

    public byte[] getAccountValue() {
      if (!accountPresent) {
        throw new IllegalStateException("Historical account is absent");
      }
      return Arrays.copyOf(accountValue, accountValue.length);
    }

    public long getBalance() {
      if (!accountPresent) {
        throw new IllegalStateException("Historical account is absent");
      }
      return balance;
    }
  }
}
