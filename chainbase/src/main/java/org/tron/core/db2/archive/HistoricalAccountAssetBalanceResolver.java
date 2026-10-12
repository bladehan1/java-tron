package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;

/** Resolves one historical TRC10 balance through a request-owned archive accessor. */
@Slf4j(topic = "DB")
public final class HistoricalAccountAssetBalanceResolver {

  private static final AtomicLong ORPHAN_DIRECT_ROWS = new AtomicLong();

  static final String PROPERTIES_DATABASE = "properties";
  static final String ACCOUNT_DATABASE = "account";
  static final String ACCOUNT_ASSET_DATABASE = "account-asset";

  private static final byte[] PROPOSAL_66_PHYSICAL_KEY =
      "ALLOW_ASSET_OPTIMIZATION".getBytes(StandardCharsets.UTF_8);

  private final P66AccountAssetCodec codec = new P66AccountAssetCodec();

  public Result resolve(ArchivePointSnapshot snapshot, byte[] address, String tokenId)
      throws IOException {
    ArchivePointSnapshot pinned = Objects.requireNonNull(snapshot, "snapshot");
    requireScopedDatabases();
    pinned.requirePinnedIdentity();

    byte[] directKey = codec.assetPhysicalKey(address, tokenId);
    OldValue propertyValue = pinned.get(PROPERTIES_DATABASE, PROPOSAL_66_PHYSICAL_KEY);
    OldValue accountValue = pinned.get(ACCOUNT_DATABASE, address);
    OldValue directValue = pinned.get(ACCOUNT_ASSET_DATABASE, directKey);

    pinned.requirePinnedIdentity();
    return resolveValues(pinned.getTargetBlock(), address, tokenId, directKey, propertyValue,
        accountValue, directValue);
  }

  /**
   * Upstream {@code AssetUtil.importAsset} semantics over three already-read values at S(T)
   * (chapter 05 section 3.11 point 1):
   * <ul>
   *   <li>Account ABSENT: balance 0; the direct row must also be ABSENT.</li>
   *   <li>P66_OFF: {@code Account.assetV2[tokenId]} or 0; no direct row; the Account must not be
   *   optimized.</li>
   *   <li>P66_ON: the inline {@code assetV2} entry when present; otherwise the direct row for an
   *   optimized Account (ABSENT = 0); otherwise (never re-written, unoptimized Account) 0. A direct
   *   row under an unoptimized Account does not change the answer but is counted and logged.</li>
   * </ul>
   * The direct row itself is always validated (8 bytes, non-zero, address and token bound).
   */
  public Result resolveValues(long targetBlock, byte[] address, String tokenId,
      byte[] directKey, OldValue propertyValue, OldValue accountValue, OldValue directValue) {
    Phase phase = decodeTargetPhase(propertyValue);
    if (!accountValue.isPresent()) {
      if (directValue.isPresent()) {
        throw new ArchivePersistenceException(
            "Historical AccountAsset row has no owning Account");
      }
      return Result.absent(targetBlock, address, tokenId, phase);
    }

    if (phase == Phase.P66_OFF) {
      Account account = codec.decodeCanonicalAccount(phase, address, accountValue.getValue());
      if (directValue.isPresent()) {
        throw new ArchivePersistenceException("P66-off historical layout contains a direct row");
      }
      Long balance = account.getAssetV2Map().get(tokenId);
      return Result.present(targetBlock, address, tokenId, phase,
          accountValue.getValue(), balance == null ? 0L : balance);
    }

    Account account = codec.decodeAccount(address, accountValue.getValue());
    Long directBalance = null;
    if (directValue.isPresent()) {
      DecodedAssetRow decoded = codec.decodePresentAssetRow(directKey, directValue.getValue());
      if (!Arrays.equals(address, decoded.getAccountAddress())
          || !tokenId.equals(decoded.getTokenId())) {
        throw new ArchivePersistenceException("Historical AccountAsset identity mismatch");
      }
      directBalance = decoded.getBalance();
    }
    Long inline = account.getAssetV2Map().get(tokenId);
    long balance;
    if (inline != null) {
      balance = inline;
    } else if (account.getAssetOptimized()) {
      balance = directBalance == null ? 0L : directBalance;
    } else {
      balance = 0L;
    }
    if (!account.getAssetOptimized() && directBalance != null) {
      recordOrphanDirectRow(targetBlock, tokenId);
    }
    return Result.present(targetBlock, address, tokenId, phase, accountValue.getValue(), balance);
  }

  /** Direct rows found under unoptimized Accounts (expected 0; chapter 07 orphan-row audit). */
  public static long getOrphanDirectRowCount() {
    return ORPHAN_DIRECT_ROWS.get();
  }

  private static void recordOrphanDirectRow(long targetBlock, String tokenId) {
    long count = ORPHAN_DIRECT_ROWS.incrementAndGet();
    if (Long.bitCount(count) == 1) {
      logger.warn("Historical AccountAsset direct row under an unoptimized Account at block {} "
          + "(token {}); answered with upstream importAsset semantics; total={}",
          targetBlock, tokenId, count);
    }
  }

  static byte[] proposal66PhysicalKey() {
    return Arrays.copyOf(PROPOSAL_66_PHYSICAL_KEY, PROPOSAL_66_PHYSICAL_KEY.length);
  }

  public static Phase decodeTargetPhase(OldValue propertyValue) {
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
