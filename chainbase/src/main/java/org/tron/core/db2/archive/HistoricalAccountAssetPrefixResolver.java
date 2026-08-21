package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;

/** Bounded historical AccountAsset address-prefix resolver over one pinned generation. */
public final class HistoricalAccountAssetPrefixResolver {

  private final P66AccountAssetCodec codec = new P66AccountAssetCodec();

  public Result resolve(ArchiveReadSnapshot snapshot, byte[] address, Limits limits)
      throws IOException {
    ArchiveReadSnapshot pinned = Objects.requireNonNull(snapshot, "snapshot");
    Limits budgets = Objects.requireNonNull(limits, "limits");
    byte[] accountAddress = requireAddress(address);
    HistoricalAccountAssetBalanceResolver.requireScopedDatabases();
    pinned.requirePinnedIdentity();

    OldValue propertyValue = pinned.get(
        HistoricalAccountAssetBalanceResolver.PROPERTIES_DATABASE,
        HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey());
    OldValue accountValue = pinned.get(
        HistoricalAccountAssetBalanceResolver.ACCOUNT_DATABASE, accountAddress);
    List<HistoricalRangeOverlay.Entry> directRows = pinned.range(
        HistoricalAccountAssetBalanceResolver.ACCOUNT_ASSET_DATABASE,
        KeyRange.prefix(accountAddress), budgets.overlayLimits());

    pinned.requirePinnedIdentity();
    Phase phase = HistoricalAccountAssetBalanceResolver.decodeTargetPhase(propertyValue);
    if (!accountValue.isPresent()) {
      if (!directRows.isEmpty()) {
        throw new ArchivePersistenceException(
            "Historical AccountAsset prefix has no owning Account");
      }
      return Result.absent(pinned.getTargetBlock(), accountAddress, phase);
    }

    byte[] exactAccount = accountValue.getValue();
    Account account = codec.decodeCanonicalAccount(phase, accountAddress, exactAccount);
    List<Balance> balances = phase == Phase.P66_OFF
        ? resolveEmbedded(accountAddress, account.getAssetV2Map(), directRows, budgets)
        : resolveDirect(accountAddress, directRows, budgets);
    return Result.present(pinned.getTargetBlock(), accountAddress, phase, exactAccount, balances);
  }

  private List<Balance> resolveEmbedded(byte[] address, Map<String, Long> embedded,
      List<HistoricalRangeOverlay.Entry> directRows, Limits limits) {
    if (!directRows.isEmpty()) {
      throw new ArchivePersistenceException("P66-off historical layout contains direct rows");
    }
    List<Map.Entry<String, Long>> sorted = new ArrayList<>(embedded.entrySet());
    sorted.sort(Comparator.comparing(Map.Entry::getKey));
    List<Balance> result = new ArrayList<>();
    long totalBytes = 0L;
    for (Map.Entry<String, Long> entry : sorted) {
      String tokenId = Objects.requireNonNull(entry.getKey(), "embedded tokenId");
      Long balance = Objects.requireNonNull(entry.getValue(), "embedded balance");
      byte[] physicalKey = codec.assetPhysicalKey(address, tokenId);
      totalBytes = limits.reserve(result.size(), physicalKey.length, Long.BYTES, totalBytes);
      result.add(new Balance(tokenId, balance));
    }
    return Collections.unmodifiableList(result);
  }

  private List<Balance> resolveDirect(byte[] address,
      List<HistoricalRangeOverlay.Entry> directRows, Limits limits) {
    List<Balance> result = new ArrayList<>();
    byte[] previous = null;
    long totalBytes = 0L;
    for (HistoricalRangeOverlay.Entry row : directRows) {
      byte[] key = row.getKey();
      byte[] value = row.getValue();
      if (previous != null && BlockReverseDiff.compareUnsigned(previous, key) >= 0) {
        throw new ArchivePersistenceException(
            "Historical AccountAsset prefix rows must be strictly sorted and unique");
      }
      totalBytes = limits.reserve(result.size(), key.length, value.length, totalBytes);
      DecodedAssetRow decoded = codec.decodePresentAssetRow(key, value);
      if (!Arrays.equals(address, decoded.getAccountAddress())) {
        throw new ArchivePersistenceException("Historical AccountAsset prefix escaped address");
      }
      result.add(new Balance(decoded.getTokenId(), decoded.getBalance()));
      previous = key;
    }
    return Collections.unmodifiableList(result);
  }

  private static byte[] requireAddress(byte[] address) {
    if (address == null || address.length != HistoricalAccountBalanceReader.ADDRESS_LENGTH) {
      throw new ArchivePersistenceException("Account address must be exactly 21 bytes");
    }
    return Arrays.copyOf(address, address.length);
  }

  public static final class Limits {
    private final int maxChangedKeys;
    private final int maxCandidateKeys;
    private final int maxEntries;
    private final int maxKeyBytes;
    private final int maxValueBytes;
    private final long maxTotalBytes;

    public Limits(int maxChangedKeys, int maxCandidateKeys, int maxEntries, int maxKeyBytes,
        int maxValueBytes, long maxTotalBytes) {
      if (maxChangedKeys <= 0 || maxCandidateKeys <= 0 || maxEntries <= 0 || maxKeyBytes <= 0
          || maxValueBytes <= 0 || maxTotalBytes <= 0) {
        throw new IllegalArgumentException("AccountAsset prefix limits must be positive");
      }
      this.maxChangedKeys = maxChangedKeys;
      this.maxCandidateKeys = maxCandidateKeys;
      this.maxEntries = maxEntries;
      this.maxKeyBytes = maxKeyBytes;
      this.maxValueBytes = maxValueBytes;
      this.maxTotalBytes = maxTotalBytes;
    }

    private HistoricalRangeOverlay.Limits overlayLimits() {
      return new HistoricalRangeOverlay.Limits(maxChangedKeys, maxCandidateKeys, maxEntries);
    }

    private long reserve(int currentEntries, int keyBytes, int valueBytes, long currentTotal) {
      if (currentEntries >= maxEntries) {
        throw new ArchiveQueryLimitExceededException("AccountAsset entry budget exceeded");
      }
      if (keyBytes > maxKeyBytes) {
        throw new ArchiveQueryLimitExceededException("AccountAsset key-byte budget exceeded");
      }
      if (valueBytes > maxValueBytes) {
        throw new ArchiveQueryLimitExceededException("AccountAsset value-byte budget exceeded");
      }
      final long entryBytes;
      final long updated;
      try {
        entryBytes = Math.addExact((long) keyBytes, valueBytes);
        updated = Math.addExact(currentTotal, entryBytes);
      } catch (ArithmeticException overflow) {
        throw new ArchiveQueryLimitExceededException(
            "AccountAsset total-byte budget overflow");
      }
      if (updated > maxTotalBytes) {
        throw new ArchiveQueryLimitExceededException("AccountAsset total-byte budget exceeded");
      }
      return updated;
    }
  }

  public static final class Balance {
    private final String tokenId;
    private final long balance;

    private Balance(String tokenId, long balance) {
      this.tokenId = tokenId;
      this.balance = balance;
    }

    public String getTokenId() {
      return tokenId;
    }

    public long getBalance() {
      return balance;
    }
  }

  public static final class Result {
    private final long blockNumber;
    private final byte[] address;
    private final Phase phase;
    private final boolean accountPresent;
    private final byte[] accountValue;
    private final List<Balance> balances;

    private Result(long blockNumber, byte[] address, Phase phase, boolean accountPresent,
        byte[] accountValue, List<Balance> balances) {
      this.blockNumber = blockNumber;
      this.address = Arrays.copyOf(address, address.length);
      this.phase = phase;
      this.accountPresent = accountPresent;
      this.accountValue = accountValue == null ? null
          : Arrays.copyOf(accountValue, accountValue.length);
      this.balances = balances;
    }

    private static Result absent(long blockNumber, byte[] address, Phase phase) {
      return new Result(blockNumber, address, phase, false, null, Collections.emptyList());
    }

    private static Result present(long blockNumber, byte[] address, Phase phase,
        byte[] accountValue, List<Balance> balances) {
      return new Result(blockNumber, address, phase, true, accountValue, balances);
    }

    public long getBlockNumber() {
      return blockNumber;
    }

    public byte[] getAddress() {
      return Arrays.copyOf(address, address.length);
    }

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

    public List<Balance> getBalances() {
      return balances;
    }
  }
}
