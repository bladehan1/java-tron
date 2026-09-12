package org.tron.core.db2.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.tron.protos.Protocol.Account;

/** Narrow historical TRX account-balance reader over one pinned archive snapshot. */
public final class HistoricalAccountBalanceReader {

  public static final String ACCOUNT_DATABASE = "account";
  public static final int ADDRESS_LENGTH = 21;

  private HistoricalAccountBalanceReader() {
  }

  public static Result read(ArchivePointSnapshot snapshot, byte[] address) throws IOException {
    Objects.requireNonNull(snapshot, "snapshot");
    if (address == null || address.length != ADDRESS_LENGTH) {
      throw new IllegalArgumentException("TRON account address must be exactly 21 bytes");
    }
    OldValue historical = snapshot.get(ACCOUNT_DATABASE, address);
    return decode(snapshot.getTargetBlock(), address, historical);
  }

  public static Result decode(long blockNumber, byte[] address, OldValue historical) {
    Objects.requireNonNull(historical, "historical");
    if (address == null || address.length != ADDRESS_LENGTH) {
      throw new IllegalArgumentException("TRON account address must be exactly 21 bytes");
    }
    if (!historical.isPresent()) {
      return Result.absent(blockNumber, address);
    }
    Account account;
    try {
      account = Account.parseFrom(historical.getValue());
    } catch (InvalidProtocolBufferException failure) {
      throw new ArchivePersistenceException("Historical account value is not valid protobuf",
          failure);
    }
    if (!Arrays.equals(address, account.getAddress().toByteArray())) {
      throw new ArchivePersistenceException(
          "Historical account value address does not match the physical key");
    }
    return Result.present(blockNumber, address, account.getBalance());
  }

  public static final class Result {
    private final long blockNumber;
    private final byte[] address;
    private final boolean present;
    private final long balance;

    private Result(long blockNumber, byte[] address, boolean present, long balance) {
      this.blockNumber = blockNumber;
      this.address = Arrays.copyOf(address, address.length);
      this.present = present;
      this.balance = balance;
    }

    private static Result absent(long blockNumber, byte[] address) {
      return new Result(blockNumber, address, false, 0);
    }

    private static Result present(long blockNumber, byte[] address, long balance) {
      return new Result(blockNumber, address, true, balance);
    }

    public long getBlockNumber() {
      return blockNumber;
    }

    public byte[] getAddress() {
      return Arrays.copyOf(address, address.length);
    }

    public boolean isPresent() {
      return present;
    }

    public long getBalance() {
      if (!present) {
        throw new IllegalStateException("Historical account is absent");
      }
      return balance;
    }
  }
}
