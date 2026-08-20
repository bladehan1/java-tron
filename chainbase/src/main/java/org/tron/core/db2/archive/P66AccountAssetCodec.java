package org.tron.core.db2.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.protos.Protocol.Account;

/** Versioned standalone codec for the P66-dependent Account and AccountAsset layout. */
public final class P66AccountAssetCodec {

  public static final String FORMAT_ID = "archive-state/p66-account-asset/v1";
  private static final int ADDRESS_LENGTH = 21;
  private static final int BALANCE_LENGTH = Long.BYTES;

  public enum Phase {
    P66_OFF,
    P66_ACTIVATION,
    P66_ON;

    private boolean directAssetsEnabled() {
      return this != P66_OFF;
    }
  }

  /** Validates and converts one raw execution Account into its target archive representation. */
  public byte[] canonicalizeAccount(Phase phase, byte[] physicalAccountKey,
      byte[] rawAccountValue) {
    Objects.requireNonNull(phase, "phase");
    byte[] accountKey = requireAddress(physicalAccountKey);
    Objects.requireNonNull(rawAccountValue, "rawAccountValue");
    Account account = parseAccount(rawAccountValue);
    requireAccountAddress(accountKey, account);
    if (!phase.directAssetsEnabled()) {
      if (account.getAssetOptimized()) {
        throw new ArchivePersistenceException(
            "P66-off Account must not use the optimized asset layout");
      }
      return Arrays.copyOf(rawAccountValue, rawAccountValue.length);
    }
    return account.toBuilder()
        .setAssetOptimized(true)
        .clearAsset()
        .clearAssetV2()
        .build()
        .toByteArray();
  }

  /** Encodes one direct-row post state; zero is represented only as ABSENT. */
  public AssetRow encodeAssetRow(Phase phase, byte[] accountAddress, String tokenId,
      long balance) {
    Objects.requireNonNull(phase, "phase");
    if (!phase.directAssetsEnabled()) {
      throw new ArchivePersistenceException("P66-off state must not contain direct asset rows");
    }
    byte[] address = requireAddress(accountAddress);
    byte[] token = requireTokenId(tokenId);
    byte[] key = ByteBuffer.allocate(address.length + token.length)
        .put(address)
        .put(token)
        .array();
    PostValue value = balance == 0 ? PostValue.absent()
        : PostValue.present(ByteBuffer.allocate(BALANCE_LENGTH).putLong(balance).array());
    return new AssetRow(key, value);
  }

  /** Decodes a PRESENT direct row and rejects the non-canonical stored zero representation. */
  public DecodedAssetRow decodePresentAssetRow(byte[] physicalRawKey, byte[] rawValue) {
    KeyIdentity identity = decodeKey(physicalRawKey);
    if (rawValue == null || rawValue.length != BALANCE_LENGTH) {
      throw new ArchivePersistenceException(
          "AccountAsset value must be exactly eight bytes");
    }
    long balance = ByteBuffer.wrap(rawValue).getLong();
    if (balance == 0) {
      throw new ArchivePersistenceException(
          "AccountAsset zero balance must be encoded as ABSENT");
    }
    return new DecodedAssetRow(identity.address, identity.tokenId, balance);
  }

  /** Validates one canonical account plus its sorted direct-row post mutations. */
  public void requireCanonicalLayout(Phase phase, byte[] physicalAccountKey,
      byte[] canonicalAccountValue, List<AssetRow> directRows) {
    Objects.requireNonNull(phase, "phase");
    byte[] accountKey = requireAddress(physicalAccountKey);
    Account account = parseAccount(canonicalAccountValue);
    requireAccountAddress(accountKey, account);
    List<AssetRow> rows = new ArrayList<>(Objects.requireNonNull(directRows, "directRows"));
    if (rows.contains(null)) {
      throw new ArchivePersistenceException("Canonical AccountAsset rows contain null");
    }
    if (!phase.directAssetsEnabled()) {
      if (account.getAssetOptimized() || !rows.isEmpty()) {
        throw new ArchivePersistenceException("P66-off durable layout is mixed");
      }
      return;
    }
    if (!account.getAssetOptimized() || !account.getAssetMap().isEmpty()
        || !account.getAssetV2Map().isEmpty()) {
      throw new ArchivePersistenceException("P66-on durable Account layout is mixed");
    }
    byte[] previous = null;
    for (AssetRow row : rows) {
      byte[] key = row.getPhysicalRawKey();
      KeyIdentity identity = decodeKey(key);
      if (!Arrays.equals(accountKey, identity.address)
          || previous != null && BlockReverseDiff.compareUnsigned(previous, key) >= 0) {
        throw new ArchivePersistenceException(
            "Canonical AccountAsset rows must be address-bound, unique, and sorted");
      }
      if (row.getPostValue().isPresent()) {
        decodePresentAssetRow(key, row.getPostValue().getValue());
      }
      previous = key;
    }
  }

  private static KeyIdentity decodeKey(byte[] physicalRawKey) {
    if (physicalRawKey == null || physicalRawKey.length <= ADDRESS_LENGTH) {
      throw new ArchivePersistenceException("AccountAsset physical key is too short");
    }
    byte[] address = Arrays.copyOf(physicalRawKey, ADDRESS_LENGTH);
    byte[] token = Arrays.copyOfRange(physicalRawKey, ADDRESS_LENGTH, physicalRawKey.length);
    requireCanonicalTokenBytes(token);
    return new KeyIdentity(address, new String(token, StandardCharsets.US_ASCII));
  }

  private static byte[] requireAddress(byte[] address) {
    if (address == null || address.length != ADDRESS_LENGTH) {
      throw new ArchivePersistenceException("Account address must be exactly 21 bytes");
    }
    return Arrays.copyOf(address, address.length);
  }

  private static byte[] requireTokenId(String tokenId) {
    if (tokenId == null) {
      throw new ArchivePersistenceException("AccountAsset token ID is missing");
    }
    byte[] encoded = tokenId.getBytes(StandardCharsets.US_ASCII);
    if (!tokenId.equals(new String(encoded, StandardCharsets.US_ASCII))) {
      throw new ArchivePersistenceException("AccountAsset token ID must be ASCII decimal");
    }
    requireCanonicalTokenBytes(encoded);
    return encoded;
  }

  private static void requireCanonicalTokenBytes(byte[] token) {
    if (token.length == 0 || token.length > 1 && token[0] == '0') {
      throw new ArchivePersistenceException("AccountAsset token ID is not canonical decimal");
    }
    for (byte value : token) {
      if (value < '0' || value > '9') {
        throw new ArchivePersistenceException("AccountAsset token ID is not canonical decimal");
      }
    }
  }

  private static Account parseAccount(byte[] value) {
    if (value == null) {
      throw new ArchivePersistenceException("Account value is missing");
    }
    try {
      return Account.parseFrom(value);
    } catch (InvalidProtocolBufferException invalid) {
      throw new ArchivePersistenceException("Account value is not valid protobuf", invalid);
    }
  }

  private static void requireAccountAddress(byte[] physicalKey, Account account) {
    if (!Arrays.equals(physicalKey, account.getAddress().toByteArray())) {
      throw new ArchivePersistenceException("Account protobuf address does not match physical key");
    }
  }

  public static final class AssetRow {
    private final byte[] physicalRawKey;
    private final PostValue postValue;

    public AssetRow(byte[] physicalRawKey, PostValue postValue) {
      this.physicalRawKey = Arrays.copyOf(
          Objects.requireNonNull(physicalRawKey, "physicalRawKey"), physicalRawKey.length);
      this.postValue = Objects.requireNonNull(postValue, "postValue");
    }

    public byte[] getPhysicalRawKey() {
      return Arrays.copyOf(physicalRawKey, physicalRawKey.length);
    }

    public PostValue getPostValue() {
      return postValue;
    }
  }

  public static final class DecodedAssetRow {
    private final byte[] accountAddress;
    private final String tokenId;
    private final long balance;

    private DecodedAssetRow(byte[] accountAddress, String tokenId, long balance) {
      this.accountAddress = Arrays.copyOf(accountAddress, accountAddress.length);
      this.tokenId = tokenId;
      this.balance = balance;
    }

    public byte[] getAccountAddress() {
      return Arrays.copyOf(accountAddress, accountAddress.length);
    }

    public String getTokenId() {
      return tokenId;
    }

    public long getBalance() {
      return balance;
    }
  }

  private static final class KeyIdentity {
    private final byte[] address;
    private final String tokenId;

    private KeyIdentity(byte[] address, String tokenId) {
      this.address = address;
      this.tokenId = tokenId;
    }
  }
}
