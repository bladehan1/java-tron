package org.tron.core.db2.stateroot;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.protos.Protocol.Account;
import org.tron.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract.ABI;

/** Canonical key/value boundary for the current path-state root. */
public final class PathStateCanonicalizer {

  public static final String PHYSICAL_RAW = "physical-raw/v1";
  public static final String P66_ACCOUNT = "p66-account/v1";
  public static final String P66_ACCOUNT_ASSET = "p66-account-asset/v1";
  public static final String STORAGE_ROW = "storage-physical-row/v1";
  public static final String ABI_PROTOBUF = "abi-protobuf/v1";
  public static final String LEGACY_ASSET_PROTOBUF = "asset-name-protobuf/v1";
  public static final String ASSET_V2_PROTOBUF = "asset-id-protobuf/v1";

  private static final int ADDRESS_LENGTH = 21;
  private static final int STORAGE_KEY_LENGTH = 32;
  private static final int STORAGE_VALUE_LENGTH = 32;
  private static final int BALANCE_LENGTH = Long.BYTES;
  private static final int STORE_FORMAT_VERSION = 1;

  private final PathStateParticipantDescriptor descriptor;
  private final Map<String, StoreFormat> formats;

  public PathStateCanonicalizer() {
    descriptor = PathStateParticipantDescriptor.current();
    LinkedHashMap<String, StoreFormat> configured = new LinkedHashMap<>();
    for (PathStateParticipantDescriptor.StoreIdentity identity : descriptor.getStores()) {
      configured.put(identity.getDbName(), new StoreFormat(identity.getDbName(), PHYSICAL_RAW));
    }
    configure(configured, "account", P66_ACCOUNT);
    configure(configured, "account-asset", P66_ACCOUNT_ASSET);
    configure(configured, "storage-row", STORAGE_ROW);
    configure(configured, "abi", ABI_PROTOBUF);
    configure(configured, "asset-issue", LEGACY_ASSET_PROTOBUF);
    configure(configured, "asset-issue-v2", ASSET_V2_PROTOBUF);
    formats = Collections.unmodifiableMap(configured);
  }

  public StoreFormat requireFormat(String dbName) {
    StoreFormat format = formats.get(Objects.requireNonNull(dbName, "dbName"));
    if (format == null) {
      throw new IllegalArgumentException("unknown path-state database: " + dbName);
    }
    return format;
  }

  /** Creates the approved exact-27 scope after every Store has an explicit format identity. */
  public PathStateParticipantScope participantScope() {
    List<PathStateParticipant> participants = new ArrayList<>();
    for (PathStateParticipantDescriptor.StoreIdentity identity : descriptor.getStores()) {
      StoreFormat format = requireFormat(identity.getDbName());
      participants.add(new PathStateParticipant(identity.getStoreId(), identity.getDbName(),
          format.getStoreFormatVersion()));
    }
    return new PathStateParticipantScope(participants);
  }

  /** Canonicalizes one physical PRESENT value for the target P66 phase. */
  public PathStateMutation put(P66Phase phase, String dbName, byte[] physicalKey,
      byte[] rawValue) {
    P66Phase target = Objects.requireNonNull(phase, "phase");
    String name = requireFormat(dbName).getDbName();
    byte[] key = canonicalKey(target, name, physicalKey);
    byte[] value = canonicalValue(target, name, key,
        Objects.requireNonNull(rawValue, "rawValue"));
    return PathStateMutation.put(name, key, value);
  }

  /** Canonicalizes one physical delete; no PRESENT value is synthesized. */
  public PathStateMutation delete(P66Phase phase, String dbName, byte[] physicalKey) {
    P66Phase target = Objects.requireNonNull(phase, "phase");
    String name = requireFormat(dbName).getDbName();
    return PathStateMutation.delete(name, canonicalKey(target, name, physicalKey));
  }

  /** Encodes a P66 direct balance; zero is represented only by a delete mutation. */
  public PathStateMutation accountAsset(P66Phase phase, byte[] address, String tokenId,
      long balance) {
    P66Phase target = Objects.requireNonNull(phase, "phase");
    if (!target.directAssetsEnabled()) {
      throw new IllegalArgumentException("P66-off state must not contain account-asset rows");
    }
    byte[] key = accountAssetKey(address, tokenId);
    return balance == 0 ? PathStateMutation.delete("account-asset", key)
        : PathStateMutation.put("account-asset", key,
            ByteBuffer.allocate(BALANCE_LENGTH).putLong(balance).array());
  }

  /** Requires the physical Account representation admitted for a rebuild target phase. */
  public void requireSnapshotAccountLayout(P66Phase phase, byte[] physicalKey,
      byte[] rawValue) {
    P66Phase target = Objects.requireNonNull(phase, "phase");
    byte[] key = copyNonEmpty(physicalKey, "physicalKey");
    requireLength(key, ADDRESS_LENGTH, "account key");
    Account account = parseAccount(key, rawValue);
    if (target.directAssetsEnabled()) {
      if (!account.getAssetOptimized() || !account.getAssetMap().isEmpty()
          || !account.getAssetV2Map().isEmpty()) {
        throw new IllegalArgumentException("P66-on snapshot Account layout is mixed");
      }
    } else if (account.getAssetOptimized()) {
      throw new IllegalArgumentException("P66-off snapshot Account layout is mixed");
    }
  }

  /** Extracts and validates the owning Account address from one direct physical key. */
  public byte[] accountAddressFromAssetKey(P66Phase phase, byte[] physicalKey) {
    P66Phase target = Objects.requireNonNull(phase, "phase");
    if (!target.directAssetsEnabled()) {
      throw new IllegalArgumentException("P66-off state must not contain account-asset rows");
    }
    byte[] key = copyNonEmpty(physicalKey, "physicalKey");
    decodeAccountAssetKey(key);
    return Arrays.copyOf(key, ADDRESS_LENGTH);
  }

  private static void configure(Map<String, StoreFormat> formats, String dbName, String codecId) {
    if (formats.replace(dbName, new StoreFormat(dbName, codecId)) == null) {
      throw new IllegalStateException("missing path-state format participant: " + dbName);
    }
  }

  private static byte[] canonicalKey(P66Phase phase, String dbName, byte[] physicalKey) {
    byte[] key = copyNonEmpty(physicalKey, "physicalKey");
    switch (dbName) {
      case "account":
      case "abi":
        requireLength(key, ADDRESS_LENGTH, dbName + " key");
        break;
      case "account-asset":
        if (!phase.directAssetsEnabled()) {
          throw new IllegalArgumentException("P66-off state must not contain account-asset rows");
        }
        decodeAccountAssetKey(key);
        break;
      case "asset-issue-v2":
        requireCanonicalDecimal(key, "asset-issue-v2 key");
        break;
      case "storage-row":
        requireLength(key, STORAGE_KEY_LENGTH, "storage-row key");
        break;
      default:
        break;
    }
    return key;
  }

  private static byte[] canonicalValue(P66Phase phase, String dbName, byte[] physicalKey,
      byte[] rawValue) {
    byte[] value = Arrays.copyOf(rawValue, rawValue.length);
    switch (dbName) {
      case "account":
        return canonicalAccount(phase, physicalKey, value);
      case "account-asset":
        requireLength(value, BALANCE_LENGTH, "account-asset value");
        if (ByteBuffer.wrap(value).getLong() == 0) {
          throw new IllegalArgumentException("account-asset zero balance must be ABSENT");
        }
        return value;
      case "storage-row":
        requireLength(value, STORAGE_VALUE_LENGTH, "storage-row value");
        if (isZero(value)) {
          throw new IllegalArgumentException("storage-row zero word must be ABSENT");
        }
        return value;
      case "abi":
        parseAbi(value);
        return value;
      case "asset-issue":
        requireAssetKey(physicalKey, value, false);
        return value;
      case "asset-issue-v2":
        requireAssetKey(physicalKey, value, true);
        return value;
      default:
        return value;
    }
  }

  private static byte[] canonicalAccount(P66Phase phase, byte[] physicalKey, byte[] rawValue) {
    Account account = parseAccount(physicalKey, rawValue);
    if (!phase.directAssetsEnabled()) {
      if (account.getAssetOptimized()) {
        throw new IllegalArgumentException("P66-off Account must not use direct asset layout");
      }
      return rawValue;
    }
    return account.toBuilder()
        .setAssetOptimized(true)
        .clearAsset()
        .clearAssetV2()
        .build()
        .toByteArray();
  }

  private static Account parseAccount(byte[] physicalKey, byte[] rawValue) {
    Account account;
    try {
      account = Account.parseFrom(Objects.requireNonNull(rawValue, "rawValue"));
    } catch (InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("account value is not valid protobuf", invalid);
    }
    if (!Arrays.equals(physicalKey, account.getAddress().toByteArray())) {
      throw new IllegalArgumentException("account protobuf address does not match physical key");
    }
    return account;
  }

  private static void parseAbi(byte[] value) {
    try {
      ABI.parseFrom(value);
    } catch (InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("abi value is not valid protobuf", invalid);
    }
  }

  private static void requireAssetKey(byte[] physicalKey, byte[] value, boolean v2) {
    AssetIssueContract asset;
    try {
      asset = AssetIssueContract.parseFrom(value);
    } catch (InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("asset value is not valid protobuf", invalid);
    }
    byte[] expected = v2 ? asset.getId().getBytes(StandardCharsets.US_ASCII)
        : asset.getName().toByteArray();
    if (v2) {
      requireCanonicalDecimal(expected, "asset protobuf ID");
    }
    if (!Arrays.equals(physicalKey, expected)) {
      throw new IllegalArgumentException("asset protobuf identity does not match physical key");
    }
  }

  private static byte[] accountAssetKey(byte[] address, String tokenId) {
    byte[] canonicalAddress = Arrays.copyOf(Objects.requireNonNull(address, "address"),
        address.length);
    requireLength(canonicalAddress, ADDRESS_LENGTH, "account-asset address");
    byte[] token = Objects.requireNonNull(tokenId, "tokenId").getBytes(StandardCharsets.US_ASCII);
    if (!tokenId.equals(new String(token, StandardCharsets.US_ASCII))) {
      throw new IllegalArgumentException("account-asset token ID must be ASCII decimal");
    }
    requireCanonicalDecimal(token, "account-asset token ID");
    return ByteBuffer.allocate(canonicalAddress.length + token.length)
        .put(canonicalAddress)
        .put(token)
        .array();
  }

  private static void decodeAccountAssetKey(byte[] key) {
    if (key.length <= ADDRESS_LENGTH) {
      throw new IllegalArgumentException("account-asset key is too short");
    }
    requireCanonicalDecimal(Arrays.copyOfRange(key, ADDRESS_LENGTH, key.length),
        "account-asset token ID");
  }

  private static void requireCanonicalDecimal(byte[] value, String name) {
    if (value.length == 0 || value.length > 1 && value[0] == '0') {
      throw new IllegalArgumentException(name + " is not canonical decimal");
    }
    for (byte digit : value) {
      if (digit < '0' || digit > '9') {
        throw new IllegalArgumentException(name + " is not canonical decimal");
      }
    }
  }

  private static byte[] copyNonEmpty(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length == 0) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return copy;
  }

  private static void requireLength(byte[] value, int length, String name) {
    if (value.length != length) {
      throw new IllegalArgumentException(name + " must be exactly " + length + " bytes");
    }
  }

  private static boolean isZero(byte[] value) {
    for (byte current : value) {
      if (current != 0) {
        return false;
      }
    }
    return true;
  }

  public enum P66Phase {
    P66_OFF,
    P66_ACTIVATION,
    P66_ON;

    private boolean directAssetsEnabled() {
      return this != P66_OFF;
    }
  }

  /** Immutable codec identity committed through a participant's Store format version. */
  public static final class StoreFormat {

    private final String dbName;
    private final String codecId;

    private StoreFormat(String dbName, String codecId) {
      this.dbName = dbName;
      this.codecId = codecId;
    }

    public String getDbName() {
      return dbName;
    }

    public String getCodecId() {
      return codecId;
    }

    public int getStoreFormatVersion() {
      return STORE_FORMAT_VERSION;
    }
  }
}
