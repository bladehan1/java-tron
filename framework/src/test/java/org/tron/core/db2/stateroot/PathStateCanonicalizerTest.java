package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.protos.Protocol.Account;
import org.tron.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;

public class PathStateCanonicalizerTest {

  private final PathStateCanonicalizer canonicalizer = new PathStateCanonicalizer();

  @Test
  public void assignsFormatsToTheExact27Scope() {
    PathStateParticipantScope scope = canonicalizer.participantScope();
    assertEquals(27, scope.getParticipants().size());
    assertEquals(PathStateCanonicalizer.ABI_PROTOBUF,
        canonicalizer.requireFormat("abi").getCodecId());
    assertEquals(PathStateCanonicalizer.P66_ACCOUNT,
        canonicalizer.requireFormat("account").getCodecId());
    assertEquals(PathStateCanonicalizer.P66_ACCOUNT_ASSET,
        canonicalizer.requireFormat("account-asset").getCodecId());
    assertEquals(PathStateCanonicalizer.LEGACY_ASSET_PROTOBUF,
        canonicalizer.requireFormat("asset-issue").getCodecId());
    assertEquals(PathStateCanonicalizer.ASSET_V2_PROTOBUF,
        canonicalizer.requireFormat("asset-issue-v2").getCodecId());
    assertEquals(PathStateCanonicalizer.STORAGE_ROW,
        canonicalizer.requireFormat("storage-row").getCodecId());
    assertEquals(PathStateCanonicalizer.PHYSICAL_RAW,
        canonicalizer.requireFormat("proposal").getCodecId());
  }

  @Test
  public void p66OffPreservesAccountBytesAndRejectsDirectRows() throws Exception {
    byte[] address = address(1);
    byte[] raw = account(address).toBuilder()
        .putAsset("legacy-name", 7L)
        .putAssetV2("1000001", 11L)
        .build()
        .toByteArray();

    PathStateMutation account = canonicalizer.put(P66Phase.P66_OFF, "account", address, raw);
    assertArrayEquals(raw, account.getCanonicalValue());
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.accountAsset(P66Phase.P66_OFF, address, "1000001", 11L));
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_OFF, "account-asset",
            accountAssetKey(address, "1000001"), longBytes(11L)));

    byte[] mixed = Account.parseFrom(raw).toBuilder()
        .setAssetOptimized(true)
        .build()
        .toByteArray();
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_OFF, "account", address, mixed));
  }

  @Test
  public void activationAndOnProduceTheSameCanonicalAccountGolden() throws Exception {
    byte[] address = address(2);
    byte[] raw = account(address).toBuilder()
        .setBalance(99L)
        .putAsset("legacy-name", 5L)
        .putAssetV2("1000001", 17L)
        .build()
        .toByteArray();

    PathStateMutation activation = canonicalizer.put(P66Phase.P66_ACTIVATION,
        "account", address, raw);
    PathStateMutation on = canonicalizer.put(P66Phase.P66_ON, "account", address, raw);
    assertArrayEquals(activation.getCanonicalValue(), on.getCanonicalValue());
    Account canonical = Account.parseFrom(activation.getCanonicalValue());
    assertTrue(canonical.getAssetOptimized());
    assertTrue(canonical.getAssetMap().isEmpty());
    assertTrue(canonical.getAssetV2Map().isEmpty());
    assertEquals("1a154100000000000000000000000000000000000000022063e00301",
        ByteArray.toHexString(activation.getCanonicalValue()));
    assertEquals(1, canonicalizer.projectSnapshotAccountAssets(
        P66Phase.P66_ON, address, raw).size());
    assertTrue(canonicalizer.projectSnapshotAccountAssets(
        P66Phase.P66_ON, address, on.getCanonicalValue()).isEmpty());
  }

  @Test
  public void accountAssetZeroIsDeleteAndPresentRowsAreExactSignedLongs() {
    byte[] address = address(3);
    PathStateMutation present = canonicalizer.accountAsset(
        P66Phase.P66_ON, address, "1000001", -9L);
    assertFalse(present.isDelete());
    assertArrayEquals(accountAssetKey(address, "1000001"), present.getCanonicalKey());
    assertArrayEquals(longBytes(-9L), present.getCanonicalValue());

    PathStateMutation zero = canonicalizer.accountAsset(
        P66Phase.P66_ON, address, "1000001", 0L);
    assertTrue(zero.isDelete());
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "account-asset",
            accountAssetKey(address, "1000001"), longBytes(0L)));
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.accountAsset(P66Phase.P66_ON, address, "01", 1L));
  }

  @Test
  public void storageZeroIsDeleteWhileNonzeroWordIsExact() {
    byte[] key = new byte[32];
    key[31] = 1;
    byte[] word = new byte[32];
    word[31] = 2;
    PathStateMutation present = canonicalizer.put(
        P66Phase.P66_ON, "storage-row", key, word);
    assertArrayEquals(word, present.getCanonicalValue());
    assertTrue(canonicalizer.delete(P66Phase.P66_ON, "storage-row", key).isDelete());
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "storage-row", key, new byte[32]));
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "storage-row", new byte[31], word));
  }

  @Test
  public void abiAllowsPresentEmptyButRejectsMalformedProtobufAndAddress() {
    byte[] address = address(4);
    PathStateMutation cleared = canonicalizer.put(P66Phase.P66_ON, "abi", address, new byte[0]);
    assertFalse(cleared.isDelete());
    assertEquals(0, cleared.getCanonicalValue().length);
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "abi", address, new byte[]{-1}));
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "abi", new byte[20], new byte[0]));
  }

  @Test
  public void assetStoresRetainIndependentPhysicalIdentities() {
    AssetIssueContract asset = AssetIssueContract.newBuilder()
        .setName(ByteString.copyFromUtf8("legacy-name"))
        .setId("1000001")
        .build();
    byte[] raw = asset.toByteArray();
    PathStateMutation legacy = canonicalizer.put(P66Phase.P66_ON, "asset-issue",
        "legacy-name".getBytes(StandardCharsets.UTF_8), raw);
    PathStateMutation v2 = canonicalizer.put(P66Phase.P66_ON, "asset-issue-v2",
        "1000001".getBytes(StandardCharsets.US_ASCII), raw);
    assertFalse(legacy.isDelete());
    assertFalse(v2.isDelete());
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "asset-issue",
            "other".getBytes(StandardCharsets.UTF_8), raw));
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "asset-issue-v2",
            "01000001".getBytes(StandardCharsets.US_ASCII), raw));
  }

  @Test
  public void genericPresentEmptyRemainsDistinctFromDelete() {
    byte[] key = new byte[]{1};
    PathStateMutation present = canonicalizer.put(
        P66Phase.P66_ON, "proposal", key, new byte[0]);
    PathStateMutation absent = canonicalizer.delete(P66Phase.P66_ON, "proposal", key);
    assertFalse(present.isDelete());
    assertTrue(absent.isDelete());
    assertArrayEquals(new byte[0], present.getCanonicalValue());
    assertThrows(IllegalArgumentException.class,
        () -> canonicalizer.put(P66Phase.P66_ON, "unknown", key, new byte[0]));
  }

  @Test
  public void genericIndexStoresPreserveEmptyPhysicalKeys() {
    PathStateMutation accountId = canonicalizer.put(
        P66Phase.P66_ON, "accountid-index", new byte[0], address(5));
    PathStateMutation accountName = canonicalizer.put(
        P66Phase.P66_ON, "account-index", new byte[0], address(6));

    assertArrayEquals(new byte[0], accountId.getCanonicalKey());
    assertArrayEquals(new byte[0], accountName.getCanonicalKey());
  }

  private static Account account(byte[] address) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address)).build();
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] accountAssetKey(byte[] address, String tokenId) {
    byte[] token = tokenId.getBytes(StandardCharsets.US_ASCII);
    return ByteBuffer.allocate(address.length + token.length)
        .put(address)
        .put(token)
        .array();
  }

  private static byte[] longBytes(long value) {
    return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
  }
}
