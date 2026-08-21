package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.tron.common.crypto.Hash;
import org.tron.core.capsule.utils.RLP;

public class PathStateCommitmentCodecTest {

  private static final byte[] STORE_DOMAIN =
      "java-tron/path-state/store-leaf-key".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SUPER_DOMAIN =
      "java-tron/path-state/super-leaf-key".getBytes(StandardCharsets.US_ASCII);

  @Test
  public void fixedStoreKeyGoldensMatchIndependentKeccakOracle() throws Exception {
    byte[] accountKey = Hex.decode("410000000000000000000000000000000000000001");
    byte[] storageKey = new byte[32];
    for (int i = 0; i < storageKey.length; i++) {
      storageKey[i] = (byte) i;
    }

    assertGolden("0b7f18d3381a9e44da93058f4214d7f0d818d1824be0f30839ed5200eb7f946a",
        4, accountKey);
    assertGolden("90ad9575451bd26f005db5063deaeac71d8b221edd0a0bb0a345f21b40c16ff5",
        22, storageKey);
    assertGolden("ec6a48ade48f24cd456a89de3a5f86d282b0ae9892f265be58e5e8a704856350",
        21, new byte[]{1});
  }

  @Test
  public void approvedAbiAndAssetIssueStoresHaveIndependentLeafDomains() throws Exception {
    assertGolden("14af9866899065b509f6ea5d45902d443a4357efad5e6478c47ef108d603b8d7",
        1, new byte[]{1});
    assertGolden("5c0a5639b07fa98f7e067c3e0c1d067ca1de752810b7576bc20fd2c75ba89eb5",
        6, new byte[]{1});
    assertGolden("99951328ba6d7d4a7fa199cf727a7c4494bd885ce2ac8c04475dd1fd699af7de",
        7, new byte[]{1});
  }

  @Test
  public void presentValuesKeepEmptyAndZeroDistinct() {
    assertArrayEquals(Hex.decode("c20180"),
        PathStateCommitmentCodec.presentLeafValue(new byte[0]));
    assertArrayEquals(Hex.decode("c20100"),
        PathStateCommitmentCodec.presentLeafValue(new byte[]{0}));
    assertArrayEquals(Hex.decode("c7018568656c6c6f"),
        PathStateCommitmentCodec.presentLeafValue("hello".getBytes(StandardCharsets.US_ASCII)));
    assertFalse(Arrays.equals(PathStateCommitmentCodec.presentLeafValue(new byte[0]),
        PathStateCommitmentCodec.presentLeafValue(new byte[]{0})));
  }

  @Test
  public void superLeafGoldensBindStableIdentityFormatAndRoot() throws Exception {
    byte[] storeRoot = new byte[32];
    for (int i = 0; i < storeRoot.length; i++) {
      storeRoot[i] = (byte) i;
    }

    assertArrayEquals(
        Hex.decode("cf8715b85b2ac18d2b63e57b9e8902887f1986df7dc6a46da01fbc1f8f99f8bf"),
        PathStateCommitmentCodec.superLeafKey(4));
    assertArrayEquals(referenceSuperKey(4), PathStateCommitmentCodec.superLeafKey(4));
    assertArrayEquals(Hex.decode("f38400000004876163636f756e748400000001a0000102030405060708090a0b"
            + "0c0d0e0f101112131415161718191a1b1c1d1e1f"),
        PathStateCommitmentCodec.superLeafValue(4, "account", 1, storeRoot));
    assertArrayEquals(referenceSuperValue(4, "account", 1, storeRoot),
        PathStateCommitmentCodec.superLeafValue(4, "account", 1, storeRoot));
  }

  @Test
  public void rejectsAmbiguousOrUnboundInputs() {
    assertThrows(IllegalArgumentException.class,
        () -> PathStateCommitmentCodec.storeLeafKey(0, new byte[]{1}));
    assertThrows(IllegalArgumentException.class,
        () -> PathStateCommitmentCodec.storeLeafKey(1, new byte[0]));
    assertThrows(NullPointerException.class,
        () -> PathStateCommitmentCodec.presentLeafValue(null));
    assertThrows(IllegalArgumentException.class,
        () -> PathStateCommitmentCodec.superLeafValue(1, "", 1, new byte[32]));
    assertThrows(IllegalArgumentException.class,
        () -> PathStateCommitmentCodec.superLeafValue(1, "account", 0, new byte[32]));
    assertThrows(IllegalArgumentException.class,
        () -> PathStateCommitmentCodec.superLeafValue(1, "account", 1, new byte[31]));
  }

  private static void assertGolden(String expectedHex, int storeId, byte[] key)
      throws IOException {
    byte[] actual = PathStateCommitmentCodec.storeLeafKey(storeId, key);
    assertArrayEquals(Hex.decode(expectedHex), actual);
    assertArrayEquals(referenceStoreKey(storeId, key), actual);
  }

  private static byte[] referenceStoreKey(int storeId, byte[] key) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeShort(STORE_DOMAIN.length);
      output.write(STORE_DOMAIN);
      output.writeShort(PathStateCommitmentCodec.FORMAT_VERSION);
      output.writeInt(storeId);
      output.writeInt(key.length);
      output.write(key);
    }
    return new Keccak.Digest256().digest(bytes.toByteArray());
  }

  private static byte[] referenceSuperKey(int storeId) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeShort(SUPER_DOMAIN.length);
      output.write(SUPER_DOMAIN);
      output.writeShort(PathStateCommitmentCodec.FORMAT_VERSION);
      output.writeInt(storeId);
    }
    return new Keccak.Digest256().digest(bytes.toByteArray());
  }

  private static byte[] referenceSuperValue(int storeId, String dbName, int formatVersion,
      byte[] storeRoot) {
    return RLP.encodeList(Hash.encodeElement(ByteBuffer.allocate(4).putInt(storeId).array()),
        Hash.encodeElement(dbName.getBytes(StandardCharsets.UTF_8)),
        Hash.encodeElement(ByteBuffer.allocate(4).putInt(formatVersion).array()),
        Hash.encodeElement(storeRoot));
  }
}
