package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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

    assertGolden("18d18850670fc1314f55e5346718606abf66777fdd67228eb193bbffffd9a2d7",
        4, accountKey);
    assertGolden("ec0aa93f7d668a1604e02eef0876edc5ff8e3904b117c172f066085fb290a26f",
        22, storageKey);
    assertGolden("21bccc1258bd8d933e0c1de0cb40c3c33e3c5b12beb832d29313f2f99dd9ce0d",
        21, new byte[]{1});
  }

  @Test
  public void approvedAbiAndAssetIssueStoresHaveIndependentLeafDomains() throws Exception {
    assertGolden("29f5801fed0819272800fc0bb431887f257e7e52d18086edbb61b21dd38a2aaa",
        1, new byte[]{1});
    assertGolden("94ae32adcf9abf3bab5286ae66f5f1939083e78918f4621bbf7cd3ace8305101",
        6, new byte[]{1});
    assertGolden("01c15364ad6927f41150cd5900739eb1a117c2f5e4d71c63c3a456eea63e401b",
        7, new byte[]{1});
  }

  @Test
  public void lengthDelimitedEmptyKeyHasAnIndependentLeafIdentity() throws Exception {
    byte[] empty = PathStateCommitmentCodec.storeLeafKey(2, new byte[0]);
    assertArrayEquals(referenceStoreKey(2, new byte[0]), empty);
    assertFalse(Arrays.equals(empty,
        PathStateCommitmentCodec.storeLeafKey(2, new byte[]{0})));
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

    assertEquals("8c8018ac64709921cac7388f659d7396acef5e373bf3b329f9d93088536434a1",
        Hex.toHexString(PathStateCommitmentCodec.superLeafKey(4)));
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
    assertEquals(expectedHex, Hex.toHexString(actual));
    assertArrayEquals(referenceStoreKey(storeId, key), actual);
  }

  private static byte[] referenceStoreKey(int storeId, byte[] key) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeShort(STORE_DOMAIN.length);
      output.write(STORE_DOMAIN);
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
