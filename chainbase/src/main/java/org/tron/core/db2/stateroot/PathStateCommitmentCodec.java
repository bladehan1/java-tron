package org.tron.core.db2.stateroot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.tron.common.crypto.Hash;

/**
 * Experimental byte contract for the TASK-016 current path-state commitment.
 *
 * <p>This codec is independent from the existing account trie and State Archive formats. Its
 * output is not a persistent compatibility promise until the H1-L1 gate approves the root domain
 * and golden vectors.
 */
public final class PathStateCommitmentCodec {

  public static final int FORMAT_VERSION = 1;
  public static final int ROOT_LENGTH = 32;

  private static final byte PRESENT_TAG = 1;
  private static final byte[] STORE_LEAF_KEY_DOMAIN =
      "java-tron/path-state/store-leaf-key".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SUPER_LEAF_KEY_DOMAIN =
      "java-tron/path-state/super-leaf-key".getBytes(StandardCharsets.US_ASCII);
  private static final int RLP_SHORT_LIMIT = 56;
  private static final int RLP_SHORT_ITEM_OFFSET = 0x80;
  private static final int RLP_LONG_ITEM_OFFSET = 0xb7;
  private static final int RLP_SHORT_LIST_OFFSET = 0xc0;
  private static final int RLP_LONG_LIST_OFFSET = 0xf7;

  private PathStateCommitmentCodec() {
  }

  /** Returns the secure per-Store trie key for one exact physical raw key. */
  public static byte[] storeLeafKey(int stableStoreId, byte[] physicalRawKey) {
    requireStoreId(stableStoreId);
    byte[] key = copy(physicalRawKey, "physicalRawKey");
    ByteBuffer material = ByteBuffer.allocate(Short.BYTES + STORE_LEAF_KEY_DOMAIN.length
        + Short.BYTES + Integer.BYTES + Integer.BYTES + key.length);
    putDomain(material, STORE_LEAF_KEY_DOMAIN);
    material.putShort((short) FORMAT_VERSION);
    material.putInt(stableStoreId);
    material.putInt(key.length);
    material.put(key);
    return Hash.sha3(material.array());
  }

  /** Encodes PRESENT(empty) distinctly from PRESENT(0x00); ABSENT has no leaf. */
  public static byte[] presentLeafValue(byte[] physicalRawValue) {
    byte[] value = Arrays.copyOf(Objects.requireNonNull(physicalRawValue, "physicalRawValue"),
        physicalRawValue.length);
    return rlpList(new byte[]{PRESENT_TAG}, value);
  }

  /** Returns the secure super-trie key for one stable Store identity. */
  public static byte[] superLeafKey(int stableStoreId) {
    requireStoreId(stableStoreId);
    ByteBuffer material = ByteBuffer.allocate(Short.BYTES + SUPER_LEAF_KEY_DOMAIN.length
        + Short.BYTES + Integer.BYTES);
    putDomain(material, SUPER_LEAF_KEY_DOMAIN);
    material.putShort((short) FORMAT_VERSION);
    material.putInt(stableStoreId);
    return Hash.sha3(material.array());
  }

  /** Encodes a Store identity and current Store root as one super-trie leaf value. */
  public static byte[] superLeafValue(int stableStoreId, String dbName, int storeFormatVersion,
      byte[] storeRoot) {
    requireStoreId(stableStoreId);
    if (storeFormatVersion <= 0) {
      throw new IllegalArgumentException("storeFormatVersion must be positive");
    }
    String name = Objects.requireNonNull(dbName, "dbName");
    byte[] encodedName = name.getBytes(StandardCharsets.UTF_8);
    if (encodedName.length == 0 || encodedName.length > 128) {
      throw new IllegalArgumentException("dbName must encode to 1..128 bytes");
    }
    byte[] root = Objects.requireNonNull(storeRoot, "storeRoot");
    if (root.length != ROOT_LENGTH) {
      throw new IllegalArgumentException("storeRoot must be exactly 32 bytes");
    }
    return rlpList(intBytes(stableStoreId), encodedName, intBytes(storeFormatVersion), root);
  }

  private static void putDomain(ByteBuffer target, byte[] domain) {
    target.putShort((short) domain.length);
    target.put(domain);
  }

  private static byte[] intBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
  }

  private static void requireStoreId(int stableStoreId) {
    if (stableStoreId <= 0) {
      throw new IllegalArgumentException("stableStoreId must be positive");
    }
  }

  private static byte[] copy(byte[] value, String name) {
    return Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
  }

  private static byte[] rlpList(byte[]... rawItems) {
    byte[][] encoded = new byte[rawItems.length][];
    int payloadLength = 0;
    for (int i = 0; i < rawItems.length; i++) {
      encoded[i] = rlpItem(Objects.requireNonNull(rawItems[i], "raw RLP item"));
      payloadLength = Math.addExact(payloadLength, encoded[i].length);
    }
    byte[] prefix = rlpLength(payloadLength, RLP_SHORT_LIST_OFFSET, RLP_LONG_LIST_OFFSET);
    ByteBuffer result = ByteBuffer.allocate(Math.addExact(prefix.length, payloadLength));
    result.put(prefix);
    for (byte[] item : encoded) {
      result.put(item);
    }
    return result.array();
  }

  private static byte[] rlpItem(byte[] raw) {
    if (raw.length == 1 && (raw[0] & 0xff) < RLP_SHORT_ITEM_OFFSET) {
      return Arrays.copyOf(raw, raw.length);
    }
    byte[] prefix = rlpLength(raw.length, RLP_SHORT_ITEM_OFFSET, RLP_LONG_ITEM_OFFSET);
    ByteBuffer result = ByteBuffer.allocate(Math.addExact(prefix.length, raw.length));
    result.put(prefix);
    result.put(raw);
    return result.array();
  }

  private static byte[] rlpLength(int length, int shortOffset, int longOffset) {
    if (length < RLP_SHORT_LIMIT) {
      return new byte[]{(byte) (shortOffset + length)};
    }
    int lengthOfLength = (Integer.SIZE - Integer.numberOfLeadingZeros(length) + 7) / 8;
    byte[] encoded = new byte[lengthOfLength + 1];
    encoded[0] = (byte) (longOffset + lengthOfLength);
    for (int i = lengthOfLength; i > 0; i--) {
      encoded[i] = (byte) length;
      length >>>= Byte.SIZE;
    }
    return encoded;
  }
}
