package org.tron.core.store;

import static java.lang.System.arraycopy;

import java.util.Objects;
import org.tron.common.crypto.Hash;
import org.tron.common.utils.ByteUtil;

/** Canonical physical-key mapping shared by latest and historical contract storage reads. */
public final class StorageRowKeyCodec {

  public static final int KEY_BYTES = 32;
  /** Key width of rows written after ALLOW_OPTIMIZE_TVM_STORAGE (proposal #99) is active. */
  public static final int OPTIMIZED_KEY_BYTES = 48;
  private static final int HALF_KEY_BYTES = KEY_BYTES / 2;

  private StorageRowKeyCodec() {
  }

  /**
   * Maps a logical contract address and slot to the existing 32-byte storage-row key.
   *
   * <p>The mapping intentionally preserves the current truncated alias/collision semantics.
   */
  public static byte[] physicalKey(byte[] address, byte[] logicalSlot, int contractVersion,
      byte[] createTransactionHash) {
    return physicalKeyFromAddressHash(addressHash(address, createTransactionHash), logicalSlot,
        contractVersion);
  }

  /** Returns the address-side hash used by the storage-row key mapping. */
  public static byte[] addressHash(byte[] address, byte[] createTransactionHash) {
    Objects.requireNonNull(address, "address");
    if (ByteUtil.isNullOrZeroArray(createTransactionHash)) {
      return Hash.sha3(address);
    }
    return Hash.sha3(ByteUtil.merge(address, createTransactionHash));
  }

  /** Maps a precomputed address hash and logical slot to the existing storage-row key. */
  public static byte[] physicalKeyFromAddressHash(byte[] addressHash, byte[] logicalSlot,
      int contractVersion) {
    Objects.requireNonNull(addressHash, "addressHash");
    Objects.requireNonNull(logicalSlot, "logicalSlot");
    if (addressHash.length != KEY_BYTES) {
      throw new IllegalArgumentException("addressHash must be exactly 32 bytes");
    }
    if (logicalSlot.length != KEY_BYTES) {
      throw new IllegalArgumentException("logicalSlot must be exactly 32 bytes");
    }
    byte[] transformedSlot = contractVersion == 1 ? Hash.sha3(logicalSlot) : logicalSlot;
    byte[] physicalKey = new byte[KEY_BYTES];
    arraycopy(addressHash, 0, physicalKey, 0, HALF_KEY_BYTES);
    arraycopy(transformedSlot, HALF_KEY_BYTES, physicalKey, HALF_KEY_BYTES, HALF_KEY_BYTES);
    return physicalKey;
  }

  /**
   * Maps a precomputed address hash and logical slot to the 48-byte storage-row key used once
   * ALLOW_OPTIMIZE_TVM_STORAGE is active: the 16-byte address prefix followed by
   * {@code sha3(addressHash || logicalSlot)}. Mirrors GreatVoyage-v4.8.2.3 {@code Storage}.
   */
  public static byte[] optimizedKeyFromAddressHash(byte[] addressHash, byte[] logicalSlot) {
    Objects.requireNonNull(addressHash, "addressHash");
    Objects.requireNonNull(logicalSlot, "logicalSlot");
    if (addressHash.length != KEY_BYTES) {
      throw new IllegalArgumentException("addressHash must be exactly 32 bytes");
    }
    if (logicalSlot.length != KEY_BYTES) {
      throw new IllegalArgumentException("logicalSlot must be exactly 32 bytes");
    }
    byte[] physicalKey = new byte[OPTIMIZED_KEY_BYTES];
    arraycopy(addressHash, 0, physicalKey, 0, HALF_KEY_BYTES);
    arraycopy(Hash.sha3(ByteUtil.merge(addressHash, logicalSlot)), 0, physicalKey,
        HALF_KEY_BYTES, KEY_BYTES);
    return physicalKey;
  }

  /** Maps a logical contract address and slot to the 48-byte optimized storage-row key. */
  public static byte[] optimizedKey(byte[] address, byte[] logicalSlot,
      byte[] createTransactionHash) {
    return optimizedKeyFromAddressHash(addressHash(address, createTransactionHash), logicalSlot);
  }
}
