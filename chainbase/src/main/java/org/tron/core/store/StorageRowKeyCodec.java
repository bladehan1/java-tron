package org.tron.core.store;

import static java.lang.System.arraycopy;

import java.util.Objects;
import org.tron.common.crypto.Hash;
import org.tron.common.utils.ByteUtil;

/** Canonical physical-key mapping shared by latest and historical contract storage reads. */
public final class StorageRowKeyCodec {

  public static final int KEY_BYTES = 32;
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
}
