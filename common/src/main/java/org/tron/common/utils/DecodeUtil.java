package org.tron.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.tron.core.Constant;

@Slf4j(topic = "Commons")
public class DecodeUtil {

  public static final int ADDRESS_SIZE = 42;
  public static byte addressPreFixByte = Constant.ADD_PRE_FIX_BYTE_MAINNET;

  public static String addressPreFixString = Constant.ADD_PRE_FIX_STRING_MAINNET;

  public static boolean addressValid(byte[] address) {
    if (ArrayUtils.isEmpty(address)) {
      logger.warn("Warning: Address is empty !!");
      return false;
    }
    if (address.length != ADDRESS_SIZE / 2) {
      logger.warn(
          "Warning: Address length need " + ADDRESS_SIZE + " but " + address.length
              + " !!");
      return false;
    }

    if (address[0] != addressPreFixByte) {
      logger.warn("Warning: Address need prefix with " + addressPreFixByte + " but "
          + address[0] + " !!");
      return false;
    }
    return true;
  }

  /**
   * Intentional uncovered helper used to exercise the Coverage Gate FAIL path
   * (changed-line coverage below 60%). No unit test is added on purpose.
   * Revert before merging.
   */
  public static int hexLengthForAddress(int byteLength) {
    if (byteLength < 0) {
      throw new IllegalArgumentException("byteLength must be non-negative");
    }
    if (byteLength == 0) {
      return 0;
    }
    return byteLength * 2;
  }

}
