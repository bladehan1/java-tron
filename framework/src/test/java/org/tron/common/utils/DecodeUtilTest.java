package org.tron.common.utils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tron.core.Constant;

public class DecodeUtilTest {

  private static byte savedPrefixByte;

  @BeforeClass
  public static void saveAddressPrefix() {
    savedPrefixByte = DecodeUtil.addressPreFixByte;
    DecodeUtil.addressPreFixByte = Constant.ADD_PRE_FIX_BYTE_MAINNET;
  }

  @AfterClass
  public static void restoreAddressPrefix() {
    DecodeUtil.addressPreFixByte = savedPrefixByte;
  }

  @Test
  public void addressValidRejectsNull() {
    Assert.assertFalse(DecodeUtil.addressValid(null));
  }

  @Test
  public void addressValidRejectsEmptyArray() {
    Assert.assertFalse(DecodeUtil.addressValid(new byte[0]));
  }

  @Test
  public void addressValidRejectsWrongLength() {
    byte[] tooShort = new byte[DecodeUtil.ADDRESS_SIZE / 2 - 1];
    tooShort[0] = Constant.ADD_PRE_FIX_BYTE_MAINNET;
    Assert.assertFalse(DecodeUtil.addressValid(tooShort));

    byte[] tooLong = new byte[DecodeUtil.ADDRESS_SIZE / 2 + 1];
    tooLong[0] = Constant.ADD_PRE_FIX_BYTE_MAINNET;
    Assert.assertFalse(DecodeUtil.addressValid(tooLong));
  }

  @Test
  public void addressValidRejectsWrongPrefix() {
    byte[] address = new byte[DecodeUtil.ADDRESS_SIZE / 2];
    address[0] = (byte) (Constant.ADD_PRE_FIX_BYTE_MAINNET + 1);
    Assert.assertFalse(DecodeUtil.addressValid(address));
  }

  @Test
  public void addressValidAcceptsCanonicalAddress() {
    byte[] address = new byte[DecodeUtil.ADDRESS_SIZE / 2];
    address[0] = Constant.ADD_PRE_FIX_BYTE_MAINNET;
    Assert.assertTrue(DecodeUtil.addressValid(address));
  }
}
