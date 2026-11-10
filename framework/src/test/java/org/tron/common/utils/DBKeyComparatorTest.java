package org.tron.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.tron.core.capsule.utils.MarketUtils;


@Slf4j
public class DBKeyComparatorTest {


  @Test
  public void dbComparing() {
    MarketOrderPriceComparatorForLevelDB comparator = new MarketOrderPriceComparatorForLevelDB();

    byte[] pairPriceKey1 = MarketUtils.createPairPriceKey(
        ByteArray.fromString("100"),
        ByteArray.fromString("200"),
        1000L,
        2000L
    );
    byte[] pairPriceKey2 = MarketUtils.createPairPriceKey(
        ByteArray.fromString("100"),
        ByteArray.fromString("200"),
        1000L,
        2001L
    );
    Assert.assertEquals(-1, comparator.compare(pairPriceKey1, pairPriceKey2));
  }


  @Test
  public void pairKeyIsEqual() {

    byte[] pairPriceKey1 = MarketUtils.createPairPriceKey(
        ByteArray.fromString("100"),
        ByteArray.fromString("200"),
        1000L,
        2000L
    );
    byte[] pairPriceKey2 = MarketUtils.createPairPriceKey(
        ByteArray.fromString("10"),
        ByteArray.fromString("200"),
        1000L,
        2001L
    );

    Assert.assertFalse(MarketUtils.pairKeyIsEqual(pairPriceKey1, pairPriceKey2));
  }

  @Test
  public void shortInternalKeysUseDeterministicUnsignedOrdering() {
    byte[] empty = new byte[0];
    byte[] shortKey = new byte[]{1, (byte) 0xff};
    byte[] valid = MarketUtils.createPairPriceKey(
        ByteArray.fromString("100"), ByteArray.fromString("200"), 1000L, 2000L);

    Assert.assertEquals(0, MarketComparator.comparePriceKey(empty, empty));
    Assert.assertEquals(-1, MarketComparator.comparePriceKey(empty, shortKey));
    Assert.assertEquals(1, MarketComparator.comparePriceKey(shortKey, empty));
    Assert.assertTrue(MarketComparator.comparePriceKey(shortKey, valid) < 0);
    Assert.assertTrue(MarketComparator.comparePriceKey(valid, shortKey) > 0);
  }



}
