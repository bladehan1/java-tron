package org.tron.common.math;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class StrictMathWrapperTest {

  @Test
  public void toIntExactAcceptsIntegerBoundaries() {
    assertEquals(Integer.MIN_VALUE, StrictMathWrapper.toIntExact(Integer.MIN_VALUE));
    assertEquals(0, StrictMathWrapper.toIntExact(0L));
    assertEquals(Integer.MAX_VALUE, StrictMathWrapper.toIntExact(Integer.MAX_VALUE));
  }

  @Test
  public void toIntExactRejectsOverflow() {
    assertThrows(ArithmeticException.class,
        () -> StrictMathWrapper.toIntExact((long) Integer.MIN_VALUE - 1));
    assertThrows(ArithmeticException.class,
        () -> StrictMathWrapper.toIntExact((long) Integer.MAX_VALUE + 1));
    assertThrows(ArithmeticException.class, () -> StrictMathWrapper.toIntExact(Long.MIN_VALUE));
    assertThrows(ArithmeticException.class, () -> StrictMathWrapper.toIntExact(Long.MAX_VALUE));
  }
}
