package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class PathStateReadDistanceTraceTest {

  @Test
  public void firstTouchGoesToFirstTouchBucket() {
    PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(1000);
    trace.beginBlock(100);
    trace.touch(4, new byte[]{1, 2, 3});
    long[] snapshot = trace.endBlock();
    assertEquals(1, snapshot[10]);
    for (int index = 0; index < 10; index++) {
      assertEquals(0, snapshot[index]);
    }
    assertEquals(0, snapshot[11]);
  }

  @Test
  public void sameBlockRepeatGoesToBucketZero() {
    PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(1000);
    trace.beginBlock(100);
    trace.touch(4, new byte[]{1});
    trace.touch(4, new byte[]{1});
    long[] snapshot = trace.endBlock();
    assertEquals(1, snapshot[0]);
    assertEquals(1, snapshot[10]);
  }

  @Test
  public void distancesLandInExpectedBuckets() {
    long[] distances = new long[]{1, 2, 3, 4, 5, 8, 9, 16, 17, 32, 33, 64, 65, 128, 129};
    int[] expectedBucket = new int[]{1, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9};
    for (int index = 0; index < distances.length; index++) {
      PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(1000);
      byte[] path = new byte[]{(byte) index};
      trace.beginBlock(10);
      trace.touch(4, path);
      trace.beginBlock(10 + distances[index]);
      trace.touch(4, path);
      long[] snapshot = trace.endBlock();
      assertEquals("distance " + distances[index], 1, snapshot[expectedBucket[index]]);
      assertEquals("distance " + distances[index], 1, snapshot[10]);
      long total = 0;
      for (long count : snapshot) {
        total += count;
      }
      assertEquals("distance " + distances[index], 2, total);
    }
  }

  @Test
  public void tracksTouchesPerStoreIndependently() {
    PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(1000);
    byte[] path = new byte[]{9};
    trace.beginBlock(7);
    trace.touch(4, path);
    trace.touch(5, path);
    long[] snapshot = trace.endBlock();
    assertEquals(2, snapshot[10]);
    assertEquals(2, trace.trackedKeys());

    trace.beginBlock(8);
    trace.touch(4, path);
    trace.touch(5, path);
    long[] repeat = trace.endBlock();
    assertEquals(2, repeat[1]);
  }

  @Test
  public void dropsNewKeysBeyondCapAndCountsOverflow() {
    PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(4);
    trace.beginBlock(1);
    for (int index = 0; index < 8; index++) {
      trace.touch(4, new byte[]{(byte) index});
    }
    long[] snapshot = trace.endBlock();
    assertEquals(4, snapshot[10]);
    assertEquals(4, snapshot[11]);
    assertEquals(4, trace.trackedKeys());
  }

  @Test
  public void endBlockResetsCountsButKeepsLastTouch() {
    PathStateReadDistanceTrace trace = new PathStateReadDistanceTrace(1000);
    trace.beginBlock(1);
    trace.touch(4, new byte[]{1});
    trace.endBlock();
    trace.beginBlock(2);
    trace.touch(4, new byte[]{1});
    long[] snapshot = trace.endBlock();
    assertEquals(1, snapshot[1]);
    assertEquals(0, snapshot[0]);
    assertEquals(0, snapshot[10]);
  }

  @Test
  public void rejectsNonPositiveMaxKeys() {
    assertThrows(IllegalArgumentException.class, () -> new PathStateReadDistanceTrace(0));
  }
}
