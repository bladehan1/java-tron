package org.tron.core.db;

import org.junit.Assert;
import org.junit.Test;

public class ManagerCatchUpModeTrackerTest {

  private static final long EXIT_GAP_MS = 60_000;
  private static final long REENTER_GAP_MS = 10 * 60_000;

  private Manager.CatchUpModeTracker tracker() {
    return new Manager.CatchUpModeTracker(EXIT_GAP_MS, REENTER_GAP_MS);
  }

  @Test
  public void staysInactiveBelowReenterThreshold() {
    Manager.CatchUpModeTracker tracker = tracker();
    Assert.assertFalse(tracker.update(REENTER_GAP_MS - 1));
    Assert.assertFalse(tracker.isActive());
  }

  @Test
  public void entersAtReenterThreshold() {
    Manager.CatchUpModeTracker tracker = tracker();
    Assert.assertTrue(tracker.update(REENTER_GAP_MS));
    Assert.assertTrue(tracker.isActive());
  }

  @Test
  public void exitsAtExitThreshold() {
    Manager.CatchUpModeTracker tracker = tracker();
    tracker.update(REENTER_GAP_MS);
    Assert.assertFalse(tracker.update(EXIT_GAP_MS));
    Assert.assertFalse(tracker.isActive());
  }

  @Test
  public void hysteresisKeepsActiveInsideBand() {
    Manager.CatchUpModeTracker tracker = tracker();
    tracker.update(REENTER_GAP_MS);
    // Between exit and re-enter: gap shrinks below the re-enter threshold but stays above
    // the exit threshold -> must remain active (this is the anti-flapping core).
    Assert.assertTrue(tracker.update(5 * 60_000));
    Assert.assertTrue(tracker.update(2 * 60_000));
    Assert.assertTrue(tracker.update(REENTER_GAP_MS / 2));
    Assert.assertTrue(tracker.isActive());
  }

  @Test
  public void hysteresisKeepsInactiveInsideBand() {
    Manager.CatchUpModeTracker tracker = tracker();
    // Gap oscillates inside the band without ever reaching the re-enter threshold.
    Assert.assertFalse(tracker.update(5 * 60_000));
    Assert.assertFalse(tracker.update(REENTER_GAP_MS - 1));
    Assert.assertFalse(tracker.update(5 * 60_000));
    Assert.assertFalse(tracker.isActive());
  }

  @Test
  public void reentersAfterDeepGapAgain() {
    Manager.CatchUpModeTracker tracker = tracker();
    tracker.update(REENTER_GAP_MS);
    tracker.update(EXIT_GAP_MS);
    Assert.assertFalse(tracker.isActive());
    Assert.assertTrue(tracker.update(REENTER_GAP_MS + 1));
    Assert.assertTrue(tracker.isActive());
  }
}
