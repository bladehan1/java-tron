package org.tron.core.db2.archive.serving.telemetry;

/** Bridges the root integration fixture to the unchanged package-private timing observation. */
public final class ServingTimingTestAccess {
  private ServingTimingTestAccess() {}

  public static long calls(ServingIndexTiming timing, ServingIndexTiming.Stage stage) {
    return timing.calls(stage);
  }
}
