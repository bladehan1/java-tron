package org.tron.core.db2.stateroot.runtime;

import java.io.IOException;

/** Keeps queue-capacity injection in test sources while exercising the actual worker. */
public final class PathStateHeadTestSupport {
  private PathStateHeadTestSupport() {
  }

  public static PathStateAsyncPrepareHead async(PathStateHead head, int capacity)
      throws IOException {
    return new PathStateAsyncPrepareHead(head, capacity);
  }
}
