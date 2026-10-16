package org.tron.core.db2.core;

import java.io.IOException;

/** Builds an in-memory completion plan after every durable authority published one target. */
@FunctionalInterface
public interface CommonCheckpointMemoryRebaser {

  RebasePlan prepare(CommonCheckpointTarget target) throws IOException;

  /** A fully validated pointer-only completion that must not perform fallible work. */
  @FunctionalInterface
  interface RebasePlan {

    void apply();
  }
}
