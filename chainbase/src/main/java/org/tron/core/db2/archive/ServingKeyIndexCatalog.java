package org.tron.core.db2.archive;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Atomic publication point for immutable serving-key-index generations. */
public final class ServingKeyIndexCatalog {

  private final AtomicReference<ServingKeyIndexGeneration> current;

  public ServingKeyIndexCatalog(ServingKeyIndexGeneration initial) {
    current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
  }

  /** Pins the current immutable generation by strong reference. */
  public ServingKeyIndexGeneration pin() {
    return current.get();
  }

  /** Publishes only if the generation used as the build base is still current. */
  public boolean publish(ServingKeyIndexGeneration expected,
      ServingKeyIndexGeneration replacement) {
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(replacement, "replacement");
    if (replacement.getIndexedFrom() != expected.getIndexedFrom()) {
      throw new IllegalArgumentException("Serving index replacement changes the coverage base");
    }
    if (replacement.getIndexedThrough() < expected.getIndexedThrough()) {
      throw new IllegalArgumentException("Serving index replacement regresses the watermark");
    }
    return current.compareAndSet(expected, replacement);
  }
}
