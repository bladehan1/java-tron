package org.tron.core.db2.core;

import java.io.IOException;

/** One idempotent authority participant in common-checkpoint redo and publication. */
public interface CommonCheckpointMaterializer {

  Authority authority();

  /**
   * Returns only an exact state for {@code target}. Implementations must throw when durable state
   * is corrupt, ambiguous, or belongs to a different target.
   */
  Status inspect(CommonCheckpointTarget target) throws IOException;

  /** Idempotently writes and forces this authority's data without publishing its public marker. */
  void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target)
      throws IOException;

  /** Idempotently publishes this authority's already-materialized exact target. */
  void publish(CommonCheckpointTarget target) throws IOException;

  enum Authority {
    CHAINBASE,
    PATH_STATE,
    STATE_ARCHIVE
  }

  enum Status {
    NEEDS_MATERIALIZATION,
    MATERIALIZED,
    PUBLISHED
  }
}
