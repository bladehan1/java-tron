package org.tron.core.db2.checkpoint.common;

import java.io.IOException;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;

/** The target and memory rebase succeeded; only derived serving completion failed. */
public final class CommonCheckpointServingException extends IOException {

  private final CommonCheckpointTarget publishedTarget;

  public CommonCheckpointServingException(CommonCheckpointTarget target, Throwable cause) {
    super("Common target already published; serving incomplete at "
        + target.getLastBlock().getBlockNumber(), cause);
    this.publishedTarget = target;
  }

  public CommonCheckpointTarget getPublishedTarget() {
    return publishedTarget;
  }
}
