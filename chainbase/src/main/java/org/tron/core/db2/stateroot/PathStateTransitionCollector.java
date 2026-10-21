package org.tron.core.db2.stateroot;

import java.io.IOException;
import org.tron.core.db2.archive.BlockChangeView;

/** Builds one immutable path-state transition from the shared block-final change view. */
@FunctionalInterface
public interface PathStateTransitionCollector {

  PathStateBlockTransition collect(BlockChangeView view) throws IOException;
}
