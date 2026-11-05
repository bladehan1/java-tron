package org.tron.core.db2.archive.serving.index;

import java.io.IOException;
import org.tron.core.db2.archive.checkpoint.progress.ArchiveProgressEnvelope;

/** Pins a generation lease; ownership of the shared catalog stays with its runtime. */
@FunctionalInterface
public interface ServingKeyIndexSource {

  PinnedServingKeyIndex pin(ArchiveProgressEnvelope readerVisible) throws IOException;
}
