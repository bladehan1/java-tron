package org.tron.core.db2.archive.serving;

import java.io.IOException;
import org.tron.core.db2.archive.ArchiveProgressEnvelope;

/** Pins a generation lease; ownership of the shared catalog stays with its runtime. */
@FunctionalInterface
public interface ServingKeyIndexSource {

  PinnedServingKeyIndex pin(ArchiveProgressEnvelope readerVisible) throws IOException;
}
