package org.tron.core.db2.archive.checkpoint.progress;

import java.io.IOException;

/** Engine-neutral durable participant D authority source. */
@FunctionalInterface
public interface ArchiveParticipantProgressSource {

  ArchiveProgressEnvelope loadProgress() throws IOException;
}
