package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.List;

/** Engine-neutral archive participant with one atomic business+D apply boundary. */
public interface ArchiveParticipant extends ArchiveParticipantProgressSource {

  void apply(List<ArchiveParticipantMutation> mutations, ArchiveProgressEnvelope progress)
      throws IOException;
}
