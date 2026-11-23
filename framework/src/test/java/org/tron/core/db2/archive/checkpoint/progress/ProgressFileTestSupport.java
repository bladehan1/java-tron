package org.tron.core.db2.archive.checkpoint.progress;

import java.io.IOException;
import java.nio.file.Path;

public final class ProgressFileTestSupport {
  private final ArchiveProgressFile file;

  public ProgressFileTestSupport(Path path, ArchiveProgressEnvelopeCodec codec) {
    file = new ArchiveProgressFile(path, codec);
  }

  public void store(ArchiveProgressEnvelope envelope) throws IOException {
    file.store(envelope);
  }

  public ArchiveProgressEnvelope load() throws IOException {
    return file.load();
  }
}
