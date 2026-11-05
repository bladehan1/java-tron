package org.tron.core.db2.archive.checkpoint.progress;

import java.io.IOException;
import java.nio.file.Path;

/** Read-only progress-file capability shared by latest-state and snapshot acquisition. */
public final class ArchiveProgressReader {

  private final ArchiveProgressFile file;

  public ArchiveProgressReader(Path path) {
    this(path, new ArchiveProgressEnvelopeCodec());
  }

  public ArchiveProgressReader(Path path, ArchiveProgressEnvelopeCodec codec) {
    file = new ArchiveProgressFile(path, codec);
  }

  public ArchiveProgressEnvelope read() throws IOException {
    return file.load();
  }
}
