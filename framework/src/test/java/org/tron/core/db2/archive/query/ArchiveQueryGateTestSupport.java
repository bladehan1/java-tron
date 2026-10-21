package org.tron.core.db2.archive.query;

import java.io.IOException;
import org.tron.core.db2.archive.query.snapshot.ArchiveReadSnapshot;

/** Shares the package-private pin-source fixture with tests of root-package storage owners. */
public final class ArchiveQueryGateTestSupport {

  private ArchiveQueryGateTestSupport() {
  }

  public static ArchiveRuntimeQueryGate create(PinSource source) {
    return new ArchiveRuntimeQueryGate(source::pin);
  }

  @FunctionalInterface
  public interface PinSource {
    ArchiveReadSnapshot pin(long targetBlock) throws IOException;
  }
}
