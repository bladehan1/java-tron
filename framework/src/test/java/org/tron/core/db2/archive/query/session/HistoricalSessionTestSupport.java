package org.tron.core.db2.archive.query.session;

import java.io.IOException;
import org.tron.core.db2.archive.query.HistoricalQueryControl;
import org.tron.core.db2.archive.query.snapshot.ArchivePointSnapshot;

/** Keeps the fake-snapshot session opener in the production session's test package. */
public final class HistoricalSessionTestSupport {

  private HistoricalSessionTestSupport() {
  }

  public static HistoricalQuerySession openPoint(ArchivePointSnapshot snapshot, byte[] hash,
      HistoricalQuerySession.Limits limits, HistoricalQueryControl control, Runnable release)
      throws IOException {
    return HistoricalQuerySession.openPoint(snapshot, hash, limits, control, release);
  }
}
