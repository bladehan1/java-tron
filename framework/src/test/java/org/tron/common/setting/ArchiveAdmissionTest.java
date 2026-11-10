package org.tron.common.setting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.tron.core.config.archive.ArchiveRocksDbPolicy;
import org.tron.core.config.args.Storage;

public class ArchiveAdmissionTest {
  @Test
  public void diskBoundaryAndFailureLatch() {
    AtomicLong free = new AtomicLong(ArchiveDiskRedLine.MIN_FREE_BYTES);
    AtomicInteger stops = new AtomicInteger();
    ArchiveDiskRedLine gate = new ArchiveDiskRedLine(
        Collections.singletonList(Paths.get("unused")), path -> free.get(), stops::incrementAndGet);
    gate.check();
    free.decrementAndGet();
    assertThrows(IllegalStateException.class, gate::check);
    free.set(Long.MAX_VALUE);
    assertThrows(IllegalStateException.class, gate::check);
    assertEquals(1, stops.get());
  }

  @Test
  public void failedProbeStopsAdmission() {
    AtomicInteger stops = new AtomicInteger();
    ArchiveDiskRedLine gate = new ArchiveDiskRedLine(
        Collections.singletonList(Paths.get("unused")), path -> {
      throw new IOException("probe failure");
    }, stops::incrementAndGet);
    assertThrows(IllegalStateException.class, gate::check);
    assertEquals(1, stops.get());
  }

  @Test
  public void archiveRejectsMixedEnginesWhileOffKeepsUpstreamChoice() {
    Storage storage = new Storage();
    storage.setDbEngine("LEVELDB");
    ArchiveRocksDbPolicy.validate(storage);
    storage.setCommonCheckpointEnabled(true);
    assertThrows(IllegalArgumentException.class, () -> ArchiveRocksDbPolicy.validate(storage));
    storage.setDbEngine("ROCKSDB");
    ArchiveRocksDbPolicy.validate(storage);
    storage.setStateArchiveServingIndexEngine("LEVELDB");
    assertThrows(IllegalArgumentException.class, () -> ArchiveRocksDbPolicy.validate(storage));
    storage.setStateArchiveServingIndexEngine("ROCKSDB");
    storage.setPathStateRootEngine("LEVELDB");
    assertThrows(IllegalArgumentException.class, () -> ArchiveRocksDbPolicy.validate(storage));
  }
}
