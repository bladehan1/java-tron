package org.tron.common.setting;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** A single 30 GiB free-space admission boundary; shutdown flush remains permitted. */
public final class ArchiveDiskRedLine {
  public static final long MIN_FREE_BYTES = 30L * 1024 * 1024 * 1024;

  @FunctionalInterface
  public interface Probe {
    long usableBytes(Path path) throws IOException;
  }

  private final List<Path> directories;
  private final Probe probe;
  private final Runnable stop;
  private final AtomicBoolean stopped = new AtomicBoolean();

  public ArchiveDiskRedLine(List<Path> directories, Probe probe, Runnable stop) {
    this.directories = new ArrayList<>(directories);
    this.probe = probe;
    this.stop = stop;
  }

  public static long usableBytes(Path path) throws IOException {
    Path existing = path.toAbsolutePath();
    while (!Files.exists(existing)) {
      existing = existing.getParent();
      if (existing == null) {
        throw new IOException("No filesystem for " + path);
      }
    }
    return Files.getFileStore(existing).getUsableSpace();
  }

  /** Stop is latched once; errors reading capacity also refuse new work. */
  public void check() {
    if (!stopped.get()) {
      try {
        for (Path directory : directories) {
          if (probe.usableBytes(directory) < MIN_FREE_BYTES) {
            refuse("Archive free space is below 30 GiB: " + directory);
          }
        }
      } catch (IOException failure) {
        refuse("Archive free-space probe failed: " + failure.getMessage());
      }
    }
    if (stopped.get()) {
      throw new IllegalStateException("Archive admission stopped by disk redline");
    }
  }

  private void refuse(String reason) {
    if (stopped.compareAndSet(false, true)) {
      stop.run();
    }
    throw new IllegalStateException(reason);
  }
}
