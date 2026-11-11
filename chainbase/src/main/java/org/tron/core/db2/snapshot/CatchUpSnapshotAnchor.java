package org.tron.core.db2.snapshot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.FileUtil;
import org.tron.core.config.args.Storage;

/**
 * Catch-up anchor snapshots (test branch {@code feature/state_archive_test2}).
 *
 * <p>While the node is catching up, the common checkpoint cadence stretches to
 * {@code anchorBlocks} (a memory-safety cap only) and this component takes one filesystem
 * anchor of the whole output directory on a timer: every {@code anchorIntervalMinutes} it
 * copies the tree — reflink/clonefile where the filesystem supports it, otherwise hardlink
 * for immutable files (RocksDB SSTs) plus plain copy for everything else. An unclean restart
 * restores the output directory from the anchor and re-syncs the gap; a clean restart keeps
 * the newer live state.
 *
 * <p>The component is stateless: the anchor height/time and the RUNNING marker live only in
 * files, so restarts re-read them; in-memory caches are hints only.
 */
public final class CatchUpSnapshotAnchor {

  private static final Logger logger = LoggerFactory.getLogger("DB");

  static final String ANCHOR_DIR = "anchor";
  static final String TMP_DIR = "anchor.tmp";
  static final String OLD_DIR = "anchor.old";
  static final String MARKER = "ANCHOR";
  static final String RUNNING = "RUNNING";

  /** Files that are sealed by construction and never written again after creation. */
  private static final String IMMUTABLE_LINK_SUFFIX = ".sst";

  /** Copy strategy for the anchor tree. */
  enum CopyMode {
    /** Whole-tree clone via cp: --reflink (GNU/Linux FICLONE) or -c (APFS clonefile). */
    REFLINK,
    /** Per-file walk: hardlink immutable files, copy the rest. */
    HYBRID_LINK
  }

  private final boolean enabled;
  private final int anchorBlocks;
  private final long anchorIntervalMs;
  private final long exitGapMs;
  private final long reenterGapMs;
  private final Path outputDirectory;
  private final Path anchorParent;
  private final CopyMode copyMode;

  private volatile long lastAnchorUtcMs;

  private CatchUpSnapshotAnchor(boolean enabled, int anchorBlocks, int anchorIntervalMinutes,
      long exitGapSeconds, long reenterGapMinutes, String directory, Path outputDirectory,
      CopyMode copyMode) {
    this.enabled = enabled;
    this.anchorBlocks = anchorBlocks;
    this.anchorIntervalMs = anchorIntervalMinutes * 60_000L;
    this.exitGapMs = exitGapSeconds * 1_000L;
    this.reenterGapMs = reenterGapMinutes * 60_000L;
    Path out = outputDirectory.toAbsolutePath().normalize();
    this.outputDirectory = out;
    this.copyMode = copyMode;
    if (directory == null || directory.trim().isEmpty()) {
      this.anchorParent = out.getParent().resolve("catchup-anchor");
    } else {
      this.anchorParent = Paths.get(directory.trim()).toAbsolutePath().normalize();
    }
    if (enabled && anchorParent.startsWith(out)) {
      throw new IllegalArgumentException(
          "catchUpSnapshot.directory must stay outside the output directory");
    }
  }

  /** Builds the anchor from the process-wide parameter; any unreadable config means disabled. */
  public static CatchUpSnapshotAnchor fromCommonParameter() {
    try {
      CommonParameter parameter = CommonParameter.getInstance();
      if (parameter == null || parameter.getStorage() == null) {
        return disabled();
      }
      Storage storage = parameter.getStorage();
      return new CatchUpSnapshotAnchor(storage.isCatchUpSnapshotEnabled(),
          storage.getCatchUpSnapshotAnchorBlocks(), storage.getCatchUpSnapshotAnchorIntervalMinutes(),
          storage.getCatchUpSnapshotExitGapSeconds(), storage.getCatchUpSnapshotReenterGapMinutes(),
          storage.getCatchUpSnapshotDirectory(), Paths.get(parameter.getOutputDirectory()),
          CopyMode.REFLINK);
    } catch (RuntimeException e) {
      logger.warn("Catch-up snapshot anchor disabled: config unreadable", e);
      return disabled();
    }
  }

  /** Visible for tests: build an anchor against an explicit output directory. */
  public static CatchUpSnapshotAnchor createForTest(boolean enabled, int anchorBlocks,
      int anchorIntervalMinutes, String directory, Path outputDirectory) {
    return createForTest(enabled, anchorBlocks, anchorIntervalMinutes, directory, outputDirectory,
        CopyMode.HYBRID_LINK);
  }

  static CatchUpSnapshotAnchor createForTest(boolean enabled, int anchorBlocks,
      int anchorIntervalMinutes, String directory, Path outputDirectory, CopyMode copyMode) {
    return new CatchUpSnapshotAnchor(enabled, anchorBlocks, anchorIntervalMinutes,
        60, 10, directory, outputDirectory, copyMode);
  }

  private static CatchUpSnapshotAnchor disabled() {
    return new CatchUpSnapshotAnchor(false, 0, 0, 60, 10, "", Paths.get("."),
        CopyMode.HYBRID_LINK);
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getAnchorBlocks() {
    return anchorBlocks;
  }

  public long getAnchorIntervalMs() {
    return anchorIntervalMs;
  }

  public long getExitGapMs() {
    return exitGapMs;
  }

  public long getReenterGapMs() {
    return reenterGapMs;
  }

  /**
   * Takes an anchor when the timer says the last one is older than
   * {@code anchorIntervalMinutes}. Runs on the push thread, so the walk pauses block
   * processing; it is scheduled only while catching up. Failures are logged and retried on
   * the next block; they never fail the block pipeline.
   */
  public void maybeAnchor(long checkpointHeight) {
    if (!enabled || checkpointHeight < 0) {
      return;
    }
    try {
      long last = lastAnchorUtcMs();
      if (last >= 0 && System.currentTimeMillis() - last < anchorIntervalMs) {
        return;
      }
      takeAnchor(checkpointHeight);
    } catch (IOException | RuntimeException e) {
      logger.error("Catch-up snapshot anchor failed at height {}", checkpointHeight, e);
    }
  }

  /**
   * Restores the output directory from the anchor when the previous run did not shut down cleanly.
   * Returns the restored height, or -1 when no restore happened (clean stop, disabled, or no
   * anchor was ever taken). A failed restore is fatal: starting on possibly torn state would be
   * worse than refusing to boot.
   */
  public synchronized long restoreIfUnclean() {
    if (!enabled) {
      return -1;
    }
    Path running = anchorParent.resolve(RUNNING);
    if (!Files.exists(running)) {
      return -1;
    }
    long height = anchorHeight();
    if (height < 0) {
      // Unclean stop before the first anchor ever completed: nothing trusted to restore from,
      // fall back to the normal retained-WAL recovery.
      logger.warn("Unclean shutdown with no valid catch-up anchor; keeping live state");
      deleteRunningQuietly();
      return -1;
    }
    try {
      logger.warn("Unclean shutdown detected; restoring output directory from anchor height {}",
          height);
      try (java.util.stream.Stream<Path> children = Files.list(outputDirectory)) {
        for (Path child : (Iterable<Path>) children::iterator) {
          if (!FileUtil.recursiveDelete(child.toString())) {
            throw new IOException("Failed to delete " + child + " before anchor restore");
          }
        }
      }
      copyTree(anchorParent.resolve(ANCHOR_DIR), outputDirectory);
      runSync();
      logger.warn("Catch-up anchor restore complete: height={}", height);
      deleteRunningQuietly();
      return height;
    } catch (IOException | RuntimeException e) {
      throw new IllegalStateException(
          "Catch-up anchor restore failed; refusing to start on possibly torn state", e);
    }
  }

  /** Marks the process as running; must be called once per startup, after restoreIfUnclean. */
  public void markRunning() {
    if (!enabled) {
      return;
    }
    try {
      Files.createDirectories(anchorParent);
      Files.write(anchorParent.resolve(RUNNING),
          ("pid=" + ProcessHandle.current().pid() + "\nutc=" + Instant.now() + "\n")
              .getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.warn("Failed to write catch-up RUNNING marker", e);
    }
  }

  /** Clears the running marker on a clean shutdown. */
  public void markCleanShutdown() {
    if (!enabled) {
      return;
    }
    deleteRunningQuietly();
  }

  private void takeAnchor(long height) throws IOException {
    long start = System.currentTimeMillis();
    Files.createDirectories(anchorParent);
    Path tmp = anchorParent.resolve(TMP_DIR);
    FileUtil.recursiveDelete(tmp.toString());
    Files.createDirectories(tmp);
    copyTree(outputDirectory, tmp);
    runSync();
    writeMarker(tmp.resolve(MARKER), height);
    Path anchor = anchorParent.resolve(ANCHOR_DIR);
    Path old = anchorParent.resolve(OLD_DIR);
    FileUtil.recursiveDelete(old.toString());
    if (Files.exists(anchor)) {
      Files.move(anchor, old);
    }
    Files.move(tmp, anchor);
    FileUtil.recursiveDelete(old.toString());
    runSync();
    lastAnchorUtcMs = System.currentTimeMillis();
    logger.info("Catch-up snapshot anchor: height={}, costMs={}", height,
        System.currentTimeMillis() - start);
  }

  private long anchorHeight() {
    List<String> lines = markerLines();
    if (lines == null) {
      return -1;
    }
    for (String line : lines) {
      if (line.startsWith("height=")) {
        try {
          return Long.parseLong(line.substring("height=".length()).trim());
        } catch (NumberFormatException e) {
          logger.warn("Unreadable catch-up anchor height", e);
          return -1;
        }
      }
    }
    return -1;
  }

  private long lastAnchorUtcMs() {
    long cached = lastAnchorUtcMs;
    if (cached > 0) {
      return cached;
    }
    List<String> lines = markerLines();
    if (lines == null) {
      return -1;
    }
    for (String line : lines) {
      if (line.startsWith("utc=")) {
        try {
          long parsed = Instant.parse(line.substring("utc=".length()).trim()).toEpochMilli();
          lastAnchorUtcMs = parsed;
          return parsed;
        } catch (RuntimeException e) {
          logger.warn("Unreadable catch-up anchor time", e);
          return -1;
        }
      }
    }
    return -1;
  }

  private List<String> markerLines() {
    Path marker = anchorParent.resolve(ANCHOR_DIR).resolve(MARKER);
    if (!Files.isRegularFile(marker)) {
      return null;
    }
    try {
      return Files.readAllLines(marker, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.warn("Unreadable catch-up anchor marker", e);
      return null;
    }
  }

  private void deleteRunningQuietly() {
    try {
      Files.deleteIfExists(anchorParent.resolve(RUNNING));
    } catch (IOException e) {
      logger.warn("Failed to delete catch-up RUNNING marker", e);
    }
  }

  private static void writeMarker(Path marker, long height) throws IOException {
    Files.write(marker,
        ("height=" + height + "\nutc=" + Instant.now() + "\n").getBytes(StandardCharsets.UTF_8));
    try (FileChannel channel = FileChannel.open(marker, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
    try (FileChannel dir = FileChannel.open(marker.getParent(), StandardOpenOption.READ)) {
      dir.force(true);
    } catch (IOException e) {
      // Directory fsync is not supported on every platform; the file itself is already forced.
      logger.debug("Catch-up anchor marker directory fsync skipped", e);
    }
  }

  private void copyTree(Path src, Path dst) throws IOException {
    switch (effectiveCopyMode()) {
      case REFLINK:
        copyTreeReflink(src, dst);
        break;
      case HYBRID_LINK:
      default:
        copyTreeHybrid(src, dst);
        break;
    }
  }

  private void copyTreeReflink(Path src, Path dst) throws IOException {
    List<String> command = reflinkCommand;
    if (command == null) {
      throw new IOException("Catch-up anchor reflink command unresolved");
    }
    List<String> fullCommand = new ArrayList<>(command);
    fullCommand.add(src.toString() + "/.");
    fullCommand.add(dst.toString());
    Process process = new ProcessBuilder(fullCommand).redirectErrorStream(true).start();
    String output;
    int rc;
    try {
      output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      rc = process.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Catch-up anchor copy interrupted", e);
    }
    if (rc != 0) {
      String tail = output.length() > 400 ? output.substring(output.length() - 400) : output;
      throw new IOException("Catch-up anchor copy failed rc=" + rc + ": " + tail);
    }
  }

  private void copyTreeHybrid(Path src, Path dst) throws IOException {
    Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException {
        Files.createDirectories(dst.resolve(src.relativize(dir).toString()));
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        Path target = dst.resolve(src.relativize(file).toString());
        if (isImmutable(file.getFileName().toString())) {
          try {
            Files.createLink(target, file);
            return FileVisitResult.CONTINUE;
          } catch (IOException | UnsupportedOperationException | SecurityException e) {
            // Cross-device or unsupported: fall through to a real copy.
            logger.debug("Catch-up anchor hardlink failed for {}; copying", file, e);
          }
        }
        copyFile(file, target);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static boolean isImmutable(String fileName) {
    return fileName.toLowerCase(Locale.ROOT).endsWith(IMMUTABLE_LINK_SUFFIX);
  }

  private static void copyFile(Path src, Path dst) throws IOException {
    try {
      Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
    } catch (IOException e) {
      Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private volatile CopyMode resolvedCopyMode;
  private volatile List<String> reflinkCommand;

  private CopyMode effectiveCopyMode() {
    CopyMode mode = resolvedCopyMode;
    if (mode == null) {
      mode = probeCopyMode();
      resolvedCopyMode = mode;
    }
    return mode;
  }

  /**
   * Probes cp clone support on the anchor filesystem itself (FICLONE via GNU
   * {@code --reflink=always}, or APFS {@code clonefile} via BSD {@code -c}); falls back to
   * hardlink-hybrid when neither works. A plain {@code --reflink=auto} probe is not enough:
   * it silently degrades to a full copy, which is exactly the cost this mode exists to avoid.
   */
  private CopyMode probeCopyMode() {
    if (copyMode == CopyMode.HYBRID_LINK) {
      return CopyMode.HYBRID_LINK;
    }
    Path probeDir = null;
    try {
      Files.createDirectories(anchorParent);
      probeDir = Files.createTempDirectory(anchorParent, "probe");
      Path source = probeDir.resolve("a");
      Path target = probeDir.resolve("b");
      Files.write(source, "probe".getBytes(StandardCharsets.UTF_8));
      List<String> gnu = List.of("cp", "-a", "--reflink=always",
          source.toString(), target.toString());
      if (cloneCopyWorks(gnu, target)) {
        reflinkCommand = List.of("cp", "-a", "--reflink=auto");
        logger.info("Catch-up anchor copy mode: reflink (FICLONE)");
        return CopyMode.REFLINK;
      }
      List<String> apfs = List.of("cp", "-a", "-c", source.toString(), target.toString());
      if (cloneCopyWorks(apfs, target)) {
        reflinkCommand = List.of("cp", "-a", "-c");
        logger.info("Catch-up anchor copy mode: clonefile (APFS)");
        return CopyMode.REFLINK;
      }
      logger.warn("No filesystem clone support on anchor path; using hardlink-hybrid copy");
      return CopyMode.HYBRID_LINK;
    } catch (IOException | RuntimeException e) {
      logger.warn("Catch-up anchor copy probe failed; using hardlink-hybrid copy", e);
      return CopyMode.HYBRID_LINK;
    } finally {
      if (probeDir != null) {
        FileUtil.recursiveDelete(probeDir.toString());
      }
    }
  }

  private static boolean cloneCopyWorks(List<String> command, Path target) {
    try {
      Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
      return process.waitFor() == 0 && Files.exists(target);
    } catch (IOException e) {
      logger.debug("cp clone probe failed: {}", command, e);
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static void runSync() {
    try {
      new ProcessBuilder("sync").start().waitFor();
    } catch (IOException e) {
      logger.warn("sync after catch-up anchor copy failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
