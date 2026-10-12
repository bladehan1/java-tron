package org.tron.core.db2.core;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.common.WrappedByteArray;

/**
 * Out-of-block write guard, phase one (design v2 ch02 §3.11.2, WP-02-11, X2-08, invariant I19).
 *
 * <p>Once a Common baseline exists, the state of the exact-27 Chainbase roots may change only
 * through Common materialization (the explicit entry
 * {@code SnapshotRoot.applyCheckpointMutations}, which does not pass here). Any other
 * direct root write — a constructor-time default of a new DynamicProperties key, a startup
 * migration helper, or an unknown path that bypasses the block session — would never enter any
 * {@code RD(B)} and would silently change the answers for every historical {@code T} before it.
 * Such a write is refused before it reaches the database; a write whose bytes equal the current
 * value is admitted (e.g. {@code updateDynamicStoreByConfig} restating the same value).
 *
 * <p>The guard is enabled by the Common traces on disk ({@code COMMON_BASELINE},
 * {@code CHAINBASE_CURRENT} or {@code COMMON_CHECKPOINT}), not by the configured form, so it also
 * covers Spring construction on an archive directory started with form OFF by an entry that
 * skipped the file-level form guard (ch01 §3.8.4). It probes those three files itself on the
 * first exact-27 root write. Without traces (an upstream-equivalent directory) it never
 * activates and costs one file probe per process (D3).
 *
 * <p>Phase two (§3.11.3, WP-02-12): on a node with a Common baseline the registered startup
 * writers run inside the maintenance layer that {@link SnapshotManager#openMaintenanceLayer()}
 * places above the roots, so they never reach this guard; outside its write window the layer
 * applies the same rule to the visible state ({@code SnapshotImpl}).
 */
public final class OutOfBlockWriteGuard {

  private static final Logger logger = LoggerFactory.getLogger("DB");

  /** Common directory → whether Common traces were found (or Common was established). */
  private static final Map<Path, Boolean> ACTIVE = new ConcurrentHashMap<>();

  private OutOfBlockWriteGuard() {
  }

  /** Raised instead of writing; the node must stop (startup) or fail-stop (runtime). */
  public static final class OutOfBlockWriteException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final byte[] key;

    OutOfBlockWriteException(String dbName, byte[] key) {
      super("STATE_ARCHIVE_STARTUP_FAILURE code=OUT_OF_BLOCK_WRITE member=database detail="
          + "write to exact-27 Store " + dbName + " key=" + hex(key) + " after the Common "
          + "baseline exists; it would bypass History (design v2 ch02 §3.11.2, X2-08). Only "
          + "the startup migrations and DynamicProperties constructor defaults are captured by "
          + "the maintenance layer (§3.11.3); classify this writer or run the previous binary");
      this.dbName = dbName;
      this.key = Arrays.copyOf(key, key.length);
    }

    public String getDbName() {
      return dbName;
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }
  }

  /** Current-value reader of the root database (never through any cache or layer). */
  @FunctionalInterface
  interface CurrentValue {
    byte[] get(byte[] key);
  }

  /** Single put ({@code value} non-null) or delete ({@code value == null}). */
  static void check(String dbName, byte[] key, byte[] value, CurrentValue current) {
    if (!applies(dbName)) {
      return;
    }
    byte[] existing = current.get(key);
    if (!sameState(existing, value)) {
      throw refuse(dbName, key);
    }
  }

  /** Batch merge; values of {@code null} bytes are deletes. */
  static void checkBatch(String dbName, Map<WrappedByteArray, WrappedByteArray> batch,
      CurrentValue current) {
    if (!applies(dbName)) {
      return;
    }
    for (Map.Entry<WrappedByteArray, WrappedByteArray> entry : batch.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = entry.getValue() == null ? null : entry.getValue().getBytes();
      if (!sameState(current.get(key), value)) {
        throw refuse(dbName, key);
      }
    }
  }

  /**
   * Common became the authority in this process (bootstrap B8 published COMMON_BASELINE): from
   * now on the guard is active for {@code commonDirectory} without another probe.
   */
  public static void markCommonEstablished(Path commonDirectory) {
    ACTIVE.put(normalize(commonDirectory), Boolean.TRUE);
  }

  /** Whether the guard is active for the configured Common directory of this process. */
  public static boolean isActive() {
    Path directory = configuredCommonDirectory();
    return directory != null && ACTIVE.computeIfAbsent(directory,
        OutOfBlockWriteGuard::hasCommonTraces);
  }

  /**
   * Whether {@code DynamicPropertiesStore} registers its constructor-time defaults as intents
   * instead of writing them (design v2 ch02 §3.11.3, WP-02-12): only with Common configured and
   * a {@code COMMON_BASELINE} on disk, exactly when {@code Manager} runs Common startup recovery
   * and opens the maintenance layer. Otherwise (form OFF, first bootstrap) the defaults are
   * written as upstream does; with OFF configured on a directory with Common traces they
   * still reach the phase-one guard.
   */
  public static boolean defersConstructorDefaults() {
    CommonParameter parameter = CommonParameter.getInstance();
    if (parameter == null || parameter.getStorage() == null
        || parameter.getOutputDirectory() == null
        || !parameter.getStorage().isCommonCheckpointEnabled()
        || parameter.getStorage().getCommonCheckpointDirectory() == null) {
      return false;
    }
    Path directory = Paths.get(parameter.getOutputDirectory())
        .resolve(parameter.getStorage().getCommonCheckpointDirectory()).normalize();
    return Files.isRegularFile(directory.resolve(CommonCheckpointBaselineFile.FILE_NAME),
        LinkOption.NOFOLLOW_LINKS);
  }

  /** Test support: forget every cached probe result. */
  static void reset() {
    ACTIVE.clear();
  }

  private static boolean applies(String dbName) {
    return dbName != null && ArchiveStoreScope.isStateDatabase(dbName) && isActive();
  }

  private static boolean sameState(byte[] existing, byte[] proposed) {
    if (existing == null || proposed == null) {
      return existing == proposed;
    }
    return Arrays.equals(existing, proposed);
  }

  static OutOfBlockWriteException refuse(String dbName, byte[] key) {
    OutOfBlockWriteException failure = new OutOfBlockWriteException(dbName, key);
    logger.error(failure.getMessage());
    return failure;
  }

  private static Path configuredCommonDirectory() {
    CommonParameter parameter = CommonParameter.getInstance();
    if (parameter == null || parameter.getStorage() == null
        || parameter.getOutputDirectory() == null) {
      return null;
    }
    String common = parameter.getStorage().getCommonCheckpointDirectory();
    if (common == null || common.trim().isEmpty()) {
      common = "common-checkpoint";
    }
    return normalize(Paths.get(parameter.getOutputDirectory(), common));
  }

  private static Path normalize(Path path) {
    return path.toAbsolutePath().normalize();
  }

  private static boolean hasCommonTraces(Path directory) {
    boolean traces = isFile(directory.resolve(CommonCheckpointBaselineFile.FILE_NAME))
        || isFile(directory.resolve(ChainbaseCheckpointMaterializer.CURRENT_FILE))
        || isFile(directory.resolve(CommonCheckpointFile.FILE_NAME));
    if (traces) {
      logger.info("Out-of-block write guard active: Common traces found in {}", directory);
    }
    return traces;
  }

  private static boolean isFile(Path path) {
    return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte current : value) {
      encoded.append(Character.forDigit(current >>> 4 & 0xf, 16));
      encoded.append(Character.forDigit(current & 0xf, 16));
    }
    return encoded.toString();
  }
}
