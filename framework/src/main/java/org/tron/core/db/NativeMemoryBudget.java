package org.tron.core.db;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.math.StrictMathWrapper;
import org.tron.common.utils.StorageUtils;
import org.tron.core.config.args.Storage;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.stateroot.PathStateNativeBudget;

/**
 * Estimated process memory budget, startup diagnostics (design v2 ch08 §3.8, E9, WP8-7).
 *
 * <pre>
 * A_cap = Xmx + D_max + Meta_max + Code_max + g * Xmx + S_threads + J_misc
 *       + sum_DB (cache_cap + memtable_cap) + sum_DB max(0, P_pin - cache_cap)
 *       + N_ovh + F_frag                          (F_frag = 10% of the native items)
 * verdict: A_cap <= memory.high - R_pc ? OK : OVER_BUDGET (WARN only; resource risk, not a
 * correctness issue). Without a readable cgroup v2 memory.high the verdict is UNKNOWN and only
 * the items are printed.
 * </pre>
 *
 * <p>Runs only with an Archive/PathState feature on (G3). LevelDB memtables count twice
 * ({@code mem_} plus {@code imm_}); RocksDB memtables count {@code write_buffer_size x
 * max_write_buffer_number}; Chainbase RocksDB counts the effective 8 MiB per-database cache and
 * default 64 MiB x 2 memtables (its table config is materialized before its setters, §3.6);
 * Chainbase table-reader memory is not bounded here and must be measured
 * ({@code rocksdb.estimate-table-readers-mem}). PathState overlay and node cache live in the
 * heap (part of Xmx).</p>
 */
@Slf4j(topic = "DB")
public final class NativeMemoryBudget {

  static final long GIB = 1024L * 1024 * 1024;
  static final long MIB = 1024L * 1024;
  /** S_threads + J_misc initial value (§3.8.1). */
  static final long THREADS_AND_MISC = GIB / 2;
  /** N_ovh initial value (§3.8.1). */
  static final long NATIVE_OVERHEAD = GIB / 2;
  static final double FRAGMENTATION = 0.10d;
  /** RocksJava default block cache when the table config is materialized too early (§3.6). */
  static final long CHAINBASE_ROCKSDB_EFFECTIVE_CACHE = 8L * MIB;
  static final long ROCKSDB_DEFAULT_WRITE_BUFFER = 64L * MIB;
  static final int ROCKSDB_DEFAULT_WRITE_BUFFERS = 2;
  /** Unset MaxMetaspaceSize: counted with this placeholder until NMT calibrates it. */
  static final long METASPACE_PLACEHOLDER = 256L * MIB;

  /** JVM inputs. */
  static final class Jvm {
    final long maxHeap;
    final long maxDirect;
    final long maxMetaspace;
    final long codeCache;
    final String gc;
    final double gcFactor;

    Jvm(long maxHeap, long maxDirect, long maxMetaspace, long codeCache, String gc,
        double gcFactor) {
      this.maxHeap = maxHeap;
      this.maxDirect = maxDirect;
      this.maxMetaspace = maxMetaspace;
      this.codeCache = codeCache;
      this.gc = gc;
      this.gcFactor = gcFactor;
    }
  }

  /** One native database configured or estimated allocation. */
  static final class Db {
    final String family;
    final long cacheBytes;
    final long memtableBytes;
    final long pinnedBytes;

    Db(String family, long cacheBytes, long memtableBytes, long pinnedBytes) {
      this.family = family;
      this.cacheBytes = cacheBytes;
      this.memtableBytes = memtableBytes;
      this.pinnedBytes = pinnedBytes;
    }

    long pinnedOverflow() {
      return StrictMathWrapper.max(0L, pinnedBytes - cacheBytes);
    }
  }

  /** The computed budget. */
  static final class Result {
    final Map<String, Long> items;
    final long total;
    final Long memoryHigh;
    final Long memoryMax;
    final long pageCacheReserve;
    final String verdict;

    Result(Map<String, Long> items, long total, Long memoryHigh, Long memoryMax,
        long pageCacheReserve, String verdict) {
      this.items = Collections.unmodifiableMap(items);
      this.total = total;
      this.memoryHigh = memoryHigh;
      this.memoryMax = memoryMax;
      this.pageCacheReserve = pageCacheReserve;
      this.verdict = verdict;
    }

    String describe() {
      StringBuilder text = new StringBuilder("memory={verdict=").append(verdict)
          .append(", anonCap=").append(gib(total))
          .append(", memoryHigh=").append(memoryHigh == null ? "unknown" : gib(memoryHigh))
          .append(", memoryMax=").append(memoryMax == null ? "unknown" : gib(memoryMax))
          .append(", pageCacheReserve=").append(gib(pageCacheReserve)).append(", items={");
      String separator = "";
      for (Map.Entry<String, Long> item : items.entrySet()) {
        text.append(separator).append(item.getKey()).append('=').append(gib(item.getValue()));
        separator = ", ";
      }
      return text.append("}}").toString();
    }
  }

  private NativeMemoryBudget() {
  }

  /** Pure budget equation (§3.8.1). */
  static Result compute(Jvm jvm, List<Db> databases, Long memoryHigh, Long memoryMax,
      long pageCacheReserve) {
    Map<String, Long> items = new LinkedHashMap<>();
    items.put("heap(Xmx)", jvm.maxHeap);
    items.put("directMax", jvm.maxDirect);
    items.put("metaspaceMax", jvm.maxMetaspace);
    items.put("codeCache", jvm.codeCache);
    items.put("gc(" + jvm.gc + ",g=" + jvm.gcFactor + ")", (long) (jvm.gcFactor * jvm.maxHeap));
    items.put("threadsAndJvmMisc", THREADS_AND_MISC);
    Map<String, long[]> families = new LinkedHashMap<>();
    for (Db db : databases) {
      long[] sums = families.computeIfAbsent(db.family, ignored -> new long[3]);
      sums[0] += db.cacheBytes;
      sums[1] += db.memtableBytes;
      sums[2] += db.pinnedOverflow();
    }
    long nativeItems = 0;
    for (Map.Entry<String, long[]> family : families.entrySet()) {
      items.put(family.getKey() + ".cache", family.getValue()[0]);
      items.put(family.getKey() + ".memtable", family.getValue()[1]);
      if (family.getValue()[2] > 0) {
        items.put(family.getKey() + ".pinnedOverflow", family.getValue()[2]);
      }
      nativeItems += family.getValue()[0] + family.getValue()[1] + family.getValue()[2];
    }
    items.put("nativeOverhead", NATIVE_OVERHEAD);
    long fragmentation = (long) (FRAGMENTATION * nativeItems);
    items.put("mallocFragmentation", fragmentation);
    long total = 0;
    for (long value : items.values()) {
      total += value;
    }
    String verdict = memoryHigh == null ? "UNKNOWN"
        : total <= memoryHigh - pageCacheReserve ? "OK" : "OVER_BUDGET";
    return new Result(new LinkedHashMap<>(items), total, memoryHigh, memoryMax, pageCacheReserve,
        verdict);
  }

  static Db levelDb(String family, long cacheBytes, long writeBufferBytes) {
    return new Db(family, cacheBytes, 2L * writeBufferBytes, 0L);
  }

  static Db rocksDb(String family, long cacheBytes, long writeBufferBytes, int writeBuffers,
      long pinnedBytes) {
    return new Db(family, cacheBytes, writeBufferBytes * writeBuffers, pinnedBytes);
  }

  /** Startup check: logs every item and the verdict; never refuses startup. */
  static Result checkAtStartup(Storage storage, Path outputDirectory) {
    List<Db> databases = new ArrayList<>();
    boolean chainRocks = "ROCKSDB".equalsIgnoreCase(storage.getDbEngine());
    for (String name : chainbaseDatabases(outputDirectory.resolve(storage.getDbDirectory()))) {
      if (chainRocks) {
        databases.add(rocksDb("chainbase", CHAINBASE_ROCKSDB_EFFECTIVE_CACHE,
            ROCKSDB_DEFAULT_WRITE_BUFFER, ROCKSDB_DEFAULT_WRITE_BUFFERS, 0L));
      } else {
        org.iq80.leveldb.Options options = StorageUtils.getOptionsByDbName(name);
        databases.add(levelDb("chainbase", options.cacheSize(), options.writeBufferSize()));
      }
    }
    if ((storage.isPathStateRootEnabled() || storage.isCommonCheckpointEnabled())
        && storage.getPathStateRootDbSettings() != null) {
      boolean rocks = true;
      for (PathStateNativeBudget.Entry entry : PathStateNativeBudget.entries(
          storage.getPathStateRootDbSettings(), rocks,
          Boolean.getBoolean("tron.pathstate.largeReadCacheBenchmark"))) {
        databases.add(new Db("pathState", entry.getCacheBytes(), entry.getMemtableBytes(),
            entry.getPinnedBytes()));
      }
    }
    if (storage.isStateArchiveEnabled() || storage.isCommonCheckpointEnabled()) {
      NativeDbConfig serving = storage.getStateArchiveServingIndexDbSettings() == null
          ? NativeDbConfig.large() : storage.getStateArchiveServingIndexDbSettings();
      databases.add(rocksDb("serving", serving.getCacheSize(), serving.getWriteBufferSize(),
          serving.getMaxWriteBufferNumber(), 0L));
    }
    if (storage.isCommonCheckpointEnabled()) {
      NativeDbConfig redo = storage.getStateArchiveServingIndexDbSettings() == null
          ? NativeDbConfig.large() : storage.getStateArchiveServingIndexDbSettings();
      databases.add(rocksDb("common-redo", redo.getCacheSize(), redo.getWriteBufferSize(),
          redo.getMaxWriteBufferNumber(), 0L));
    }
    org.tron.core.config.args.StorageConfig.StateArchiveHotStoreConfig hot =
        storage.getStateArchiveHotStoreSettings();
    boolean append = storage.getStateArchiveAppendFileSettings() != null
        && storage.getStateArchiveAppendFileSettings().isEnabled();
    if (storage.isCommonCheckpointEnabled() && hot != null && hot.isEnabled() && !append) {
      NativeDbConfig config = hot.getDbSettings();
      long caches = config.getCacheSize() * (hot.getMaxFrozenGenerations() + 1L);
      int columns = org.tron.core.db2.archive.ArchiveStoreScope.getStateDatabases().size() + 2;
      long memtables = (long) config.getWriteBufferSize() * config.getMaxWriteBufferNumber()
          * columns;
      databases.add(new Db("legacy-hot", caches, memtables, 0));
    }
    Long[] cgroup = readCgroup(Paths.get("/proc/self/cgroup"), Paths.get("/sys/fs/cgroup"));
    Result result = compute(readJvm(), databases, cgroup[0], cgroup[1],
        4L * GIB);
    if ("OVER_BUDGET".equals(result.verdict)) {
      logger.warn("Native memory budget OVER_BUDGET (design v2 ch08 §3.8): {}",
          result.describe());
    } else {
      logger.info("Estimated native memory budget (table readers require measurement): {}",
          result.describe());
    }
    return result;
  }

  static List<String> chainbaseDatabases(Path databaseDirectory) {
    List<String> names = new ArrayList<>();
    if (!Files.isDirectory(databaseDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return names;
    }
    try (Stream<Path> entries = Files.list(databaseDirectory)) {
      entries.filter(path -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
          .map(path -> path.getFileName().toString())
          .filter(name -> !name.startsWith("checkpoint") && !name.startsWith("."))
          .sorted().forEach(names::add);
    } catch (IOException | RuntimeException unreadable) {
      names.clear();
    }
    return names;
  }

  /** {memory.high, memory.max} of this process's cgroup v2; null for unknown or "max". */
  static Long[] readCgroup(Path procSelfCgroup, Path cgroupRoot) {
    Long[] values = new Long[2];
    try {
      if (!Files.isRegularFile(procSelfCgroup)) {
        return values;
      }
      String relative = null;
      for (String line : Files.readAllLines(procSelfCgroup, StandardCharsets.UTF_8)) {
        if (line.startsWith("0::")) {
          relative = line.substring(3);
        }
      }
      if (relative == null) {
        return values;
      }
      Path group = cgroupRoot.resolve(relative.startsWith("/") ? relative.substring(1)
          : relative);
      values[0] = readLimit(group.resolve("memory.high"));
      values[1] = readLimit(group.resolve("memory.max"));
    } catch (IOException | RuntimeException unreadable) {
      values[0] = null;
      values[1] = null;
    }
    return values;
  }

  private static Long readLimit(Path file) throws IOException {
    if (!Files.isRegularFile(file)) {
      return null;
    }
    String value = new String(Files.readAllBytes(file), StandardCharsets.US_ASCII).trim();
    return value.isEmpty() || "max".equals(value) ? null : Long.parseLong(value);
  }

  /** Effective JVM limits; MaxDirectMemorySize 0 means "equal to the max heap" (§3.8.1). */
  static Jvm readJvm() {
    long maxHeap = Runtime.getRuntime().maxMemory();
    long direct = vmOption("MaxDirectMemorySize", 0L);
    long metaspace = vmOption("MaxMetaspaceSize", -1L);
    long codeCache = vmOption("ReservedCodeCacheSize", 240L * MIB);
    String gc = "unknown";
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      gc = gcName(bean.getName(), gc);
    }
    return effectiveJvm(maxHeap, direct, metaspace, codeCache, gc);
  }

  static String gcName(String beanName, String current) {
    String name = beanName.toLowerCase(Locale.ROOT);
    if (name.startsWith("g1")) {
      return "G1";
    }
    if (name.startsWith("zgc")) {
      return "ZGC";
    }
    if (name.contains("concurrentmarksweep") || name.equals("parnew")) {
      return "CMS";
    }
    if (name.startsWith("ps ")) {
      return "Parallel";
    }
    if (name.startsWith("shenandoah")) {
      return "Shenandoah";
    }
    return "unknown".equals(current) && (name.equals("copy") || name.equals("marksweepcompact"))
        ? "Serial" : current;
  }

  static Jvm effectiveJvm(long maxHeap, long maxDirectOption, long maxMetaspaceOption,
      long codeCache, String gc) {
    long direct = maxDirectOption <= 0 ? maxHeap : maxDirectOption;
    long metaspace = maxMetaspaceOption <= 0 || maxMetaspaceOption > 64L * GIB
        ? METASPACE_PLACEHOLDER : maxMetaspaceOption;
    // g: G1 initial 0.05 (to be calibrated with NMT); other collectors use the same placeholder.
    return new Jvm(maxHeap, direct, metaspace, codeCache, gc, 0.05d);
  }

  private static long vmOption(String name, long fallback) {
    try {
      com.sun.management.HotSpotDiagnosticMXBean bean = ManagementFactory
          .getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
      String value = bean.getVMOption(name).getValue();
      return Long.parseUnsignedLong(value) < 0 ? Long.MAX_VALUE : Long.parseLong(value);
    } catch (RuntimeException | LinkageError unavailable) {
      return fallback;
    }
  }

  private static String gib(long bytes) {
    return String.format(Locale.ROOT, "%.2fGiB", bytes / (double) GIB);
  }
}
