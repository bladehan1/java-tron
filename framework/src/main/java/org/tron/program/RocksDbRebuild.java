package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.core.config.args.Args;

/** Rebuilds one RocksDB database so every output SST uses the active table options. */
public final class RocksDbRebuild {

  private static final int BATCH_SIZE = 10_000;

  private RocksDbRebuild() {
  }

  public static void main(String[] args) {
    int exitCode = execute(args, System.out, System.err);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int execute(String[] args, PrintStream output, PrintStream error) {
    Options options = new Options();
    JCommander commander = JCommander.newBuilder()
        .addObject(options)
        .programName("RocksDbRebuild")
        .build();
    try {
      commander.parse(args);
      if (options.help) {
        commander.usage();
        return 0;
      }
      options.validate();
      Args.setParam(options.nodeArgs(), "config.conf");
      long start = System.nanoTime();
      long entries = options.compactExisting ? compactExisting(options) : rebuild(options);
      double elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
      output.printf("rebuilt database=%s entries=%d elapsed_seconds=%.3f%n",
          options.database, entries, elapsedSeconds);
      return 0;
    } catch (ParameterException | IllegalArgumentException e) {
      error.println(e.getMessage());
      commander.usage();
      return 2;
    } catch (Exception e) {
      error.println("RocksDB rebuild failed: " + e.getMessage());
      return 1;
    } finally {
      Args.clearParam();
    }
  }

  private static long rebuild(Options options) throws Exception {
    RocksDbDataSourceImpl target = new RocksDbDataSourceImpl(
        options.targetDirectory, options.database);
    long entries = 0;
    Map<byte[], byte[]> batch = new LinkedHashMap<>(BATCH_SIZE);
    try (org.rocksdb.Options sourceOptions = RocksDbSettings.getOptionsByDbName(options.database);
        RocksDB source = RocksDB.openReadOnly(sourceOptions, options.sourcePath().toString());
        ReadOptions readOptions = new ReadOptions().setFillCache(false);
        RocksIterator iterator = source.newIterator(readOptions)) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        batch.put(iterator.key(), iterator.value());
        entries++;
        if (batch.size() == BATCH_SIZE) {
          target.updateByBatch(batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        target.updateByBatch(batch);
      }
      iterator.status();
      target.getDatabase().compactRange();
      verifySameContent(source, target, entries);
      return entries;
    } finally {
      target.closeDB();
    }
  }

  private static long compactExisting(Options options) throws Exception {
    RocksDbDataSourceImpl target = new RocksDbDataSourceImpl(
        options.targetDirectory, options.database);
    try (org.rocksdb.Options sourceOptions = RocksDbSettings.getOptionsByDbName(options.database);
        RocksDB source = RocksDB.openReadOnly(sourceOptions, options.sourcePath().toString())) {
      forceCompactBottommost(target);
      return verifySameContent(source, target, -1);
    } finally {
      target.closeDB();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void forceCompactBottommost(RocksDbDataSourceImpl target) throws Exception {
    Class<?> optionsClass = Class.forName("org.rocksdb.CompactRangeOptions");
    Class<? extends Enum> bottommostClass = (Class<? extends Enum>) Class.forName(
        "org.rocksdb.CompactRangeOptions$BottommostLevelCompaction");
    Object options = optionsClass.getConstructor().newInstance();
    Object force = Enum.valueOf(bottommostClass, "kForce");
    try {
      optionsClass.getMethod("setBottommostLevelCompaction", bottommostClass)
          .invoke(options, force);
      optionsClass.getMethod("setExclusiveManualCompaction", boolean.class)
          .invoke(options, true);
      optionsClass.getMethod("setMaxSubcompactions", int.class).invoke(options, 8);
      Method compactRange = target.getDatabase().getClass().getMethod("compactRange",
          Class.forName("org.rocksdb.ColumnFamilyHandle"), byte[].class, byte[].class,
          optionsClass);
      compactRange.invoke(target.getDatabase(), target.getDatabase().getDefaultColumnFamily(),
          null, null, options);
    } finally {
      ((AutoCloseable) options).close();
    }
  }

  private static long verifySameContent(RocksDB source,
      RocksDbDataSourceImpl target, long expectedEntries) throws Exception {
    long compared = 0;
    try (ReadOptions sourceOptions = new ReadOptions().setFillCache(false);
        ReadOptions targetOptions = new ReadOptions().setFillCache(false);
        RocksIterator sourceIterator = source.newIterator(sourceOptions);
        RocksIterator targetIterator = target.getDatabase().newIterator(targetOptions)) {
      sourceIterator.seekToFirst();
      targetIterator.seekToFirst();
      while (sourceIterator.isValid() && targetIterator.isValid()) {
        if (!Arrays.equals(sourceIterator.key(), targetIterator.key())
            || !Arrays.equals(sourceIterator.value(), targetIterator.value())) {
          throw new IllegalStateException("Rebuilt content differs at entry " + compared);
        }
        compared++;
        sourceIterator.next();
        targetIterator.next();
      }
      sourceIterator.status();
      targetIterator.status();
      if (sourceIterator.isValid() || targetIterator.isValid()
          || (expectedEntries >= 0 && compared != expectedEntries)) {
        throw new IllegalStateException("Rebuilt entry count differs: copied=" + expectedEntries
            + ", compared=" + compared);
      }
      return compared;
    }
  }

  static final class Options {
    @Parameter(names = {"-s", "--source-directory"},
        description = "Existing directory containing the source database.")
    private String sourceDirectory;

    @Parameter(names = {"-t", "--target-directory"},
        description = "Existing empty directory that will contain the rebuilt database.")
    private String targetDirectory;

    @Parameter(names = "--database", description = "Database name to rebuild.")
    private String database;

    @Parameter(names = "--compact-existing",
        description = "Force-compact an existing target copy, including bottommost SST files.")
    private boolean compactExisting;

    @Parameter(names = {"-c", "--config"}, description = "Node config file.")
    private String config = "config.conf";

    @Parameter(names = "--rocksdb-config",
        description = "RocksDB profile to materialize into every output SST file.")
    private String rocksDbConfig;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private void validate() {
      requireDirectory(sourceDirectory, "--source-directory");
      requireDirectory(targetDirectory, "--target-directory");
      if (database == null || database.trim().isEmpty()) {
        throw new ParameterException("--database is required");
      }
      Path source = Paths.get(sourceDirectory, database).toAbsolutePath().normalize();
      Path target = Paths.get(targetDirectory, database).toAbsolutePath().normalize();
      if (!Files.isRegularFile(source.resolve("CURRENT"))) {
        throw new ParameterException("Existing RocksDB source is required: " + source);
      }
      if (compactExisting && !Files.isRegularFile(target.resolve("CURRENT"))) {
        throw new ParameterException("Existing RocksDB target is required: " + target);
      }
      if (!compactExisting && Files.exists(target)) {
        throw new ParameterException("Target database must not exist: " + target);
      }
    }

    private void requireDirectory(String value, String optionName) {
      if (value == null || value.trim().isEmpty() || !Files.isDirectory(Paths.get(value))) {
        throw new ParameterException(optionName + " must be an existing directory");
      }
    }

    private String[] nodeArgs() {
      List<String> args = new ArrayList<>();
      args.add("-c");
      args.add(config);
      if (rocksDbConfig != null) {
        args.add("--rocksdb-config");
        args.add(rocksDbConfig);
      }
      return args.toArray(new String[0]);
    }

    private Path sourcePath() {
      return Paths.get(sourceDirectory, database).toAbsolutePath().normalize();
    }
  }
}
