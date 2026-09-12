package org.tron.core.db2.stateroot;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Command-line entry for verifying one offline physical-store reverse window. */
public final class PathStatePhysicalOracleTool {

  private static final int DEFAULT_ROWS_PER_FLUSH = 100_000;

  private PathStatePhysicalOracleTool() {
  }

  public static void main(String[] args) throws Exception {
    PathStatePhysicalOracle.Result result = run(args);
    System.out.println("PATH_STATE_ORACLE_OK"
        + " current=" + result.getCurrent().getBlockNumber()
        + " oldest=" + result.getOldest().getBlockNumber()
        + " blocks=" + result.getBlockCount()
        + " rows=" + result.getRowCount());
  }

  static PathStatePhysicalOracle.Result run(String[] args) throws IOException {
    Map<String, String> options = parse(args);
    Path root = Paths.get(require(options, "--root"));
    Path scratch = Paths.get(require(options, "--scratch"));
    int blocks = positiveInt(require(options, "--blocks"), "--blocks");
    int rowsPerFlush = options.containsKey("--rows-per-flush")
        ? positiveInt(options.get("--rows-per-flush"), "--rows-per-flush")
        : DEFAULT_ROWS_PER_FLUSH;
    Engine engine;
    try {
      engine = Engine.valueOf(options.getOrDefault("--engine", Engine.ROCKSDB.name()));
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("unsupported physical oracle engine", invalid);
    }
    PathStateParticipantScope scope = new PathStateCanonicalizer().participantScope();
    try (PathStatePhysicalStoreSet stores = PathStatePhysicalStoreSet.openExisting(root, scope,
        engine)) {
      PathStatePhysicalOracleWindow window = stores.loadOracleWindow(blocks,
          PathStateLayerLimits.defaults());
      return PathStatePhysicalOracle.verify(stores, window, scratch, rowsPerFlush);
    }
  }

  private static Map<String, String> parse(String[] args) {
    if (args == null || args.length == 0 || args.length % 2 != 0) {
      throw usage();
    }
    Map<String, String> options = new LinkedHashMap<>();
    for (int index = 0; index < args.length; index += 2) {
      String name = args[index];
      if (!"--root".equals(name) && !"--scratch".equals(name)
          && !"--blocks".equals(name) && !"--rows-per-flush".equals(name)
          && !"--engine".equals(name)) {
        throw usage();
      }
      if (options.put(name, args[index + 1]) != null) {
        throw new IllegalArgumentException("duplicate physical oracle option: " + name);
      }
    }
    return options;
  }

  private static String require(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null || value.isEmpty()) {
      throw usage();
    }
    return value;
  }

  private static int positiveInt(String value, String name) {
    try {
      int parsed = Integer.parseInt(value);
      if (parsed <= 0) {
        throw new NumberFormatException("not positive");
      }
      return parsed;
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException(name + " must be a positive integer", invalid);
    }
  }

  private static IllegalArgumentException usage() {
    return new IllegalArgumentException("usage: PathStatePhysicalOracleTool"
        + " --root <offline-path-state-root> --scratch <fresh-directory> --blocks <count>"
        + " [--rows-per-flush <count>] [--engine ROCKSDB|LEVELDB]");
  }
}
