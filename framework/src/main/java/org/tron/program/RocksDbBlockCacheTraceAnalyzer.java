package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Aggregates bounded per-database block-cache trace CSV files. */
public final class RocksDbBlockCacheTraceAnalyzer {

  private RocksDbBlockCacheTraceAnalyzer() {
  }

  public static void main(String[] args) throws Exception {
    Arguments options = new Arguments();
    JCommander.newBuilder().addObject(options).build().parse(args);
    analyze(Paths.get(options.input), Paths.get(options.output));
  }

  static void analyze(Path input, Path output) throws Exception {
    Files.createDirectories(output);
    Map<EventKey, Aggregate> events = new HashMap<>();
    Map<GetKey, GetAggregate> gets = new HashMap<>();
    try (Stream<Path> paths = Files.list(input)) {
      List<Path> files = new ArrayList<>();
      paths.filter(p -> p.toString().endsWith(".csv")).sorted().forEach(files::add);
      for (Path path : files) {
        read(path, events, gets);
      }
    }
    writeEvents(output.resolve("block-access.csv"), events);
    writeGets(output.resolve("get-path.csv"), gets);
  }

  private static void read(Path path, Map<EventKey, Aggregate> events,
      Map<GetKey, GetAggregate> gets) throws Exception {
    String db = path.getFileName().toString().replaceFirst("\\.csv$", "");
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      reader.readLine();
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = splitCsv(line);
        if (values.length != 12) {
          throw new IllegalArgumentException("Invalid trace row in " + path + ": " + line);
        }
        long getId = Long.parseUnsignedLong(values[1]);
        int level = Integer.parseInt(values[2]);
        int caller = Integer.parseInt(values[4]);
        int blockType = Integer.parseInt(values[5]);
        boolean cacheHit = "1".equals(values[6]);
        boolean keyExists = "1".equals(values[8]);
        long blockSize = Long.parseLong(values[9]);
        EventKey eventKey = new EventKey(db, level, callerName(caller), blockName(blockType),
            cacheHit ? "hit" : "miss");
        events.computeIfAbsent(eventKey, ignored -> new Aggregate()).add(blockSize);
        if (getId != 0 && caller == 1) {
          gets.computeIfAbsent(new GetKey(db, getId), ignored -> new GetAggregate())
              .add(level, blockType, cacheHit, keyExists);
        }
      }
    }
  }

  private static String[] splitCsv(String line) {
    List<String> values = new ArrayList<>();
    StringBuilder value = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char current = line.charAt(i);
      if (current == '"') {
        if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          value.append('"');
          i++;
        } else {
          quoted = !quoted;
        }
      } else if (current == ',' && !quoted) {
        values.add(value.toString());
        value.setLength(0);
      } else {
        value.append(current);
      }
    }
    values.add(value.toString());
    return values.toArray(new String[0]);
  }

  private static void writeEvents(Path path, Map<EventKey, Aggregate> values) throws Exception {
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write("db,level,caller,block_type,cache_result,accesses,bytes\n");
      values.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> write(writer,
          entry.getKey().csv() + "," + entry.getValue().count + "," + entry.getValue().bytes));
    }
  }

  private static void writeGets(Path path, Map<GetKey, GetAggregate> values) throws Exception {
    Map<String, long[]> summary = new LinkedHashMap<>();
    values.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
      GetAggregate get = entry.getValue();
      long[] counts = summary.computeIfAbsent(entry.getKey().db, ignored -> new long[6]);
      counts[0]++;
      counts[1] += get.dataCandidates;
      counts[2] += get.found ? get.candidateMisses : 0;
      counts[3] += get.found ? 1 : 0;
      counts[4] += get.dataCacheMisses;
      counts[5] += get.found ? 0 : get.candidateMisses;
    });
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write("db,sampled_gets,data_candidates,upper_misses,trace_found_gets,"
          + "data_cache_misses,final_miss_candidates\n");
      summary.forEach((db, count) -> write(writer, db + "," + count[0] + "," + count[1]
          + "," + count[2] + "," + count[3] + "," + count[4] + "," + count[5]));
    }
  }

  private static void write(BufferedWriter writer, String value) {
    try {
      writer.write(value);
      writer.newLine();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String callerName(int caller) {
    String[] names = {"reserved", "get", "multiget", "iterator", "approximate_size",
        "verify_checksum", "sst_dump", "ingest", "repair", "prefetch", "compaction",
        "compaction_refill", "flush", "sst_reader", "uncategorized"};
    return caller >= 0 && caller < names.length ? names[caller] : "caller_" + caller;
  }

  private static String blockName(int type) {
    switch (type) {
      case 7:
        return "index";
      case 8:
        return "filter";
      case 9:
        return "data";
      case 10:
        return "compression_dict";
      case 11:
        return "range_deletion";
      default:
        return "block_" + type;
    }
  }

  private static final class Aggregate {
    private long count;
    private long bytes;

    void add(long size) {
      count++;
      bytes += size;
    }
  }

  private static final class GetAggregate {
    private int dataCandidates;
    private int candidateMisses;
    private int dataCacheMisses;
    private boolean found;

    void add(int level, int blockType, boolean cacheHit, boolean keyExists) {
      if (blockType != 9) {
        return;
      }
      dataCandidates++;
      if (!cacheHit) {
        dataCacheMisses++;
      }
      if (keyExists) {
        found = true;
      } else {
        candidateMisses++;
      }
    }
  }

  private static final class EventKey implements Comparable<EventKey> {
    private final String db;
    private final int level;
    private final String caller;
    private final String block;
    private final String result;

    EventKey(String db, int level, String caller, String block, String result) {
      this.db = db;
      this.level = level;
      this.caller = caller;
      this.block = block;
      this.result = result;
    }

    String csv() {
      return db + "," + level + "," + caller + "," + block + "," + result;
    }

    public int compareTo(EventKey other) {
      return csv().compareTo(other.csv());
    }

    public boolean equals(Object other) {
      return other instanceof EventKey && compareTo((EventKey) other) == 0;
    }

    public int hashCode() {
      return csv().hashCode();
    }
  }

  private static final class GetKey implements Comparable<GetKey> {
    private final String db;
    private final long id;

    GetKey(String db, long id) {
      this.db = db;
      this.id = id;
    }

    public int compareTo(GetKey other) {
      int comparison = db.compareTo(other.db);
      return comparison != 0 ? comparison : Long.compareUnsigned(id, other.id);
    }

    public boolean equals(Object other) {
      return other instanceof GetKey && compareTo((GetKey) other) == 0;
    }

    public int hashCode() {
      return 31 * db.hashCode() + Long.hashCode(id);
    }
  }

  static final class Arguments {
    @Parameter(names = "--input", required = true)
    private String input;
    @Parameter(names = "--output", required = true)
    private String output;
  }
}
