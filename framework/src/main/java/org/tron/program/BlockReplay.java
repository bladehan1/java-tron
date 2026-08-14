package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.log.LogService;
import org.tron.common.prometheus.Metrics;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.utils.BlockFile;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.consensus.ConsensusService;
import org.tron.core.db.RevokingDatabase;
import org.tron.core.net.TronNetDelegate;

/**
 * Offline block replay entry point for fixed-window database benchmarks.
 */
@Slf4j(topic = "app")
public final class BlockReplay {

  private BlockReplay() {
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
        .programName("BlockReplay")
        .build();
    try {
      commander.parse(args);
      if (options.help) {
        commander.usage();
        return 0;
      }
      options.validate();
      ReplayResult result = options.apply
          ? apply(options)
          : verify(options);
      output.println(result.format());
      return 0;
    } catch (ParameterException | IllegalArgumentException e) {
      error.println(e.getMessage());
      commander.usage();
      return 2;
    } catch (Exception e) {
      logger.error("Block replay failed", e);
      error.println("Block replay failed: " + e.getMessage());
      return 1;
    } finally {
      Args.clearParam();
    }
  }

  private static ReplayResult verify(Options options) throws Exception {
    Args.setParam(nodeArgs(options, null), "config.conf");
    return replay(Paths.get(options.input), null, null, options.warmupBlocks,
        options.maxBlocks, false);
  }

  private static ReplayResult apply(Options options) throws Exception {
    Path outputDirectory = Paths.get(options.outputDirectory).toAbsolutePath().normalize();
    if (!Files.isDirectory(outputDirectory)) {
      throw new IllegalArgumentException(
          "Output directory must be an existing D0 snapshot: " + outputDirectory);
    }
    Args.setParam(nodeArgs(options, outputDirectory), "config.conf");
    LogService.load(Args.getInstance().getLogbackPath());
    Metrics.init();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    try {
      context.register(DefaultConfig.class);
      context.refresh();
      startConsensus(context);
      ReplayResult result = replay(Paths.get(options.input), context.getBean(TronNetDelegate.class),
          context.getBean(ChainBaseManager.class), options.warmupBlocks,
          options.maxBlocks, true);
      flushPending(context);
      return result;
    } finally {
      context.close();
    }
  }

  static void startConsensus(TronApplicationContext context) {
    context.getBean(ConsensusService.class).start();
  }

  private static String[] nodeArgs(Options options, Path outputDirectory) {
    List<String> args = new ArrayList<>();
    args.add("-c");
    args.add(options.config);
    if (options.rocksDbConfig != null) {
      args.add("--rocksdb-config");
      args.add(options.rocksDbConfig);
    }
    if (outputDirectory != null) {
      args.add("-d");
      args.add(outputDirectory.toString());
      args.add("--p2p-disable");
      args.add("true");
    }
    return args.toArray(new String[0]);
  }

  static void flushPending(TronApplicationContext context) {
    context.getBean(RevokingDatabase.class).flushPending();
  }

  static ReplayResult replay(Path input, TronNetDelegate tronNetDelegate,
      ChainBaseManager chainBaseManager,
      long warmupBlocks, long maxBlocks, boolean apply) throws Exception {
    try (BlockFile.Reader reader = BlockFile.open(input)) {
      BlockFile.Header header = reader.getHeader();
      long selectedCount = Math.min(header.getCount(), maxBlocks);
      if (apply) {
        long expectedStart = chainBaseManager.getHeadBlockNum() + 1;
        if (header.getStart() != expectedStart) {
          throw new IllegalArgumentException("Block file starts at " + header.getStart()
              + " but D0 requires block " + expectedStart);
        }
      }

      long processed = 0;
      long measured = 0;
      long measuredNanos = 0;
      long lastHeight = header.getStart() - 1;
      while (processed < selectedCount && reader.hasNext()) {
        BlockFile.Record record = reader.next();
        BlockCapsule block = new BlockCapsule(record.getBlock());
        if (!Arrays.equals(record.getBlockId(), block.getBlockId().getBytes())) {
          throw new IOException("Computed block ID mismatch at height " + record.getHeight());
        }
        if (apply && processed == 0 && !block.getParentHash().equals(
            chainBaseManager.getHeadBlockId())) {
          throw new IllegalArgumentException("First block parent does not match D0 head");
        }

        long startNanos = apply ? System.nanoTime() : 0;
        if (apply) {
          tronNetDelegate.processBlock(block, true);
          if (chainBaseManager.getHeadBlockNum() != record.getHeight()
              || !chainBaseManager.getHeadBlockId().equals(block.getBlockId())) {
            throw new IllegalStateException(
                "D0 head did not advance to block " + record.getHeight());
          }
        }
        long elapsedNanos = apply ? System.nanoTime() - startNanos : 0;
        if (processed >= warmupBlocks) {
          measuredNanos += elapsedNanos;
          measured++;
        }
        processed++;
        lastHeight = record.getHeight();
      }
      if (selectedCount == header.getCount()) {
        reader.hasNext();
      }
      return new ReplayResult(apply, header.getStart(), lastHeight, processed,
          Math.min(warmupBlocks, processed), measured, measuredNanos);
    }
  }

  static final class Options {
    @Parameter(names = {"-i", "--input"}, description = "Block file to verify or replay.")
    private String input;

    @Parameter(names = {"-d", "--output-directory"},
        description = "Existing stopped-node D0 output directory. Required with --apply.")
    private String outputDirectory;

    @Parameter(names = {"-c", "--config"}, description = "Node config file.")
    private String config = "config.conf";

    @Parameter(names = "--rocksdb-config",
        description = "RocksDB benchmark profile applied over storage.dbSettings.")
    private String rocksDbConfig;

    @Parameter(names = "--apply",
        description = "Apply blocks to D0. Without this flag the command only verifies the file.")
    private boolean apply;

    @Parameter(names = "--warmup-blocks",
        description = "Exclude this many leading blocks from measured time.")
    private long warmupBlocks;

    @Parameter(names = "--max-blocks",
        description = "Process at most this many blocks from the file.")
    private long maxBlocks = Long.MAX_VALUE;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private void validate() {
      if (input == null || input.trim().isEmpty()) {
        throw new ParameterException("--input is required");
      }
      if (!Files.isRegularFile(Paths.get(input))) {
        throw new ParameterException("Block file does not exist: " + input);
      }
      if (config == null || config.trim().isEmpty()) {
        throw new ParameterException("--config must not be empty");
      }
      if (rocksDbConfig != null && !Files.isRegularFile(Paths.get(rocksDbConfig))) {
        throw new ParameterException("RocksDB profile does not exist: " + rocksDbConfig);
      }
      if (apply && (outputDirectory == null || outputDirectory.trim().isEmpty())) {
        throw new ParameterException("--output-directory is required with --apply");
      }
      if (warmupBlocks < 0) {
        throw new ParameterException("--warmup-blocks must be non-negative");
      }
      if (maxBlocks <= 0) {
        throw new ParameterException("--max-blocks must be positive");
      }
    }
  }

  static final class ReplayResult {
    private final boolean applied;
    private final long start;
    private final long end;
    private final long processed;
    private final long warmup;
    private final long measured;
    private final long measuredNanos;

    private ReplayResult(boolean applied, long start, long end, long processed,
        long warmup, long measured, long measuredNanos) {
      this.applied = applied;
      this.start = start;
      this.end = end;
      this.processed = processed;
      this.warmup = warmup;
      this.measured = measured;
      this.measuredNanos = measuredNanos;
    }

    String format() {
      RocksDbSettings rocksDbSettings = RocksDbSettings.getSettings();
      double elapsedMs = measuredNanos / 1_000_000.0;
      double blocksPerSecond = measuredNanos == 0 ? 0.0
          : measured * 1_000_000_000.0 / measuredNanos;
      return String.format(Locale.ROOT,
          "mode=%s rocksdb_profile=%s rocksdb_experiment=%s "
              + "range=[%d,%d] processed=%d warmup=%d measured=%d "
              + "elapsed_ms=%.3f blocks_per_second=%.3f",
          applied ? "apply" : "verify", rocksDbSettings.getBenchmarkProfile(),
          rocksDbSettings.getBenchmarkMode(), start, end, processed, warmup,
          measured, elapsedMs, blocksPerSecond);
    }
  }
}
