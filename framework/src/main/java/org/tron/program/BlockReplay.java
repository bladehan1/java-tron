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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.log.LogService;
import org.tron.common.prometheus.Metrics;
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
    Args.setParam(new String[] {"-c", options.config}, "config.conf");
    return replayInput(Paths.get(options.input), null, null, options.warmupBlocks,
        options.maxBlocks, false);
  }

  private static ReplayResult apply(Options options) throws Exception {
    Path outputDirectory = Paths.get(options.outputDirectory).toAbsolutePath().normalize();
    if (!Files.isDirectory(outputDirectory)) {
      throw new IllegalArgumentException(
          "Output directory must be an existing D0 snapshot: " + outputDirectory);
    }
    Args.setParam(new String[] {"-c", options.config, "-d", outputDirectory.toString(),
        "--p2p-disable", "true"}, "config.conf");
    LogService.load(Args.getInstance().getLogbackPath());
    Metrics.init();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    try {
      context.register(DefaultConfig.class);
      context.refresh();
      startConsensus(context);
      ReplayResult result = replayInput(Paths.get(options.input),
          context.getBean(TronNetDelegate.class),
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

  static void flushPending(TronApplicationContext context) {
    context.getBean(RevokingDatabase.class).flushPending();
  }

  static ReplayResult replay(Path input, TronNetDelegate tronNetDelegate,
      ChainBaseManager chainBaseManager,
      long warmupBlocks, long maxBlocks, boolean apply) throws Exception {
    return replay(input, tronNetDelegate, chainBaseManager, warmupBlocks, maxBlocks, apply, null);
  }

  private static ReplayResult replay(Path input, TronNetDelegate tronNetDelegate,
      ChainBaseManager chainBaseManager, long warmupBlocks, long maxBlocks, boolean apply,
      byte[] expectedParentBlockId) throws Exception {
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
      byte[] lastBlockId = expectedParentBlockId;
      while (processed < selectedCount && reader.hasNext()) {
        BlockFile.Record record = reader.next();
        BlockCapsule block = new BlockCapsule(record.getBlock());
        if (!Arrays.equals(record.getBlockId(), block.getBlockId().getBytes())) {
          throw new IOException("Computed block ID mismatch at height " + record.getHeight());
        }
        if (processed == 0 && expectedParentBlockId != null && !Arrays.equals(
            expectedParentBlockId, block.getParentHash().getBytes())) {
          throw new IllegalArgumentException("Block file parent mismatch at "
              + record.getHeight());
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
        lastBlockId = record.getBlockId();
      }
      if (selectedCount == header.getCount()) {
        reader.hasNext();
      }
      return new ReplayResult(apply, header.getStart(), lastHeight, processed,
          Math.min(warmupBlocks, processed), measured, measuredNanos, lastBlockId);
    }
  }

  static ReplayResult replayInput(Path input, TronNetDelegate tronNetDelegate,
      ChainBaseManager chainBaseManager, long warmupBlocks, long maxBlocks, boolean apply)
      throws Exception {
    if (Files.isRegularFile(input)) {
      return replay(input, tronNetDelegate, chainBaseManager, warmupBlocks, maxBlocks, apply);
    }
    List<InputFile> inputs = listInputFiles(input);
    long processed = 0;
    long warmup = 0;
    long measured = 0;
    long measuredNanos = 0;
    long start = inputs.get(0).start;
    long end = start - 1;
    byte[] previousBlockId = null;
    for (InputFile inputFile : inputs) {
      long remaining = maxBlocks - processed;
      if (remaining <= 0) {
        break;
      }
      ReplayResult part = replay(inputFile.path, tronNetDelegate, chainBaseManager,
          Math.max(0, warmupBlocks - processed), remaining, apply, previousBlockId);
      processed += part.processed;
      warmup += part.warmup;
      measured += part.measured;
      measuredNanos += part.measuredNanos;
      end = part.end;
      previousBlockId = part.lastBlockId;
    }
    return new ReplayResult(apply, start, end, processed, warmup, measured, measuredNanos,
        previousBlockId);
  }

  private static List<InputFile> listInputFiles(Path input) throws IOException {
    if (!Files.isDirectory(input)) {
      throw new IllegalArgumentException("Block input does not exist: " + input);
    }
    List<Path> paths;
    try (Stream<Path> stream = Files.list(input)) {
      paths = stream.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".dat"))
          .collect(Collectors.toList());
    }
    List<InputFile> inputs = new ArrayList<>(paths.size());
    for (Path path : paths) {
      try (BlockFile.Reader reader = BlockFile.open(path)) {
        BlockFile.Header header = reader.getHeader();
        inputs.add(new InputFile(path, header.getStart(), header.getEnd()));
      }
    }
    inputs.sort(Comparator.comparingLong(value -> value.start));
    if (inputs.isEmpty()) {
      throw new IllegalArgumentException("Block input directory has no .dat files: " + input);
    }
    for (int i = 1; i < inputs.size(); i++) {
      if (inputs.get(i).start != inputs.get(i - 1).end + 1) {
        throw new IllegalArgumentException("Block input files are not consecutive between "
            + inputs.get(i - 1).path + " and " + inputs.get(i).path);
      }
    }
    return inputs;
  }

  static final class Options {
    @Parameter(names = {"-i", "--input"},
        description = "Block file or consecutive chunk directory to verify or replay.")
    private String input;

    @Parameter(names = {"-d", "--output-directory"},
        description = "Existing stopped-node D0 output directory. Required with --apply.")
    private String outputDirectory;

    @Parameter(names = {"-c", "--config"}, description = "Node config file.")
    private String config = "config.conf";

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
      if (!Files.isRegularFile(Paths.get(input)) && !Files.isDirectory(Paths.get(input))) {
        throw new ParameterException("Block input does not exist: " + input);
      }
      if (config == null || config.trim().isEmpty()) {
        throw new ParameterException("--config must not be empty");
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
    private final byte[] lastBlockId;

    private ReplayResult(boolean applied, long start, long end, long processed,
        long warmup, long measured, long measuredNanos, byte[] lastBlockId) {
      this.applied = applied;
      this.start = start;
      this.end = end;
      this.processed = processed;
      this.warmup = warmup;
      this.measured = measured;
      this.measuredNanos = measuredNanos;
      this.lastBlockId = lastBlockId == null ? null
          : Arrays.copyOf(lastBlockId, lastBlockId.length);
    }

    String format() {
      double elapsedMs = measuredNanos / 1_000_000.0;
      double blocksPerSecond = measuredNanos == 0 ? 0.0
          : measured * 1_000_000_000.0 / measuredNanos;
      return String.format(Locale.ROOT,
          "mode=%s range=[%d,%d] processed=%d warmup=%d measured=%d "
              + "elapsed_ms=%.3f blocks_per_second=%.3f",
          applied ? "apply" : "verify", start, end, processed, warmup, measured,
          elapsedMs, blocksPerSecond);
    }
  }

  private static final class InputFile {
    private final Path path;
    private final long start;
    private final long end;

    private InputFile(Path path, long start, long end) {
      this.path = path;
      this.start = start;
      this.end = end;
    }
  }
}
