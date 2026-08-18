package org.tron.program;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.tron.common.utils.BlockFile;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.services.http.JsonFormat;
import org.tron.json.JSONArray;
import org.tron.json.JSONObject;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.BlockHeader;
import org.tron.protos.Protocol.Transaction;

/** Downloads consecutive blocks from a java-tron HTTP API into resumable block-file chunks. */
public final class HttpBlockExport {

  static final int MAX_CHUNK_SIZE = 100;

  private HttpBlockExport() {
  }

  public static void main(String[] args) {
    int exitCode = execute(args, System.out, System.err);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int execute(String[] args, java.io.PrintStream output, java.io.PrintStream error) {
    Options options = new Options();
    JCommander commander = JCommander.newBuilder()
        .addObject(options)
        .programName("HttpBlockExport")
        .build();
    try {
      commander.parse(args);
      if (options.help) {
        commander.usage();
        return 0;
      }
      options.validate();
      Files.createDirectories(Paths.get(options.outputDirectory));
      export(options, output);
      return 0;
    } catch (ParameterException | IllegalArgumentException e) {
      error.println(e.getMessage());
      commander.usage();
      return 2;
    } catch (Exception e) {
      error.println("HTTP block export failed: " + e.getMessage());
      return 1;
    }
  }

  private static void export(Options options, java.io.PrintStream output) throws IOException {
    long next = options.start;
    while (next <= options.end) {
      long chunkEnd = Math.min(options.end, next + options.chunkSize - 1L);
      Path target = Paths.get(options.outputDirectory)
          .resolve(String.format("%d-%d.dat", next, chunkEnd));
      if (Files.isRegularFile(target) && !options.overwrite) {
        verifyChunk(target, next, chunkEnd);
        output.printf("Reused verified chunk [%d, %d] %s%n", next, chunkEnd, target);
      } else {
        List<BlockFile.Record> records = fetchChunk(options, next, chunkEnd);
        long chunkStart = next;
        BlockFile.write(target, next, chunkEnd, options.overwrite,
            height -> records.get(Math.toIntExact(height - chunkStart)));
        output.printf("Downloaded chunk [%d, %d] %s%n", next, chunkEnd, target);
      }
      if (chunkEnd == Long.MAX_VALUE) {
        break;
      }
      next = chunkEnd + 1;
    }
  }

  static void verifyChunk(Path input, long expectedStart, long expectedEnd) throws IOException {
    try (BlockFile.Reader reader = BlockFile.open(input)) {
      BlockFile.Header header = reader.getHeader();
      if (header.getStart() != expectedStart || header.getEnd() != expectedEnd) {
        throw new IOException("Existing chunk has unexpected range: " + input);
      }
      while (reader.hasNext()) {
        BlockFile.Record record = reader.next();
        BlockCapsule block = new BlockCapsule(record.getBlock());
        if (!Arrays.equals(record.getBlockId(), block.getBlockId().getBytes())) {
          throw new IOException("Computed block ID mismatch at height " + record.getHeight());
        }
      }
    }
  }

  private static List<BlockFile.Record> fetchChunk(Options options, long start, long end)
      throws IOException {
    IOException lastFailure = null;
    for (int attempt = 1; attempt <= options.retries; attempt++) {
      try {
        String response = request(options, start, end);
        List<BlockFile.Record> records = parseResponse(response);
        if (records.size() != end - start + 1) {
          throw new IOException("Expected " + (end - start + 1) + " blocks but received "
              + records.size());
        }
        for (int i = 0; i < records.size(); i++) {
          if (records.get(i).getHeight() != start + i) {
            throw new IOException("Non-consecutive HTTP response at block " + (start + i));
          }
        }
        return records;
      } catch (IOException | RuntimeException e) {
        lastFailure = e instanceof IOException ? (IOException) e
            : new IOException(e.getMessage(), e);
        if (attempt < options.retries) {
          try {
            Thread.sleep(Math.min(5000L, attempt * 1000L));
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying HTTP request", interrupted);
          }
        }
      }
    }
    throw new IOException("Failed to download blocks [" + start + ", " + end + "] after "
        + options.retries + " attempts", lastFailure);
  }

  private static String request(Options options, long start, long end) throws IOException {
    URL url = new URL(trimTrailingSlash(options.endpoint) + "/wallet/getblockbylimitnext");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setConnectTimeout(options.timeoutMillis);
    connection.setReadTimeout(options.timeoutMillis);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    byte[] request = ("{\"startNum\":" + start + ",\"endNum\":" + (end + 1) + "}")
        .getBytes(StandardCharsets.UTF_8);
    connection.setFixedLengthStreamingMode(request.length);
    try {
      try (OutputStream stream = connection.getOutputStream()) {
        stream.write(request);
      }
      int status = connection.getResponseCode();
      InputStream response = status >= 200 && status < 300
          ? connection.getInputStream() : connection.getErrorStream();
      String body = readAll(response);
      if (status < 200 || status >= 300) {
        throw new IOException("HTTP " + status + " for blocks [" + start + ", " + end
            + "]: " + abbreviate(body));
      }
      return body;
    } finally {
      connection.disconnect();
    }
  }

  static List<BlockFile.Record> parseResponse(String response) throws IOException {
    JSONObject root = JSONObject.parseObject(response);
    JSONArray blocks = root.getJSONArray("block");
    if (blocks == null) {
      throw new IOException("HTTP response does not contain a block array: "
          + abbreviate(response));
    }
    List<BlockFile.Record> records = new ArrayList<>(blocks.size());
    for (int i = 0; i < blocks.size(); i++) {
      JSONObject source = blocks.getJSONObject(i);
      String expectedBlockId = source.getString("blockID");
      JSONObject headerJson = source.getJSONObject("block_header");
      if (expectedBlockId == null || headerJson == null) {
        throw new IOException("Block response is missing blockID or block_header");
      }
      BlockHeader.Builder header = BlockHeader.newBuilder();
      JsonFormat.merge(headerJson.toJSONString(), header, false);
      Block.Builder block = Block.newBuilder().setBlockHeader(header);
      JSONArray transactions = source.getJSONArray("transactions");
      if (transactions != null) {
        for (int transactionIndex = 0; transactionIndex < transactions.size();
            transactionIndex++) {
          JSONObject transactionJson = transactions.getJSONObject(transactionIndex);
          Transaction transaction = parseTransaction(transactionJson, transactionIndex);
          String expectedTransactionId = transactionJson.getString("txID");
          if (expectedTransactionId != null && !expectedTransactionId.equalsIgnoreCase(
              ByteArray.toHexString(new TransactionCapsule(transaction).getTransactionId()
                  .getBytes()))) {
            throw new IOException("Computed transaction ID mismatch at transaction "
                + transactionIndex);
          }
          block.addTransactions(transaction);
        }
      }
      Block parsed = block.build();
      BlockCapsule capsule = new BlockCapsule(parsed);
      byte[] blockId = capsule.getBlockId().getBytes();
      if (!expectedBlockId.equalsIgnoreCase(ByteArray.toHexString(blockId))) {
        throw new IOException("Computed block ID mismatch at height "
            + parsed.getBlockHeader().getRawData().getNumber());
      }
      records.add(new BlockFile.Record(
          parsed.getBlockHeader().getRawData().getNumber(), blockId, parsed.toByteArray()));
    }
    return records;
  }

  private static Transaction parseTransaction(JSONObject source, int index) throws IOException {
    String rawDataHex = source.getString("raw_data_hex");
    if (rawDataHex == null) {
      throw new IOException("Transaction " + index + " does not contain raw_data_hex");
    }
    try {
      Transaction.raw raw = Transaction.raw.parseFrom(ByteArray.fromHexString(rawDataHex));
      JSONObject envelope = JSONObject.parseObject(source.toJSONString());
      envelope.remove("txID");
      envelope.remove("raw_data");
      envelope.remove("raw_data_hex");
      envelope.remove("visible");
      Transaction.Builder transaction = Transaction.newBuilder();
      JsonFormat.merge(envelope.toJSONString(), transaction, false);
      return transaction.setRawData(raw).build();
    } catch (RuntimeException e) {
      throw new IOException("Cannot parse transaction " + index, e);
    }
  }

  private static String readAll(InputStream input) throws IOException {
    if (input == null) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(input, StandardCharsets.UTF_8))) {
      char[] buffer = new char[8192];
      int read;
      while ((read = reader.read(buffer)) != -1) {
        result.append(buffer, 0, read);
      }
    }
    return result.toString();
  }

  private static String trimTrailingSlash(String endpoint) {
    int end = endpoint.length();
    while (end > 0 && endpoint.charAt(end - 1) == '/') {
      end--;
    }
    return endpoint.substring(0, end);
  }

  private static String abbreviate(String value) {
    return value.length() <= 256 ? value : value.substring(0, 256);
  }

  static final class Options {
    @Parameter(names = "--endpoint", description = "Base URL of a java-tron HTTP API.")
    private String endpoint = "https://api.trongrid.io";

    @Parameter(names = "--start", description = "First block height, inclusive.")
    private long start = -1;

    @Parameter(names = "--end", description = "Last block height, inclusive.")
    private long end = -1;

    @Parameter(names = {"-o", "--output-directory"},
        description = "Directory for atomic block-file chunks.")
    private String outputDirectory;

    @Parameter(names = "--chunk-size", description = "Blocks per output file, at most 100.")
    private int chunkSize = MAX_CHUNK_SIZE;

    @Parameter(names = "--timeout-millis", description = "HTTP connect and read timeout.")
    private int timeoutMillis = 120000;

    @Parameter(names = "--retries", description = "Attempts for each HTTP chunk.")
    private int retries = 5;

    @Parameter(names = "--overwrite", description = "Replace existing chunks.")
    private boolean overwrite;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private void validate() {
      if (endpoint == null || endpoint.trim().isEmpty()) {
        throw new ParameterException("--endpoint must not be empty");
      }
      if (start < 0) {
        throw new ParameterException("--start must be non-negative");
      }
      if (end < start) {
        throw new ParameterException("--end must be greater than or equal to --start");
      }
      if (end == Long.MAX_VALUE) {
        throw new ParameterException("--end must be less than Long.MAX_VALUE");
      }
      if (outputDirectory == null || outputDirectory.trim().isEmpty()) {
        throw new ParameterException("--output-directory is required");
      }
      if (chunkSize <= 0 || chunkSize > MAX_CHUNK_SIZE) {
        throw new ParameterException("--chunk-size must be between 1 and " + MAX_CHUNK_SIZE);
      }
      if (timeoutMillis <= 0) {
        throw new ParameterException("--timeout-millis must be positive");
      }
      if (retries <= 0) {
        throw new ParameterException("--retries must be positive");
      }
    }
  }
}
