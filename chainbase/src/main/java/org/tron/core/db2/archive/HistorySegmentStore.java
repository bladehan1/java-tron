package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Append-only, rotating history body segments with strict tail scanning. */
public final class HistorySegmentStore implements Closeable {

  private static final String PREFIX = "history.";
  private static final String SUFFIX = ".dat";

  private final Path directory;
  private final BlockHistoryCodec codec;
  private final long maxSegmentSize;

  private FileChannel appendChannel;
  private int appendSegmentId;
  private ScanResult scanResult;

  public HistorySegmentStore(Path archiveDirectory, BlockHistoryCodec codec, long maxSegmentSize)
      throws IOException {
    if (maxSegmentSize <= 0) {
      throw new IllegalArgumentException("maxSegmentSize must be positive");
    }
    this.directory = archiveDirectory.resolve("history");
    this.codec = codec;
    this.maxSegmentSize = maxSegmentSize;
    Files.createDirectories(directory);
    scanResult = scan();
    openAppendChannel();
  }

  public synchronized HistoryLocation append(BlockReverseDiff diff) throws IOException {
    if (scanResult.getInvalidTail() != null) {
      throw new IllegalStateException("History has an invalid tail which must be truncated first");
    }
    if (scanResult.getHead() != null) {
      validateContinuity(scanResult.getHead().getDiff().getMeta(), diff.getMeta());
    }
    byte[] record = codec.encode(diff);
    long offset = appendChannel.size();
    if (offset > 0 && offset + record.length > maxSegmentSize) {
      rotate();
      offset = 0;
    }
    appendChannel.position(offset);
    writeFully(appendChannel, ByteBuffer.wrap(record));
    HistoryLocation location = location(appendSegmentId, offset, record);
    scanResult = new ScanResult(scanResult.getRecordCount() + 1,
        new ScannedRecord(diff, location), null);
    return location;
  }

  public synchronized void sync() throws IOException {
    appendChannel.force(true);
    syncDirectory(directory);
  }

  public synchronized BlockReverseDiff read(HistoryLocation location) throws IOException {
    return codec.decode(readRecord(location));
  }

  public synchronized byte[] readRecord(HistoryLocation location) throws IOException {
    Path path = segmentPath(location.getSegmentId());
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      if (location.endOffset() > channel.size()) {
        throw new IllegalArgumentException("History location is outside segment bounds");
      }
      ByteBuffer record = ByteBuffer.allocate(location.getRecordLength());
      channel.position(location.getOffset());
      readFully(channel, record);
      byte[] bytes = record.array();
      if (!Arrays.equals(sha256(bytes), location.getBodyDigest())) {
        throw new IllegalArgumentException("History location digest mismatch");
      }
      return bytes;
    }
  }

  public synchronized ScanResult getScanResult() {
    return scanResult;
  }

  public synchronized ScanResult rescan() throws IOException {
    scanResult = scan();
    return scanResult;
  }

  /** Removes only the suffix starting at the invalid record found by {@link #scan()}. */
  public synchronized void truncateInvalidTail() throws IOException {
    InvalidTail tail = scanResult.getInvalidTail();
    if (tail == null) {
      return;
    }
    closeAppendChannel();
    truncateFrom(tail.getSegmentId(), tail.getOffset());
    scanResult = scan();
    openAppendChannel();
  }

  /** Truncates all records after {@code last}; null means remove every body record. */
  public synchronized void truncateAfter(HistoryLocation last) throws IOException {
    closeAppendChannel();
    if (last == null) {
      for (Path segment : listSegments()) {
        Files.deleteIfExists(segment);
      }
    } else {
      truncateFrom(last.getSegmentId(), last.endOffset());
    }
    syncDirectory(directory);
    scanResult = scan();
    openAppendChannel();
  }

  private ScanResult scan() throws IOException {
    long recordCount = 0;
    ScannedRecord head = null;
    InvalidTail invalidTail = null;
    BlockSnapshotMeta previous = null;
    List<Path> segments = listSegments();
    int expectedSegmentId = segments.isEmpty() ? 0 : parseSegmentId(segments.get(0));
    for (Path segment : segments) {
      int segmentId = parseSegmentId(segment);
      if (segmentId != expectedSegmentId) {
        return new ScanResult(recordCount, head, new InvalidTail(segmentId, 0,
            "non-contiguous segment id"));
      }
      expectedSegmentId++;
      try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
        long offset = 0;
        while (offset < channel.size()) {
          long remaining = channel.size() - offset;
          if (remaining < BlockHistoryCodec.HEADER_LENGTH) {
            invalidTail = new InvalidTail(segmentId, offset, "truncated record header");
            break;
          }
          ByteBuffer header = ByteBuffer.allocate(BlockHistoryCodec.HEADER_LENGTH);
          channel.position(offset);
          readFully(channel, header);
          int recordLength;
          try {
            recordLength = codec.recordLength(header.array());
          } catch (IllegalArgumentException e) {
            invalidTail = new InvalidTail(segmentId, offset, e.getMessage());
            break;
          }
          if (recordLength > remaining) {
            invalidTail = new InvalidTail(segmentId, offset, "truncated record body");
            break;
          }
          ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
          channel.position(offset);
          readFully(channel, recordBuffer);
          byte[] record = recordBuffer.array();
          BlockReverseDiff diff;
          try {
            diff = codec.decode(record);
            validateContinuity(previous, diff.getMeta());
          } catch (IllegalArgumentException e) {
            invalidTail = new InvalidTail(segmentId, offset, e.getMessage());
            break;
          }
          HistoryLocation location = location(segmentId, offset, record);
          head = new ScannedRecord(diff, location);
          recordCount++;
          previous = diff.getMeta();
          offset += recordLength;
        }
      }
      if (invalidTail != null) {
        break;
      }
    }
    return new ScanResult(recordCount, head, invalidTail);
  }

  private void validateContinuity(BlockSnapshotMeta previous, BlockSnapshotMeta current) {
    if (previous == null) {
      return;
    }
    if (current.getEpoch() != previous.getEpoch() + 1
        || current.getBlockNumber() != previous.getBlockNumber() + 1
        || !Arrays.equals(current.getParentHash(), previous.getBlockHash())) {
      throw new IllegalArgumentException("non-contiguous history block metadata");
    }
  }

  private void openAppendChannel() throws IOException {
    List<Path> segments = listSegments();
    appendSegmentId = segments.isEmpty() ? 0 : parseSegmentId(segments.get(segments.size() - 1));
    Path path = segmentPath(appendSegmentId);
    boolean created = !Files.exists(path);
    appendChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
    appendChannel.position(appendChannel.size());
    if (created) {
      syncDirectory(directory);
    }
  }

  private void rotate() throws IOException {
    appendChannel.force(true);
    closeAppendChannel();
    appendSegmentId++;
    Path next = segmentPath(appendSegmentId);
    appendChannel = FileChannel.open(next, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
    syncDirectory(directory);
  }

  private void truncateFrom(int segmentId, long offset) throws IOException {
    for (Path segment : listSegments()) {
      int candidate = parseSegmentId(segment);
      if (candidate > segmentId) {
        Files.deleteIfExists(segment);
      } else if (candidate == segmentId) {
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
          channel.truncate(offset);
          channel.force(true);
        }
      }
    }
    syncDirectory(directory);
  }

  private List<Path> listSegments() throws IOException {
    List<Path> segments = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory,
        PREFIX + "*" + SUFFIX)) {
      for (Path path : stream) {
        parseSegmentId(path);
        segments.add(path);
      }
    }
    segments.sort(Comparator.comparingInt(HistorySegmentStore::parseSegmentId));
    return segments;
  }

  private Path segmentPath(int segmentId) {
    return directory.resolve(String.format("%s%06d%s", PREFIX, segmentId, SUFFIX));
  }

  private static int parseSegmentId(Path path) {
    String name = path.getFileName().toString();
    if (!name.startsWith(PREFIX) || !name.endsWith(SUFFIX)) {
      throw new IllegalArgumentException("Invalid history segment name: " + name);
    }
    String id = name.substring(PREFIX.length(), name.length() - SUFFIX.length());
    try {
      return Integer.parseInt(id);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid history segment name: " + name, e);
    }
  }

  private static HistoryLocation location(int segmentId, long offset, byte[] record) {
    int checksum = ByteBuffer.wrap(record, record.length - Integer.BYTES, Integer.BYTES).getInt();
    return new HistoryLocation(segmentId, offset, record.length, checksum, sha256(record));
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        throw new IOException("Unexpected end of history segment");
      }
    }
  }

  static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private void closeAppendChannel() throws IOException {
    if (appendChannel != null) {
      appendChannel.close();
      appendChannel = null;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closeAppendChannel();
  }

  public static final class ScannedRecord {
    private final BlockReverseDiff diff;
    private final HistoryLocation location;

    private ScannedRecord(BlockReverseDiff diff, HistoryLocation location) {
      this.diff = diff;
      this.location = location;
    }

    public BlockReverseDiff getDiff() {
      return diff;
    }

    public HistoryLocation getLocation() {
      return location;
    }
  }

  public static final class InvalidTail {
    private final int segmentId;
    private final long offset;
    private final String reason;

    private InvalidTail(int segmentId, long offset, String reason) {
      this.segmentId = segmentId;
      this.offset = offset;
      this.reason = reason;
    }

    public int getSegmentId() {
      return segmentId;
    }

    public long getOffset() {
      return offset;
    }

    public String getReason() {
      return reason;
    }
  }

  public static final class ScanResult {
    private final long recordCount;
    private final ScannedRecord head;
    private final InvalidTail invalidTail;

    private ScanResult(long recordCount, ScannedRecord head, InvalidTail invalidTail) {
      this.recordCount = recordCount;
      this.head = head;
      this.invalidTail = invalidTail;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public ScannedRecord getHead() {
      return head;
    }

    public InvalidTail getInvalidTail() {
      return invalidTail;
    }
  }
}
