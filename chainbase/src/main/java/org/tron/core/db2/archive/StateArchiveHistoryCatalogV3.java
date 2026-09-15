package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.CurrentSegment;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.SealedSegment;

/** Atomic structural catalog for the five-lane append-file history. */
final class StateArchiveHistoryCatalogV3 {

  @FunctionalInterface
  interface DirectorySync {
    void sync(Path directory) throws IOException;
  }

  static final String DIRECTORY = "catalog";
  static final String CURRENT = "CURRENT";
  private static final String GENERATIONS = "generations";
  private static final int GENERATION_MAGIC = 0x53434733; // SCG3
  private static final int CURRENT_MAGIC = 0x53435533; // SCU3
  private static final int GENERATION_TRAILER_MAGIC = 0x33474353; // 3GCS
  private static final int CURRENT_TRAILER_MAGIC = 0x33554353; // 3UCS
  private static final int HEADER_LENGTH = 256;
  private static final int CURRENT_RECORD_LENGTH = 112;
  private static final int TRAILER_LENGTH = 48;
  private static final int CURRENT_LENGTH = 96;
  private static final int MAX_RETAINED_GENERATIONS = 3;
  static final int PER_LANE_TERMINALS = 1;
  static final int TERMINAL_AWARE_FLAG = PER_LANE_TERMINALS;
  private static final int KNOWN_GENERATION_FLAGS = PER_LANE_TERMINALS;
  private static final int SEALED_TERMINAL_FLAG = 1;

  private final Path root;
  private final Path generations;
  private final DirectorySync directorySync;
  private final NavigableMap<Long, Path> generationFiles = new TreeMap<>();
  private Generation selected;

  private StateArchiveHistoryCatalogV3(Path archiveRoot, Generation selected,
      DirectorySync directorySync) {
    root = archiveRoot.resolve(DIRECTORY);
    generations = root.resolve(GENERATIONS);
    this.directorySync = Objects.requireNonNull(directorySync, "directorySync");
    this.selected = selected;
  }

  static StateArchiveHistoryCatalogV3 openOrEmpty(Path archiveRoot) throws IOException {
    return openOrEmpty(archiveRoot, StateArchiveHistoryCatalogV3::syncDirectory);
  }

  static StateArchiveHistoryCatalogV3 openOrEmpty(Path archiveRoot,
      DirectorySync directorySync) throws IOException {
    StateArchiveHistoryCatalogV3 catalog = new StateArchiveHistoryCatalogV3(archiveRoot, null,
        directorySync);
    catalog.discoverGenerationFiles();
    Path root = catalog.root;
    Path current = root.resolve(CURRENT);
    if (!Files.exists(current)) {
      return catalog;
    }
    CurrentPointer pointer = decodeCurrent(Files.readAllBytes(current));
    Path generationPath = root.resolve(GENERATIONS).resolve(fileName(pointer.generation));
    if (!Files.isRegularFile(generationPath)) {
      throw new IOException("State Archive Catalog selected generation is missing");
    }
    byte[] encoded = Files.readAllBytes(generationPath);
    Generation generation = decodeGeneration(encoded);
    if (generation.generation != pointer.generation
        || !Arrays.equals(generation.digest, pointer.digest)) {
      throw new IOException("State Archive Catalog CURRENT identity mismatch");
    }
    catalog.selected = generation;
    return catalog;
  }

  boolean isPublished() {
    return selected != null;
  }

  Generation selected() {
    if (selected == null) {
      throw new IllegalStateException("State Archive Catalog has no selected generation");
    }
    return selected;
  }

  void publish(long segmentTargetBytes, List<CurrentSegment> current,
      List<SealedSegment> sealed) throws IOException {
    publish(segmentTargetBytes, current, sealed, null);
  }

  void publish(long segmentTargetBytes, List<CurrentSegment> current,
      List<SealedSegment> sealed, List<Terminal> terminals) throws IOException {
    PreparedGeneration prepared = prepare(segmentTargetBytes, current, sealed, terminals);
    publishPrepared(selected, prepared);
  }

  PreparedGeneration prepare(long segmentTargetBytes, List<CurrentSegment> current,
      List<SealedSegment> sealed, List<Terminal> terminals) throws IOException {
    long generation = selected == null ? 0 : selected.generation + 1;
    byte[] previous = selected == null ? new byte[32] : selected.digest;
    Generation replacement = terminals == null
        ? new Generation(generation, previous, segmentTargetBytes, current, sealed, null)
        : new Generation(generation, previous, segmentTargetBytes, current, sealed, terminals,
            TERMINAL_AWARE_FLAG, null);
    byte[] encoded = encodeGeneration(replacement);
    Generation verified = decodeGeneration(encoded);
    return new PreparedGeneration(selected, verified, encoded);
  }

  PreparedGeneration prepare(long segmentTargetBytes, List<CurrentSegment> current,
      List<SealedSegment> sealed) throws IOException {
    return prepare(segmentTargetBytes, current, sealed, null);
  }

  void publishPrepared(Generation expectedSource, PreparedGeneration prepared) throws IOException {
    Objects.requireNonNull(prepared, "prepared");
    requireSource(expectedSource, selected);
    requireSource(expectedSource, prepared.source);
    long expectedGeneration = expectedSource == null ? 0 : expectedSource.generation + 1;
    if (prepared.generation.generation != expectedGeneration
        || !Arrays.equals(prepared.generation.previousDigest,
            expectedSource == null ? new byte[32] : expectedSource.digest)) {
      throw new IOException("State Archive Catalog prepared source mismatch");
    }
    Files.createDirectories(generations);
    Path temporary = generations.resolve(fileName(prepared.generation.generation) + ".tmp");
    Path target = generations.resolve(fileName(prepared.generation.generation));
    if (Files.isRegularFile(target)) {
      Generation orphan = decodeGeneration(Files.readAllBytes(target));
      if (!Arrays.equals(orphan.digest, prepared.generation.digest)) {
        throw new IOException("State Archive Catalog orphan generation identity differs");
      }
      Files.deleteIfExists(temporary);
      directorySync.sync(generations);
    } else {
      writeForced(temporary, prepared.encoded);
      atomicMove(temporary, target);
      directorySync.sync(generations);
    }
    generationFiles.put(prepared.generation.generation, target);
    byte[] currentBytes = encodeCurrent(prepared.generation.generation,
        prepared.generation.digest);
    Path currentTemporary = root.resolve(CURRENT + ".tmp");
    writeForced(currentTemporary, currentBytes);
    atomicMove(currentTemporary, root.resolve(CURRENT));
    directorySync.sync(root);
    selected = prepared.generation;
    long oldestRetained = selected.generation - MAX_RETAINED_GENERATIONS;
    for (Map.Entry<Long, Path> entry : new ArrayList<>(
        generationFiles.headMap(oldestRetained, true).entrySet())) {
      Files.deleteIfExists(entry.getValue());
      generationFiles.remove(entry.getKey());
    }
    directorySync.sync(generations);
  }

  private void requireSource(Generation expectedSource, Generation preparedSource) {
    if (expectedSource == null || preparedSource == null) {
      if (expectedSource != preparedSource) {
        throw new IllegalArgumentException("State Archive Catalog source identity mismatch");
      }
      return;
    }
    if (!expectedSource.sameSnapshot(preparedSource)) {
      throw new IllegalArgumentException("State Archive Catalog source identity mismatch");
    }
  }

  private void discoverGenerationFiles() throws IOException {
    if (!Files.isDirectory(generations)) {
      return;
    }
    try (Stream<Path> paths = Files.list(generations)) {
      paths.filter(Files::isRegularFile).forEach(path -> {
        Long generation = parseGenerationFileName(path.getFileName().toString());
        if (generation != null) {
          generationFiles.put(generation, path);
        }
      });
    }
  }

  private static Long parseGenerationFileName(String name) {
    if (!name.startsWith("catalog-") || !name.endsWith(".bin")) {
      return null;
    }
    String number = name.substring("catalog-".length(), name.length() - ".bin".length());
    try {
      return number.isEmpty() ? null : Long.parseLong(number);
    } catch (NumberFormatException invalid) {
      return null;
    }
  }

  private static byte[] encodeGeneration(Generation generation) {
    generation.validate();
    int totalLength = StrictMathWrapper.addExact(HEADER_LENGTH + TRAILER_LENGTH,
        StrictMathWrapper.addExact(generation.terminals.size() * CURRENT_RECORD_LENGTH,
            generation.sealed.size() * StateArchiveFileFormatV3.SEGMENT_MAP_ENTRY_LENGTH));
    ByteBuffer bytes = ByteBuffer.allocate(totalLength);
    bytes.putInt(GENERATION_MAGIC);
    bytes.putShort(StateArchiveFileFormatV3.MAJOR_VERSION);
    bytes.putShort(StateArchiveFileFormatV3.MINOR_VERSION);
    bytes.putInt(HEADER_LENGTH);
    bytes.putInt(generation.flags);
    bytes.putLong(totalLength);
    bytes.putLong(generation.generation);
    bytes.putLong(generation.segmentTargetBytes);
    bytes.putInt(generation.terminals.size());
    bytes.putInt(generation.sealed.size());
    bytes.putInt(CURRENT_RECORD_LENGTH);
    bytes.putInt(StateArchiveFileFormatV3.SEGMENT_MAP_ENTRY_LENGTH);
    bytes.put(generation.previousDigest);
    bytes.put(StateArchiveFileFormatV3.compositeFormatDigest());
    bytes.put(StateArchiveFileFormatV3.fiveLaneDescriptorDigest());
    bytes.put(laneSetDigest());
    bytes.put(new byte[72]);
    for (Terminal terminal : generation.terminals) {
      bytes.put(encodeTerminalRecord(terminal, generation.isTerminalAware()));
    }
    for (SealedSegment segment : generation.sealed) {
      bytes.put(StateArchiveSegmentFormatV3.encodeSealedMapRecord(segment));
    }
    int digestOffset = totalLength - TRAILER_LENGTH;
    if (bytes.position() != digestOffset) {
      throw new IllegalStateException("Invalid State Archive Catalog generation layout");
    }
    bytes.put(StateArchiveFileFormatV3.sha256(Arrays.copyOf(bytes.array(), digestOffset)));
    bytes.putLong(totalLength);
    bytes.putInt(crc32c(bytes.array(), 0, totalLength - 8));
    bytes.putInt(GENERATION_TRAILER_MAGIC);
    return bytes.array();
  }

  private static Generation decodeGeneration(byte[] encoded) throws IOException {
    try {
      if (encoded == null || encoded.length < HEADER_LENGTH + TRAILER_LENGTH) {
        throw new IllegalArgumentException("Catalog generation is truncated");
      }
      ByteBuffer bytes = ByteBuffer.wrap(encoded);
      require(bytes.getInt() == GENERATION_MAGIC, "Catalog generation magic mismatch");
      require(bytes.getShort() == StateArchiveFileFormatV3.MAJOR_VERSION,
          "Catalog generation major version mismatch");
      require(bytes.getShort() == StateArchiveFileFormatV3.MINOR_VERSION,
          "Catalog generation minor version mismatch");
      require(bytes.getInt() == HEADER_LENGTH, "Catalog generation header length mismatch");
      int flags = bytes.getInt();
      require((flags & ~KNOWN_GENERATION_FLAGS) == 0,
          "Catalog generation flags mismatch");
      require(bytes.getLong() == encoded.length, "Catalog generation length mismatch");
      long generation = bytes.getLong();
      long segmentTargetBytes = bytes.getLong();
      int currentCount = bytes.getInt();
      int sealedCount = bytes.getInt();
      require(bytes.getInt() == CURRENT_RECORD_LENGTH,
          "Catalog current record length mismatch");
      require(bytes.getInt() == StateArchiveFileFormatV3.SEGMENT_MAP_ENTRY_LENGTH,
          "Catalog sealed record length mismatch");
      byte[] previous = read(bytes, 32);
      require(Arrays.equals(read(bytes, 32), StateArchiveFileFormatV3.compositeFormatDigest()),
          "Catalog format identity mismatch");
      require(Arrays.equals(read(bytes, 32), StateArchiveFileFormatV3.fiveLaneDescriptorDigest()),
          "Catalog descriptor identity mismatch");
      require(Arrays.equals(read(bytes, 32), laneSetDigest()),
          "Catalog lane set identity mismatch");
      requireZero(bytes, 72);
      require(generation >= 0 && segmentTargetBytes > StateArchiveFileFormatV3.PART_HEADER_LENGTH
          && (currentCount == 0
              || currentCount == StateArchiveFileFormatV3.fiveLaneIds().length)
          && sealedCount >= 0, "Catalog generation header is invalid");
      long expectedLength = HEADER_LENGTH + TRAILER_LENGTH
          + (long) currentCount * CURRENT_RECORD_LENGTH
          + (long) sealedCount * StateArchiveFileFormatV3.SEGMENT_MAP_ENTRY_LENGTH;
      require(expectedLength == encoded.length, "Catalog generation record count mismatch");
      List<TerminalRecord> terminalRecords = new ArrayList<>();
      for (int index = 0; index < currentCount; index++) {
        terminalRecords.add(decodeTerminalRecord(read(bytes, CURRENT_RECORD_LENGTH),
            (flags & TERMINAL_AWARE_FLAG) != 0));
      }
      List<SealedSegment> sealed = new ArrayList<>();
      for (int index = 0; index < sealedCount; index++) {
        sealed.add(StateArchiveSegmentFormatV3.decodeSealedMapRecord(
            read(bytes, StateArchiveFileFormatV3.SEGMENT_MAP_ENTRY_LENGTH)));
      }
      int digestOffset = encoded.length - TRAILER_LENGTH;
      byte[] digest = read(bytes, 32);
      require(Arrays.equals(digest,
          StateArchiveFileFormatV3.sha256(Arrays.copyOf(encoded, digestOffset))),
          "Catalog generation digest mismatch");
      require(bytes.getLong() == encoded.length, "Catalog repeated length mismatch");
      require(bytes.getInt() == crc32c(encoded, 0, encoded.length - 8),
          "Catalog generation checksum mismatch");
      require(bytes.getInt() == GENERATION_TRAILER_MAGIC,
          "Catalog generation trailer mismatch");
      List<CurrentSegment> current = new ArrayList<>();
      for (TerminalRecord record : terminalRecords) {
        if (record.kind == TerminalKind.CURRENT) {
          current.add(record.current);
        }
      }
      List<Terminal> terminals = (flags & TERMINAL_AWARE_FLAG) == 0
          ? currentTerminals(current) : resolveTerminals(terminalRecords, sealed);
      return new Generation(generation, previous, segmentTargetBytes, current, sealed, terminals,
          flags, digest);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("State Archive Catalog generation is corrupt", invalid);
    }
  }

  private static byte[] encodeCurrentRecord(CurrentSegment segment) {
    return ByteBuffer.allocate(CURRENT_RECORD_LENGTH)
        .putShort((short) segment.getLaneId())
        .putShort(StateArchiveFileFormatV3.laneKind(segment.getLaneId()))
        .putInt(0).putLong(segment.getSegmentSeq()).putLong(segment.getFirstBlock())
        .putLong(segment.getCurrentLastBlock()).putLong(segment.getDataEndOffset())
        .putLong(segment.getBlockFrameCount()).put(segment.getHeaderDigest())
        .put(parentDigest(segment.getLaneId(), segment.getSegmentSeq())).array();
  }

  private static byte[] encodeTerminalRecord(Terminal terminal, boolean terminalAware) {
    if (terminal.kind == TerminalKind.CURRENT) {
      return encodeCurrentRecord(terminal.current);
    }
    require(terminalAware, "Catalog sealed terminal requires terminal-aware flags");
    return ByteBuffer.allocate(CURRENT_RECORD_LENGTH)
        .putShort((short) terminal.laneId)
        .putShort(StateArchiveFileFormatV3.laneKind(terminal.laneId))
        .putInt(SEALED_TERMINAL_FLAG).putLong(terminal.sealed.getSegmentSeq())
        .put(new byte[96]).array();
  }

  private static TerminalRecord decodeTerminalRecord(byte[] encoded, boolean terminalAware) {
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    int laneId = Short.toUnsignedInt(bytes.getShort());
    require(bytes.getShort() == StateArchiveFileFormatV3.laneKind(laneId),
        "Catalog current lane kind mismatch");
    int flags = bytes.getInt();
    require(flags == 0 || flags == SEALED_TERMINAL_FLAG,
        "Catalog current flags mismatch");
    if (flags == SEALED_TERMINAL_FLAG) {
      require(terminalAware, "Catalog sealed terminal requires terminal-aware flags");
      long sequence = bytes.getLong();
      require(sequence >= 0, "Catalog sealed terminal sequence is invalid");
      requireZero(bytes, 96);
      return TerminalRecord.sealed(laneId, sequence);
    }
    long sequence = bytes.getLong();
    CurrentSegment current = new CurrentSegment(laneId, sequence, bytes.getLong(),
        bytes.getLong(), bytes.getLong(), bytes.getLong(), read(bytes, 32));
    require(Arrays.equals(read(bytes, 32), parentDigest(laneId, sequence)),
        "Catalog current parent digest mismatch");
    return TerminalRecord.current(current);
  }

  private static List<Terminal> resolveTerminals(List<TerminalRecord> records,
      List<SealedSegment> sealed) {
    List<Terminal> terminals = new ArrayList<>();
    for (TerminalRecord record : records) {
      if (record.kind == TerminalKind.CURRENT) {
        terminals.add(Terminal.current(record.current));
        continue;
      }
      SealedSegment finalSegment = finalSealed(sealed, record.laneId);
      require(finalSegment != null && finalSegment.getSegmentSeq() == record.segmentSeq,
          "Catalog sealed terminal is not the final sealed segment");
      terminals.add(Terminal.sealed(finalSegment));
    }
    return terminals;
  }

  private static SealedSegment finalSealed(List<SealedSegment> sealed, int laneId) {
    SealedSegment result = null;
    for (SealedSegment segment : sealed) {
      if (segment.getLaneId() == laneId) {
        result = segment;
      }
    }
    return result;
  }

  private static byte[] parentDigest(int laneId, long sequence) {
    return StateArchiveFileFormatV3.sha256(
        ByteBuffer.allocate(Short.BYTES + Long.BYTES).putShort((short) laneId)
            .putLong(sequence).array());
  }

  private static byte[] laneSetDigest() {
    int[] lanes = StateArchiveFileFormatV3.fiveLaneIds();
    ByteBuffer bytes = ByteBuffer.allocate(lanes.length * Short.BYTES);
    for (int lane : lanes) {
      bytes.putShort((short) lane);
    }
    return StateArchiveFileFormatV3.sha256(bytes.array());
  }

  private static byte[] encodeCurrent(long generation, byte[] digest) {
    ByteBuffer bytes = ByteBuffer.allocate(CURRENT_LENGTH);
    bytes.putInt(CURRENT_MAGIC).putShort(StateArchiveFileFormatV3.MAJOR_VERSION)
        .putShort(StateArchiveFileFormatV3.MINOR_VERSION).putLong(generation).put(digest)
        .put(new byte[36]);
    bytes.putInt(crc32c(bytes.array(), 0, CURRENT_LENGTH - 12));
    bytes.putInt(CURRENT_TRAILER_MAGIC);
    return bytes.array();
  }

  private static CurrentPointer decodeCurrent(byte[] encoded) throws IOException {
    try {
      require(encoded != null && encoded.length == CURRENT_LENGTH,
          "Catalog CURRENT length mismatch");
      ByteBuffer bytes = ByteBuffer.wrap(encoded);
      require(bytes.getInt() == CURRENT_MAGIC, "Catalog CURRENT magic mismatch");
      require(bytes.getShort() == StateArchiveFileFormatV3.MAJOR_VERSION,
          "Catalog CURRENT major version mismatch");
      require(bytes.getShort() == StateArchiveFileFormatV3.MINOR_VERSION,
          "Catalog CURRENT minor version mismatch");
      long generation = bytes.getLong();
      byte[] digest = read(bytes, 32);
      requireZero(bytes, 36);
      require(bytes.getInt() == crc32c(encoded, 0, CURRENT_LENGTH - 12),
          "Catalog CURRENT checksum mismatch");
      require(bytes.getInt() == CURRENT_TRAILER_MAGIC, "Catalog CURRENT trailer mismatch");
      require(generation >= 0, "Catalog CURRENT generation is invalid");
      return new CurrentPointer(generation, digest);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("State Archive Catalog CURRENT is corrupt", invalid);
    }
  }

  private static void writeForced(Path path, byte[] encoded) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer bytes = ByteBuffer.wrap(encoded);
      while (bytes.hasRemaining()) {
        channel.write(bytes);
      }
      channel.force(true);
    }
  }

  private static void atomicMove(Path source, Path target) throws IOException {
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new IOException("State Archive Catalog requires atomic publication", unsupported);
    }
  }

  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static byte[] read(ByteBuffer bytes, int length) {
    byte[] value = new byte[length];
    bytes.get(value);
    return value;
  }

  private static void requireZero(ByteBuffer bytes, int length) {
    for (int index = 0; index < length; index++) {
      require(bytes.get() == 0, "Catalog reserved bytes are non-zero");
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static int crc32c(byte[] bytes, int offset, int length) {
    return Hashing.crc32c().hashBytes(bytes, offset, length).asInt();
  }

  private static String fileName(long generation) {
    return String.format("catalog-%020d.bin", generation);
  }

  static final class PreparedGeneration {
    private final Generation source;
    private final Generation generation;
    private final byte[] encoded;

    private PreparedGeneration(Generation source, Generation generation, byte[] encoded) {
      this.source = source;
      this.generation = generation;
      this.encoded = Arrays.copyOf(encoded, encoded.length);
    }

    long getGeneration() {
      return generation.generation;
    }

    byte[] getDigest() {
      return generation.getDigest();
    }

    byte[] getEncoded() {
      return Arrays.copyOf(encoded, encoded.length);
    }

    Generation getSource() {
      return source;
    }

    Generation getGenerationSnapshot() {
      return generation;
    }
  }

  enum TerminalKind {
    CURRENT,
    SEALED
  }

  static final class Terminal {
    private final int laneId;
    private final TerminalKind kind;
    private final CurrentSegment current;
    private final SealedSegment sealed;

    private Terminal(int laneId, TerminalKind kind, CurrentSegment current,
        SealedSegment sealed) {
      this.laneId = laneId;
      this.kind = kind;
      this.current = current;
      this.sealed = sealed;
    }

    static Terminal current(CurrentSegment segment) {
      return new Terminal(segment.getLaneId(), TerminalKind.CURRENT,
          Objects.requireNonNull(segment), null);
    }

    static Terminal sealed(SealedSegment segment) {
      return new Terminal(segment.getLaneId(), TerminalKind.SEALED, null,
          Objects.requireNonNull(segment));
    }

    int getLaneId() {
      return laneId;
    }

    TerminalKind getKind() {
      return kind;
    }

    CurrentSegment getCurrent() {
      return current;
    }

    SealedSegment getSealed() {
      return sealed;
    }
  }

  private static final class TerminalRecord {
    private final int laneId;
    private final TerminalKind kind;
    private final long segmentSeq;
    private final CurrentSegment current;

    private TerminalRecord(int laneId, TerminalKind kind, long segmentSeq,
        CurrentSegment current) {
      this.laneId = laneId;
      this.kind = kind;
      this.segmentSeq = segmentSeq;
      this.current = current;
    }

    private static TerminalRecord current(CurrentSegment segment) {
      return new TerminalRecord(segment.getLaneId(), TerminalKind.CURRENT,
          segment.getSegmentSeq(), segment);
    }

    private static TerminalRecord sealed(int laneId, long segmentSeq) {
      return new TerminalRecord(laneId, TerminalKind.SEALED, segmentSeq, null);
    }
  }

  static final class Generation {
    private final long generation;
    private final byte[] previousDigest;
    private final long segmentTargetBytes;
    private final List<CurrentSegment> current;
    private final List<SealedSegment> sealed;
    private final List<Terminal> terminals;
    private final int flags;
    private final byte[] digest;

    private Generation(long generation, byte[] previousDigest, long segmentTargetBytes,
        List<CurrentSegment> current, List<SealedSegment> sealed, byte[] digest) {
      this(generation, previousDigest, segmentTargetBytes, current, sealed,
          currentTerminals(current), 0, digest);
    }

    private Generation(long generation, byte[] previousDigest, long segmentTargetBytes,
        List<CurrentSegment> current, List<SealedSegment> sealed, List<Terminal> terminals,
        int flags, byte[] digest) {
      this.generation = generation;
      this.previousDigest = Arrays.copyOf(Objects.requireNonNull(previousDigest), 32);
      this.segmentTargetBytes = segmentTargetBytes;
      this.current = sortedCurrent(current);
      this.sealed = sortedSealed(sealed);
      this.terminals = sortedTerminals(terminals);
      this.flags = flags;
      this.digest = digest == null ? null : Arrays.copyOf(digest, digest.length);
      validate();
    }

    private boolean isTerminalAware() {
      return (flags & TERMINAL_AWARE_FLAG) != 0;
    }

    private void validate() {
      require((flags & ~KNOWN_GENERATION_FLAGS) == 0,
          "Catalog generation flags mismatch");
      require(previousDigest.length == 32 && (digest == null || digest.length == 32),
          "Catalog digest length mismatch");
      int[] expected = StateArchiveFileFormatV3.fiveLaneIds();
      if (!isTerminalAware()) {
        require(current.isEmpty() || current.size() == expected.length,
            "Catalog must contain zero or five current lanes");
        require(!current.isEmpty() || sealed.isEmpty(),
            "Catalog cannot omit current lanes after sealed history");
        for (int index = 0; index < expected.length; index++) {
          require(current.isEmpty() || current.get(index).getLaneId() == expected[index],
              "Catalog current lane set mismatch");
        }
        validateLegacyTerminals();
      } else {
        require(terminals.isEmpty()
                || terminals.size() == StateArchiveFileFormatV3.fiveLaneIds().length,
            "Catalog terminal set must be empty or complete");
        require(current.size() <= expected.length,
            "Catalog current terminal count is invalid");
        int previousCurrentLane = -1;
        for (CurrentSegment segment : current) {
          require(segment.getLaneId() != previousCurrentLane,
              "Catalog current lane is duplicated");
          previousCurrentLane = segment.getLaneId();
        }
      }
      validateSealedSequence();
      validateCurrentSequence();
      if (isTerminalAware() && (!current.isEmpty() || !sealed.isEmpty())) {
        require(terminals.size() == expected.length,
            "Catalog non-empty terminal set is incomplete");
      }
      if (isTerminalAware()) {
        validateTerminals();
      }
    }

    private void validateSealedSequence() {
      int previousLane = -1;
      long previousSequence = -1;
      long previousLast = -1;
      for (SealedSegment segment : sealed) {
        if (segment.getLaneId() != previousLane) {
          previousLane = segment.getLaneId();
          previousSequence = -1;
          previousLast = -1;
        }
        require(segment.getSegmentSeq() == previousSequence + 1,
            "Catalog sealed sequence has a gap");
        if (previousLast >= 0) {
          require(segment.getFirstBlock() == previousLast + 1,
              "Catalog sealed block range has a gap");
        }
        previousSequence = segment.getSegmentSeq();
        previousLast = segment.getLastBlock();
      }
    }

    private void validateCurrentSequence() {
      for (CurrentSegment segment : current) {
        SealedSegment last = finalSealed(sealed, segment.getLaneId());
        long lastSequence = last == null ? -1 : last.getSegmentSeq();
        require(segment.getSegmentSeq() == lastSequence + 1,
            "Catalog current sequence does not follow sealed segments");
        if (last != null) {
          require(segment.getFirstBlock() == last.getLastBlock() + 1,
              "Catalog current block range does not follow sealed segments");
        }
      }
    }

    private void validateTerminals() {
      int previousLane = -1;
      int currentTerminalCount = 0;
      long logicalHead = -1;
      for (Terminal terminal : terminals) {
        require(terminal.laneId != previousLane,
            "Catalog terminal lane is duplicated");
        previousLane = terminal.laneId;
        if (terminal.kind == TerminalKind.CURRENT) {
          currentTerminalCount++;
          CurrentSegment same = findCurrent(terminal.laneId);
          require(same != null && Arrays.equals(encodeCurrentRecord(same),
              encodeCurrentRecord(terminal.current)),
              "Catalog current terminal mismatch");
          requireLogicalHead(terminal.current.getCurrentLastBlock(), logicalHead);
          logicalHead = terminal.current.getCurrentLastBlock();
        } else {
          SealedSegment finalSegment = finalSealed(sealed, terminal.laneId);
          require(finalSegment != null && Arrays.equals(
              StateArchiveSegmentFormatV3.encodeSealedMapRecord(finalSegment),
              StateArchiveSegmentFormatV3.encodeSealedMapRecord(terminal.sealed)),
              "Catalog sealed terminal mismatch");
          requireLogicalHead(terminal.sealed.getLastBlock(), logicalHead);
          logicalHead = terminal.sealed.getLastBlock();
        }
      }
      require(currentTerminalCount == current.size(),
          "Catalog current terminal projection is not bijective");
      for (CurrentSegment segment : current) {
        Terminal terminal = findTerminal(segment.getLaneId());
        require(terminal != null && terminal.kind == TerminalKind.CURRENT,
            "Catalog current segment is not terminal");
      }
    }

    private static void requireLogicalHead(long head, long expected) {
      require(expected < 0 || head == expected,
          "Catalog terminal logical heads do not agree");
    }

    private void validateLegacyTerminals() {
      List<Terminal> expected = currentTerminals(current);
      require(terminals.size() == expected.size(), "Catalog legacy terminals mismatch");
      for (int index = 0; index < expected.size(); index++) {
        require(terminals.get(index).kind == TerminalKind.CURRENT
                && terminals.get(index).laneId == expected.get(index).laneId
                && Arrays.equals(encodeCurrentRecord(terminals.get(index).current),
                    encodeCurrentRecord(expected.get(index).current)),
            "Catalog legacy terminals mismatch");
      }
    }

    private CurrentSegment findCurrent(int laneId) {
      for (CurrentSegment segment : current) {
        if (segment.getLaneId() == laneId) {
          return segment;
        }
      }
      return null;
    }

    private Terminal findTerminal(int laneId) {
      for (Terminal terminal : terminals) {
        if (terminal.laneId == laneId) {
          return terminal;
        }
      }
      return null;
    }

    long getGeneration() {
      return generation;
    }

    List<CurrentSegment> getCurrent() {
      return current;
    }

    List<SealedSegment> getSealed() {
      return sealed;
    }

    List<Terminal> getTerminals() {
      return terminals;
    }

    Terminal terminalForLane(int laneId) {
      Terminal terminal = findTerminal(laneId);
      if (terminal == null) {
        throw new IllegalArgumentException("Catalog terminal lane is missing");
      }
      return terminal;
    }

    boolean sameSnapshot(Generation other) {
      return other != null && digest != null && Arrays.equals(digest, other.digest);
    }

    byte[] getDigest() {
      return digest == null ? null : Arrays.copyOf(digest, digest.length);
    }
  }

  private static List<CurrentSegment> sortedCurrent(List<CurrentSegment> input) {
    List<CurrentSegment> copy = new ArrayList<>(Objects.requireNonNull(input));
    copy.sort(Comparator.comparingInt(CurrentSegment::getLaneId));
    return Collections.unmodifiableList(copy);
  }

  private static List<Terminal> currentTerminals(List<CurrentSegment> current) {
    List<Terminal> terminals = new ArrayList<>();
    for (CurrentSegment segment : current) {
      terminals.add(Terminal.current(segment));
    }
    return sortedTerminals(terminals);
  }

  private static List<Terminal> sortedTerminals(List<Terminal> input) {
    List<Terminal> copy = new ArrayList<>(Objects.requireNonNull(input));
    copy.sort(Comparator.comparingInt(Terminal::getLaneId));
    return Collections.unmodifiableList(copy);
  }

  private static List<SealedSegment> sortedSealed(List<SealedSegment> input) {
    List<SealedSegment> copy = new ArrayList<>(Objects.requireNonNull(input));
    copy.sort(Comparator.comparingInt(SealedSegment::getLaneId)
        .thenComparingLong(SealedSegment::getSegmentSeq));
    return Collections.unmodifiableList(copy);
  }

  private static final class CurrentPointer {
    private final long generation;
    private final byte[] digest;

    private CurrentPointer(long generation, byte[] digest) {
      this.generation = generation;
      this.digest = digest;
    }
  }
}
