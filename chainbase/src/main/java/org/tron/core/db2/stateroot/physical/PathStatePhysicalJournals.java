package org.tron.core.db2.stateroot.physical;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.stateroot.PathStateLayerLimits;
import org.tron.core.db2.stateroot.format.PathStateRootMetadata;
import org.tron.core.db2.stateroot.physical.PathStatePhysicalRecords.GlobalIntent;
import org.tron.core.db2.stateroot.physical.PathStatePhysicalRecords.OracleWindow;
import org.tron.core.db2.stateroot.physical.PathStatePhysicalRecords.ReverseJournal;

/** Journal index, ancestry and retention; its StoreSet owner supplies durable file operations. */
public final class PathStatePhysicalJournals {
  public static final String DIRECTORY = "reverse";
  private static final String REVERSE_DIRECTORY = DIRECTORY;
  private final Path directory;
  private final JournalFiles files;
  private Map<BytesKey, ReverseJournalIndexEntry> reverseJournalIndex;

  public PathStatePhysicalJournals(Path directory, JournalFiles files) {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.files = Objects.requireNonNull(files, "files");
  }

  /** Called under the StoreSet owner's monitor; the full on-disk index replaces the cache. */
  public void verify(PathStateLayerLimits limits) throws IOException {
    reverseJournalIndex = loadReverseJournalIndex(limits);
  }

  public List<ReverseJournal> loadRewindChain(
      GlobalIntent current, long targetNumber, byte[] targetHash, PathStateLayerLimits limits)
      throws IOException {
    if (targetNumber < 0 || targetNumber > current.getMetadata().getBlockNumber()) {
      throw new IOException("physical rewind target height is outside CURRENT ancestry");
    }
    Map<BytesKey, ReverseJournalIndexEntry> journals = reverseJournalIndex(limits);
    List<ReverseJournal> chain = new ArrayList<>();
    GlobalIntent cursor = current;
    while (cursor.getMetadata().getBlockNumber() > targetNumber) {
      ReverseJournalIndexEntry entry = journals.get(new BytesKey(cursor.encode()));
      if (entry == null) {
        throw new IOException("physical reverse journal ancestry is missing");
      }
      ReverseJournal journal = loadReverseJournal(entry.path);
      chain.add(journal);
      cursor = GlobalIntent.decode(entry.parentTarget);
    }
    if (cursor.getMetadata().getBlockNumber() != targetNumber
        || !Arrays.equals(cursor.getMetadata().getBlockHash(), targetHash)) {
      throw new IOException("physical rewind target is not an exact ancestor");
    }
    return chain;
  }

  private Map<BytesKey, ReverseJournalIndexEntry> loadReverseJournalIndex(
      PathStateLayerLimits limits) throws IOException {
    Map<BytesKey, ReverseJournalIndexEntry> journals = new LinkedHashMap<>();
    Path reverse = directory.resolve(REVERSE_DIRECTORY);
    if (!Files.exists(reverse, LinkOption.NOFOLLOW_LINKS)) {
      return journals;
    }
    if (!Files.isDirectory(reverse, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(reverse)) {
      throw new IOException("physical reverse journal root is not a direct directory");
    }
    long total = 0;
    int count = 0;
    try (Stream<Path> files = Files.list(reverse)) {
      for (Path file : (Iterable<Path>) files::iterator) {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
          throw new IOException("physical reverse journal is not a regular file: " + file);
        }
        long length = Files.size(file);
        total = StrictMathWrapper.addExact(total, length);
        count = StrictMathWrapper.addExact(count, 1);
        ReverseJournal journal = loadReverseJournal(file);
        ReverseJournalIndexEntry entry =
            new ReverseJournalIndexEntry(
                file, length, journal.getChildTarget(), journal.getParentTarget());
        if (journals.put(new BytesKey(entry.childTarget), entry) != null) {
          throw new IOException("physical reverse journal child identity is duplicated");
        }
      }
    } catch (ArithmeticException overflow) {
      throw new IOException("physical reverse journal usage overflow", overflow);
    }
    if (count > limits.getMaxLayers() || total > limits.getMaxLogicalBytes()) {
      throw new IOException("physical reverse journal exceeds configured bounds");
    }
    return journals;
  }

  private ReverseJournal loadReverseJournal(Path file) throws IOException {
    try {
      return ReverseJournal.decode(
          files.loadImmutableBytes(file, ReverseJournal.MAX_ENCODED_LENGTH));
    } catch (IllegalArgumentException invalid) {
      throw new IOException("physical reverse journal is corrupt: " + file, invalid);
    }
  }

  private Map<BytesKey, ReverseJournalIndexEntry> reverseJournalIndex(PathStateLayerLimits limits)
      throws IOException {
    if (reverseJournalIndex == null) {
      reverseJournalIndex = loadReverseJournalIndex(limits);
    } else {
      requireReverseJournalLimits(reverseJournalIndex, limits);
    }
    return reverseJournalIndex;
  }

  private static void requireReverseJournalLimits(
      Map<BytesKey, ReverseJournalIndexEntry> journals, PathStateLayerLimits limits)
      throws IOException {
    long total = 0;
    try {
      for (ReverseJournalIndexEntry entry : journals.values()) {
        total = StrictMathWrapper.addExact(total, entry.length);
      }
    } catch (ArithmeticException overflow) {
      throw new IOException("physical reverse journal usage overflow", overflow);
    }
    if (journals.size() > limits.getMaxLayers() || total > limits.getMaxLogicalBytes()) {
      throw new IOException("physical reverse journal exceeds configured bounds");
    }
  }

  public void rememberReverseJournal(ReverseJournal journal, Path path, long length)
      throws IOException {
    Map<BytesKey, ReverseJournalIndexEntry> journals =
        reverseJournalIndex(new PathStateLayerLimits(Integer.MAX_VALUE, Long.MAX_VALUE));
    ReverseJournalIndexEntry entry =
        new ReverseJournalIndexEntry(
            path, length, journal.getChildTarget(), journal.getParentTarget());
    BytesKey identity = new BytesKey(entry.childTarget);
    ReverseJournalIndexEntry previous = journals.put(identity, entry);
    if (previous != null && !previous.path.equals(entry.path)) {
      journals.put(identity, previous);
      throw new IOException("physical reverse journal child identity is duplicated");
    }
  }

  public int reverseJournalCount() {
    return reverseJournalIndex == null ? 0 : reverseJournalIndex.size();
  }

  public long reverseJournalBytes() throws IOException {
    if (reverseJournalIndex == null) {
      return 0;
    }
    long total = 0;
    try {
      for (ReverseJournalIndexEntry entry : reverseJournalIndex.values()) {
        total = StrictMathWrapper.addExact(total, entry.length);
      }
      return total;
    } catch (ArithmeticException overflow) {
      throw new IOException("physical reverse journal usage overflow", overflow);
    }
  }

  public void pruneReverseJournals(
      GlobalIntent current, PathStateLayerLimits limits, Path candidate, long candidateBytes)
      throws IOException {
    if (candidateBytes > limits.getMaxLogicalBytes()) {
      throw new IOException("physical reverse journal candidate exceeds byte limit");
    }
    Path reverse = directory.resolve(REVERSE_DIRECTORY);
    if (!Files.exists(reverse, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    Map<BytesKey, ReverseJournalIndexEntry> indexed =
        reverseJournalIndex(new PathStateLayerLimits(Integer.MAX_VALUE, Long.MAX_VALUE));
    Set<BytesKey> keep = new HashSet<>();
    byte[] cursor = current.encode();
    int remainingCount = limits.getMaxLayers() - 1;
    long remainingBytes = limits.getMaxLogicalBytes() - candidateBytes;
    while (remainingCount > 0) {
      BytesKey identity = new BytesKey(cursor);
      ReverseJournalIndexEntry entry = indexed.get(identity);
      if (entry == null) {
        break;
      }
      if (entry.length > remainingBytes) {
        break;
      }
      keep.add(identity);
      remainingCount--;
      remainingBytes -= entry.length;
      cursor = entry.parentTarget;
    }
    java.util.Iterator<Map.Entry<BytesKey, ReverseJournalIndexEntry>> iterator =
        indexed.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<BytesKey, ReverseJournalIndexEntry> indexedJournal = iterator.next();
      ReverseJournalIndexEntry entry = indexedJournal.getValue();
      if (!entry.path.equals(candidate) && !keep.contains(indexedJournal.getKey())) {
        files.deleteDurable(entry.path);
        iterator.remove();
      }
    }
  }

  public Path reverseJournalPath(PathStateRootMetadata child) {
    StringBuilder hash = new StringBuilder(child.getBlockHash().length * 2);
    for (byte value : child.getBlockHash()) {
      hash.append(String.format("%02x", value & 0xff));
    }
    return directory
        .resolve(REVERSE_DIRECTORY)
        .resolve(String.format("%020d-%s.journal", child.getBlockNumber(), hash));
  }

  public OracleWindow loadOracleWindow(
      GlobalIntent current, int blockCount, PathStateLayerLimits limits) throws IOException {
    Map<BytesKey, ReverseJournalIndexEntry> indexed = loadReverseJournalIndex(limits);
    List<ReverseJournal> journals = new ArrayList<>(blockCount);
    GlobalIntent cursor = current;
    for (int index = 0; index < blockCount; index++) {
      ReverseJournalIndexEntry entry = indexed.get(new BytesKey(cursor.encode()));
      if (entry == null) {
        throw new IOException(
            "physical oracle fixed window ancestry is missing at height "
                + cursor.getMetadata().getBlockNumber());
      }
      ReverseJournal journal = loadReverseJournal(entry.path);
      if (!Arrays.equals(cursor.encode(), journal.getChildTarget())) {
        throw new IOException("physical oracle journal child differs from canonical cursor");
      }
      journals.add(journal);
      cursor = GlobalIntent.decode(journal.getParentTarget());
    }
    return PathStatePhysicalRecords.oracleWindow(current.encode(), journals);
  }

  private static final class BytesKey {

    private final byte[] bytes;

    private BytesKey(byte[] bytes) {
      this.bytes = Arrays.copyOf(Objects.requireNonNull(bytes, "bytes"), bytes.length);
    }

    @Override
    public boolean equals(Object other) {
      return this == other
          || other instanceof BytesKey && Arrays.equals(bytes, ((BytesKey) other).bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  private static final class ReverseJournalIndexEntry {

    private final Path path;
    private final long length;
    private final byte[] childTarget;
    private final byte[] parentTarget;

    private ReverseJournalIndexEntry(
        Path path, long length, byte[] childTarget, byte[] parentTarget) {
      this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
      if (length <= 0) {
        throw new IllegalArgumentException("reverse journal length must be positive");
      }
      this.length = length;
      this.childTarget =
          Arrays.copyOf(Objects.requireNonNull(childTarget, "childTarget"), childTarget.length);
      this.parentTarget =
          Arrays.copyOf(Objects.requireNonNull(parentTarget, "parentTarget"), parentTarget.length);
    }
  }

  /** The owner retains actual durability and failure semantics; no native handle is exported. */
  public interface JournalFiles {
    byte[] loadImmutableBytes(Path path, int maxLength) throws IOException;

    void deleteDurable(Path path) throws IOException;
  }
}
