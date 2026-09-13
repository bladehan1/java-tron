package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Durable generation catalog with request refcounts and safe retired-generation reaping. */
public final class PersistentServingKeyIndexCatalog implements Closeable {

  private static final int MAGIC = 0x534b4943; // SKIC
  private static final short VERSION = 1;
  private static final String CURRENT = "current";
  private static final String CURRENT_TEMP = "current.tmp";
  private static final String GENERATIONS = "generations";

  private final Path root;
  private final Path generations;
  private final Engine engine;
  private final Map<String, Integer> references = new HashMap<>();
  private final Set<String> retired = new HashSet<>();
  private final FaultHook faultHook;
  private String currentId;
  private boolean closed;

  private PersistentServingKeyIndexCatalog(Path root, String currentId, Engine engine,
      FaultHook faultHook) {
    this.root = root;
    this.generations = root.resolve(GENERATIONS);
    this.engine = Objects.requireNonNull(engine, "engine");
    this.currentId = currentId;
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  public static PersistentServingKeyIndexCatalog open(Path root) throws IOException {
    return open(root, stage -> { });
  }

  static PersistentServingKeyIndexCatalog open(Path root, FaultHook faultHook)
      throws IOException {
    Objects.requireNonNull(root, "root");
    String currentId = readCurrent(root);
    Path current = root.resolve(GENERATIONS).resolve(currentId);
    Engine engine = StateArchiveIndexEngineManifest.load(current);
    return open(root, engine, faultHook);
  }

  static PersistentServingKeyIndexCatalog open(Path root, Engine engine, FaultHook faultHook)
      throws IOException {
    Objects.requireNonNull(root, "root");
    String currentId = readCurrent(root);
    Path current = root.resolve(GENERATIONS).resolve(currentId);
    try (PersistentServingKeyIndexGeneration ignored =
        PersistentServingKeyIndexGeneration.open(current, engine)) {
      // Opening validates both the immutable descriptor and configured native generation.
    }
    PersistentServingKeyIndexCatalog catalog =
        new PersistentServingKeyIndexCatalog(root, currentId, engine, faultHook);
    catalog.discoverRetired();
    return catalog;
  }

  public static PersistentServingKeyIndexCatalog create(Path root, Path initialShadow,
      ArchiveProgressEnvelope readerVisible) throws IOException {
    return createInternal(root, initialShadow, readerVisible);
  }

  public static PersistentServingKeyIndexCatalog create(Path root, Path initialShadow)
      throws IOException {
    return createInternal(root, initialShadow, null);
  }

  static PersistentServingKeyIndexCatalog create(Path root, Path initialShadow,
      FaultHook faultHook) throws IOException {
    Objects.requireNonNull(faultHook, "faultHook");
    Objects.requireNonNull(root, "root");
    if (Files.exists(root.resolve(CURRENT))) {
      throw new IllegalArgumentException("Serving index catalog already exists");
    }
    Files.createDirectories(root.resolve(GENERATIONS));
    Engine engine = StateArchiveIndexEngineManifest.load(initialShadow);
    PersistentServingKeyIndexCatalog catalog =
        new PersistentServingKeyIndexCatalog(root, null, engine, faultHook);
    if (!catalog.publishInternal(null, initialShadow, null)) {
      throw new IllegalStateException("Failed to publish initial serving generation");
    }
    return catalog;
  }

  private static PersistentServingKeyIndexCatalog createInternal(Path root, Path initialShadow,
      ArchiveProgressEnvelope readerVisible) throws IOException {
    Objects.requireNonNull(root, "root");
    if (Files.exists(root.resolve(CURRENT))) {
      throw new IllegalArgumentException("Serving index catalog already exists");
    }
    Files.createDirectories(root.resolve(GENERATIONS));
    Engine engine = StateArchiveIndexEngineManifest.load(initialShadow);
    PersistentServingKeyIndexCatalog catalog =
        new PersistentServingKeyIndexCatalog(root, null, engine, stage -> { });
    if (!catalog.publishInternal(null, initialShadow, readerVisible)) {
      throw new IllegalStateException("Failed to publish initial serving generation");
    }
    return catalog;
  }

  /** Pins one immutable native generation handle and holds its refcount until close. */
  public synchronized PersistentServingKeyIndexGeneration pin(
      ArchiveProgressEnvelope readerVisible) throws IOException {
    return pinInternal(readerVisible);
  }

  public synchronized PersistentServingKeyIndexGeneration pin() throws IOException {
    return pinInternal(null);
  }

  private PersistentServingKeyIndexGeneration pinInternal(
      ArchiveProgressEnvelope readerVisible) throws IOException {
    ensureOpen();
    if (currentId == null) {
      throw new ArchivePersistenceException("Serving index catalog has no current generation");
    }
    String pinnedId = currentId;
    references.put(pinnedId, references.getOrDefault(pinnedId, 0) + 1);
    PersistentServingKeyIndexGeneration pinned;
    try {
      pinned = PersistentServingKeyIndexGeneration.openTrusted(generations.resolve(pinnedId),
          engine,
          () -> release(pinnedId));
    } catch (IOException | RuntimeException failure) {
      release(pinnedId);
      throw failure;
    }
    try {
      if (readerVisible != null) {
        validateReaderVisibility(pinned, readerVisible);
      }
      return pinned;
    } catch (RuntimeException failure) {
      pinned.close();
      throw failure;
    }
  }

  /** Atomically publishes a completed shadow generation if {@code expectedId} is still current. */
  public synchronized boolean publish(String expectedId, Path shadow,
      ArchiveProgressEnvelope readerVisible) throws IOException {
    return publishInternal(expectedId, shadow, readerVisible);
  }

  public synchronized boolean publish(String expectedId, Path shadow) throws IOException {
    return publishInternal(expectedId, shadow, null);
  }

  private boolean publishInternal(String expectedId, Path shadow,
      ArchiveProgressEnvelope readerVisible) throws IOException {
    ensureOpen();
    discoverRetired();
    if (!Objects.equals(expectedId, currentId)) {
      return false;
    }
    String replacementId;
    long replacementFrom;
    long replacementThrough;
    try (PersistentServingKeyIndexGeneration replacement =
        PersistentServingKeyIndexGeneration.open(shadow, engine)) {
      if (readerVisible != null) {
        validateReaderVisibility(replacement, readerVisible);
      }
      replacementId = replacement.getGenerationId();
      replacementFrom = replacement.getIndexedFrom();
      replacementThrough = replacement.getIndexedThrough();
    }
    validateGenerationId(replacementId);
    if (currentId != null) {
      try (PersistentServingKeyIndexGeneration current =
          PersistentServingKeyIndexGeneration.open(generations.resolve(currentId), engine)) {
        if (replacementFrom != current.getIndexedFrom()
            || replacementThrough < current.getIndexedThrough()) {
          throw new IllegalArgumentException("Serving generation publication regresses coverage");
        }
      }
    }
    Path destination = generations.resolve(replacementId);
    if (Files.exists(destination)) {
      throw new IllegalArgumentException("Serving generation already exists: " + replacementId);
    }
    try {
      Files.move(shadow, destination, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Serving index filesystem does not support atomic generation install", unsupported);
    }
    HistorySegmentStore.syncDirectory(generations);
    faultHook.afterDurableStage(PublicationStage.GENERATION_INSTALLED);
    persistCurrent(replacementId);
    String previous = currentId;
    currentId = replacementId;
    if (previous != null) {
      retired.add(previous);
    }
    faultHook.afterDurableStage(PublicationStage.CURRENT_PUBLISHED);
    if (previous != null) {
      reapIfUnused(previous);
    }
    return true;
  }

  public synchronized String getCurrentGenerationId() {
    ensureOpen();
    return currentId;
  }

  public synchronized int getReferenceCount(String generationId) {
    return references.getOrDefault(generationId, 0);
  }

  public synchronized boolean generationExists(String generationId) {
    return Files.isDirectory(generations.resolve(generationId));
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    if (!references.isEmpty()) {
      throw new IllegalStateException("Serving index catalog closed with pinned generations");
    }
    closed = true;
    for (String generation : new ArrayList<>(retired)) {
      reapIfUnused(generation);
    }
  }

  private synchronized void release(String generationId) {
    Integer count = references.get(generationId);
    if (count == null || count <= 0) {
      throw new IllegalStateException("Serving generation refcount underflow");
    }
    if (count == 1) {
      references.remove(generationId);
      if (retired.contains(generationId)) {
        try {
          reapIfUnused(generationId);
        } catch (IOException failure) {
          throw new ArchivePersistenceException("Failed to reap serving generation", failure);
        }
      }
    } else {
      references.put(generationId, count - 1);
    }
  }

  private void discoverRetired() throws IOException {
    Files.createDirectories(generations);
    try (Stream<Path> entries = Files.list(generations)) {
      entries.filter(Files::isDirectory)
          .map(path -> path.getFileName().toString())
          .filter(id -> !id.equals(currentId))
          .forEach(retired::add);
    }
    for (String generation : new ArrayList<>(retired)) {
      reapIfUnused(generation);
    }
  }

  private void reapIfUnused(String generationId) throws IOException {
    if (generationId.equals(currentId) || references.getOrDefault(generationId, 0) != 0) {
      return;
    }
    Path target = generations.resolve(generationId);
    if (Files.exists(target)) {
      List<Path> paths = new ArrayList<>();
      try (Stream<Path> walk = Files.walk(target)) {
        walk.sorted(Comparator.reverseOrder()).forEach(paths::add);
      }
      for (Path path : paths) {
        Files.deleteIfExists(path);
      }
      HistorySegmentStore.syncDirectory(generations);
    }
    retired.remove(generationId);
  }

  private void persistCurrent(String generationId) throws IOException {
    byte[] encoded = encodeCurrent(generationId);
    Path temporary = root.resolve(CURRENT_TEMP);
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
    try {
      Files.move(temporary, root.resolve(CURRENT), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Serving index filesystem does not support atomic catalog publication", unsupported);
    }
    HistorySegmentStore.syncDirectory(root);
  }

  private static String readCurrent(Path root) throws IOException {
    Path current = root.resolve(CURRENT);
    if (!Files.isRegularFile(current)) {
      throw new ArchivePersistenceException("Serving index current-generation pointer is missing");
    }
    byte[] encoded = Files.readAllBytes(current);
    if (encoded.length < 16) {
      throw new ArchivePersistenceException("Serving index current pointer is corrupt");
    }
    byte[] payload = java.util.Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new ArchivePersistenceException("Serving index current pointer checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new ArchivePersistenceException("Unsupported serving index current pointer");
      }
      String generationId = input.readUTF();
      if (input.available() != Integer.BYTES) {
        throw new ArchivePersistenceException("Serving index current pointer payload mismatch");
      }
      validateGenerationId(generationId);
      return generationId;
    } catch (IOException invalid) {
      throw new ArchivePersistenceException("Serving index current pointer is truncated", invalid);
    }
  }

  private static byte[] encodeCurrent(String generationId) {
    validateGenerationId(generationId);
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeUTF(generationId);
      output.flush();
      byte[] payload = bytes.toByteArray();
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected serving catalog encoding failure", impossible);
    }
  }

  private static void validateGenerationId(String generationId) {
    if (generationId == null || generationId.isEmpty() || generationId.length() > 128
        || generationId.contains("/") || generationId.contains("\\")
        || generationId.equals(".") || generationId.equals("..")) {
      throw new IllegalArgumentException("Invalid serving generation id");
    }
  }

  private static void validateReaderVisibility(PersistentServingKeyIndexGeneration generation,
      ArchiveProgressEnvelope readerVisible) {
    Objects.requireNonNull(readerVisible, "readerVisible");
    if (readerVisible.getKind() != ArchiveProgressEnvelope.Kind.READER_VISIBLE
        || !readerVisible.getScopeIdentity().equals(generation.getScopeIdentity())
        || !readerVisible.getParticipants().equals(generation.getParticipatingDatabases())
        || generation.getIndexedThrough() > readerVisible.getEpoch()
        || generation.getIndexedThrough() == readerVisible.getEpoch()
        && !java.util.Arrays.equals(generation.getHeadHash(), readerVisible.getBlockHash())) {
      throw new ArchivePersistenceException(
          "Serving generation is outside reader-visible recovery authority");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Serving index catalog is closed");
    }
  }

  enum PublicationStage {
    GENERATION_INSTALLED,
    CURRENT_PUBLISHED
  }

  @FunctionalInterface
  interface FaultHook {
    void afterDurableStage(PublicationStage stage) throws IOException;
  }
}
