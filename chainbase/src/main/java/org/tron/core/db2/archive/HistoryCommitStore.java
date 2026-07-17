package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Atomic, directory-synced history visibility markers. */
public final class HistoryCommitStore implements Closeable {

  private static final String SUFFIX = ".commit";

  private final Path directory;
  private final HistoryCommitMarkerCodec codec;
  private final DirectorySync directorySync;
  private final List<HistoryCommitMarker> markers;
  private final Map<Long, HistoryCommitMarker> markersByEpoch = new HashMap<>();
  private HistoryCommitMarker uncertainMarker;

  public HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec)
      throws IOException {
    this(archiveDirectory, codec, HistorySegmentStore::syncDirectory);
  }

  HistoryCommitStore(Path archiveDirectory, HistoryCommitMarkerCodec codec,
      DirectorySync directorySync) throws IOException {
    this.directory = archiveDirectory.resolve("commits");
    this.codec = codec;
    this.directorySync = directorySync;
    Files.createDirectories(directory);
    markers = scan();
    markers.forEach(marker -> markersByEpoch.put(marker.getMeta().getEpoch(), marker));
  }

  public synchronized void commit(HistoryCommitMarker marker) throws IOException {
    if (uncertainMarker != null) {
      throw new IllegalStateException("A previous commit marker has uncertain durability");
    }
    validateNext(head(), marker);
    byte[] encoded = codec.encode(marker);
    Path target = markerPath(marker.getMeta().getEpoch());
    if (Files.exists(target)) {
      byte[] existing = Files.readAllBytes(target);
      if (Arrays.equals(existing, encoded)) {
        return;
      }
      throw new IllegalStateException("Conflicting history commit marker for epoch "
          + marker.getMeta().getEpoch());
    }

    Path temporary = directory.resolve(".tmp-" + marker.getMeta().getEpoch() + '-'
        + UUID.randomUUID());
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)) {
      writeFully(channel, ByteBuffer.wrap(encoded));
      channel.force(true);
    }
    try {
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.deleteIfExists(temporary);
      throw new IOException("Atomic commit-marker move is not supported", e);
    }
    uncertainMarker = marker;
    directorySync.sync(directory);
    markers.add(marker);
    markersByEpoch.put(marker.getMeta().getEpoch(), marker);
    uncertainMarker = null;
  }

  public synchronized void removeHead(BlockSnapshotMeta expected) throws IOException {
    if (uncertainMarker != null) {
      throw new IllegalStateException("Cannot revert a commit marker with uncertain durability");
    }
    HistoryCommitMarker head = head();
    if (head == null || !head.getMeta().equals(expected)) {
      throw new IllegalStateException("Only the committed history head can be reverted");
    }
    Files.delete(markerPath(expected.getEpoch()));
    directorySync.sync(directory);
    markers.remove(markers.size() - 1);
    markersByEpoch.remove(expected.getEpoch());
  }

  public synchronized HistoryCommitMarker head() {
    return markers.isEmpty() ? null : markers.get(markers.size() - 1);
  }

  public synchronized List<HistoryCommitMarker> getMarkers() {
    return new ArrayList<>(markers);
  }

  public synchronized HistoryCommitMarker get(long epoch) {
    return markersByEpoch.get(epoch);
  }

  public synchronized boolean mayContain(long epoch) {
    return markersByEpoch.containsKey(epoch)
        || (uncertainMarker != null && uncertainMarker.getMeta().getEpoch() == epoch);
  }

  private List<HistoryCommitMarker> scan() throws IOException {
    List<Path> paths = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*" + SUFFIX)) {
      for (Path path : stream) {
        paths.add(path);
      }
    }
    paths.sort(Comparator.comparingLong(HistoryCommitStore::parseEpoch));
    List<HistoryCommitMarker> decoded = new ArrayList<>();
    HistoryCommitMarker previous = null;
    for (Path path : paths) {
      HistoryCommitMarker marker = codec.decode(Files.readAllBytes(path));
      if (parseEpoch(path) != marker.getMeta().getEpoch()) {
        throw new IllegalStateException("Commit marker filename/epoch mismatch: " + path);
      }
      validateNext(previous, marker);
      decoded.add(marker);
      previous = marker;
    }
    return decoded;
  }

  private void validateNext(HistoryCommitMarker previous, HistoryCommitMarker current) {
    if (previous == null) {
      if (current.getPreviousEpoch() >= current.getMeta().getEpoch()) {
        throw new IllegalArgumentException("Invalid base commit marker previous epoch");
      }
      return;
    }
    if (current.getMeta().getEpoch() != previous.getMeta().getEpoch() + 1
        || current.getPreviousEpoch() != previous.getMeta().getEpoch()
        || current.getMeta().getBlockNumber() != previous.getMeta().getBlockNumber() + 1
        || !Arrays.equals(current.getMeta().getParentHash(),
        previous.getMeta().getBlockHash())) {
      throw new IllegalArgumentException("Non-contiguous history commit marker");
    }
  }

  private Path markerPath(long epoch) {
    return directory.resolve(String.format("%020d%s", epoch, SUFFIX));
  }

  private static long parseEpoch(Path path) {
    String name = path.getFileName().toString();
    try {
      return Long.parseLong(name.substring(0, name.length() - SUFFIX.length()));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid history commit marker name: " + name, e);
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  @Override
  public void close() {
    // Marker files do not keep open resources.
  }

  interface DirectorySync {
    void sync(Path directory) throws IOException;
  }
}
