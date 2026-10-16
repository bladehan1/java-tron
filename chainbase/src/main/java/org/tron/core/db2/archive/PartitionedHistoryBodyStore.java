package org.tron.core.db2.archive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

/**
 * State Archive history placement with one default file library and dedicated hot-Store
 * libraries. The default record contains authenticated locations for every dedicated record, so
 * the existing authoritative marker commits the complete multi-library body.
 */
final class PartitionedHistoryBodyStore implements HistoryBodyStore {

  static final String DEFAULT_LIBRARY = "state-archive";
  static final List<String> DEDICATED_STORES = Collections.unmodifiableList(Arrays.asList(
      "account", "storage-row", "account-asset", "delegation"));

  private static final String REFERENCE_PREFIX = "\u0000state-archive-library/";
  private static final byte[] REFERENCE_KEY = new byte[]{1};
  private static final int ENCODED_LOCATION_LENGTH = Integer.BYTES + Long.BYTES
      + Integer.BYTES + Integer.BYTES + 32;

  private final HistorySegmentStore defaultLibrary;
  private final Map<String, HistorySegmentStore> dedicatedLibraries;

  PartitionedHistoryBodyStore(Path archiveDirectory, BlockHistoryCodec codec,
      long maxSegmentSize, ArchiveHistoryScanAnchor checkpoint) throws IOException {
    rejectLegacySharedLibrary(archiveDirectory);
    rejectLegacyAccountIndex(archiveDirectory);
    validateExistingLibraryEntries(archiveDirectory);
    this.defaultLibrary = HistorySegmentStore.openLibrary(archiveDirectory, DEFAULT_LIBRARY,
        codec, maxSegmentSize, checkpoint);
    Map<String, HistorySegmentStore> opened = new LinkedHashMap<>();
    try {
      Map<String, HistoryLocation> checkpointLocations = checkpointLocations(checkpoint);
      for (String store : DEDICATED_STORES) {
        ArchiveHistoryScanAnchor laneCheckpoint = checkpoint == null ? null
            : checkpoint.forHistoryLocation(checkpointLocations.get(store));
        opened.put(store, HistorySegmentStore.openLibrary(archiveDirectory,
            libraryName(store), codec, maxSegmentSize, laneCheckpoint));
      }
      this.dedicatedLibraries = Collections.unmodifiableMap(opened);
      validateScannedHeads();
    } catch (IOException | RuntimeException failure) {
      closeOpened(opened, failure);
      try {
        defaultLibrary.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  static String libraryName(String store) {
    return DEFAULT_LIBRARY + "-" + store;
  }

  @Override
  public synchronized HistoryLocation append(BlockReverseDiff diff) throws IOException {
    Map<String, HistoryLocation> locations = new LinkedHashMap<>();
    for (Map.Entry<String, HistorySegmentStore> library : dedicatedLibraries.entrySet()) {
      BlockReverseDiff lane = select(diff, library.getKey());
      locations.put(library.getKey(), library.getValue().append(lane));
    }
    List<DbGroup> defaultGroups = new ArrayList<>();
    for (DbGroup group : diff.getGroups()) {
      if (!dedicatedLibraries.containsKey(group.getDbName())) {
        defaultGroups.add(group);
      }
    }
    for (Map.Entry<String, HistoryLocation> location : locations.entrySet()) {
      defaultGroups.add(referenceGroup(location.getKey(), location.getValue()));
    }
    return defaultLibrary.append(new BlockReverseDiff(diff.getMeta(), defaultGroups));
  }

  @Override
  public synchronized void sync() throws IOException {
    for (HistorySegmentStore library : dedicatedLibraries.values()) {
      library.sync();
    }
    defaultLibrary.sync();
  }

  @Override
  public synchronized BlockReverseDiff read(HistoryLocation location) throws IOException {
    BlockReverseDiff envelope = defaultLibrary.read(location);
    DecodedEnvelope decoded = decodeEnvelope(envelope);
    List<DbGroup> groups = new ArrayList<>(decoded.defaultGroups);
    for (String store : DEDICATED_STORES) {
      HistoryLocation dedicatedLocation = decoded.locations.get(store);
      if (dedicatedLocation == null) {
        throw new ArchivePersistenceException(
            "State Archive history envelope is missing dedicated library: " + store);
      }
      BlockReverseDiff lane = dedicatedLibraries.get(store).read(dedicatedLocation);
      if (!envelope.getMeta().equals(lane.getMeta())) {
        throw new ArchivePersistenceException(
            "State Archive dedicated library metadata mismatch: " + store);
      }
      if (lane.getGroups().size() > 1
          || (!lane.getGroups().isEmpty()
          && !store.equals(lane.getGroups().get(0).getDbName()))) {
        throw new ArchivePersistenceException(
            "State Archive dedicated library contains a foreign Store: " + store);
      }
      groups.addAll(lane.getGroups());
    }
    return new BlockReverseDiff(envelope.getMeta(), groups);
  }

  @Override
  public synchronized HistorySegmentStore.ScanResult getScanResult() {
    return defaultLibrary.getScanResult();
  }

  @Override
  public synchronized void truncateAfter(HistoryLocation last) throws IOException {
    truncateAfter(last, -1);
  }

  @Override
  public synchronized void truncateAfter(HistoryLocation last, long knownRecordCount)
      throws IOException {
    Map<String, HistoryLocation> dedicated = Collections.emptyMap();
    if (last != null) {
      dedicated = decodeEnvelope(defaultLibrary.read(last)).locations;
    }
    IOException failure = null;
    for (Map.Entry<String, HistorySegmentStore> library : dedicatedLibraries.entrySet()) {
      try {
        HistoryLocation laneLast = last == null ? null : dedicated.get(library.getKey());
        if (last != null && laneLast == null) {
          throw new ArchivePersistenceException(
              "State Archive history envelope is missing dedicated library: "
                  + library.getKey());
        }
        library.getValue().truncateAfter(laneLast, knownRecordCount);
      } catch (IOException | RuntimeException problem) {
        if (problem instanceof IOException) {
          failure = merge(failure, (IOException) problem);
        } else {
          throw problem;
        }
      }
    }
    try {
      defaultLibrary.truncateAfter(last, knownRecordCount);
    } catch (IOException problem) {
      failure = merge(failure, problem);
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public synchronized long getStartupScannedRecords() {
    // The public counter is logical body records scanned, not the number of physical lanes read.
    return defaultLibrary.getStartupScannedRecords();
  }

  @Override
  public synchronized void close() throws IOException {
    IOException failure = null;
    for (HistorySegmentStore library : dedicatedLibraries.values()) {
      try {
        library.close();
      } catch (IOException problem) {
        failure = merge(failure, problem);
      }
    }
    try {
      defaultLibrary.close();
    } catch (IOException problem) {
      failure = merge(failure, problem);
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void validateScannedHeads() throws IOException {
    HistorySegmentStore.ScannedRecord defaultHead = defaultLibrary.getScanResult().getHead();
    if (defaultHead == null) {
      return;
    }
    read(defaultHead.getLocation());
  }

  private Map<String, HistoryLocation> checkpointLocations(
      ArchiveHistoryScanAnchor checkpoint) throws IOException {
    if (checkpoint == null) {
      return Collections.emptyMap();
    }
    BlockReverseDiff envelope = defaultLibrary.read(
        checkpoint.getMarker().getHistoryLocation());
    Map<String, HistoryLocation> locations = decodeEnvelope(envelope).locations;
    for (String store : DEDICATED_STORES) {
      if (!locations.containsKey(store)) {
        throw new ArchivePersistenceException(
            "State Archive scan anchor is missing dedicated library: " + store);
      }
    }
    return locations;
  }

  private static BlockReverseDiff select(BlockReverseDiff diff, String store) {
    for (DbGroup group : diff.getGroups()) {
      if (store.equals(group.getDbName())) {
        return new BlockReverseDiff(diff.getMeta(), Collections.singletonList(group));
      }
    }
    return new BlockReverseDiff(diff.getMeta(), Collections.emptyList());
  }

  private static DbGroup referenceGroup(String store, HistoryLocation location) {
    return new DbGroup(REFERENCE_PREFIX + store, Collections.singletonList(
        new Entry(REFERENCE_KEY, OldValue.present(encodeLocation(location)))));
  }

  private static DecodedEnvelope decodeEnvelope(BlockReverseDiff envelope) {
    Map<String, HistoryLocation> locations = new LinkedHashMap<>();
    List<DbGroup> groups = new ArrayList<>();
    for (DbGroup group : envelope.getGroups()) {
      if (!group.getDbName().startsWith(REFERENCE_PREFIX)) {
        groups.add(group);
        continue;
      }
      String store = group.getDbName().substring(REFERENCE_PREFIX.length());
      if (!DEDICATED_STORES.contains(store) || locations.containsKey(store)
          || group.getEntries().size() != 1
          || !Arrays.equals(REFERENCE_KEY, group.getEntries().get(0).getKey())
          || !group.getEntries().get(0).getOldValue().isPresent()) {
        throw new ArchivePersistenceException("Invalid State Archive history library reference");
      }
      locations.put(store, decodeLocation(group.getEntries().get(0).getOldValue().getValue()));
    }
    return new DecodedEnvelope(groups, locations);
  }

  private static byte[] encodeLocation(HistoryLocation location) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(ENCODED_LOCATION_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(location.getSegmentId());
      output.writeLong(location.getOffset());
      output.writeInt(location.getRecordLength());
      output.writeInt(location.getBodyChecksum());
      output.write(location.getBodyDigest());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected location encoding failure", impossible);
    }
  }

  private static HistoryLocation decodeLocation(byte[] encoded) {
    if (encoded.length != ENCODED_LOCATION_LENGTH) {
      throw new ArchivePersistenceException("Invalid State Archive library location length");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      int segment = input.readInt();
      long offset = input.readLong();
      int length = input.readInt();
      int checksum = input.readInt();
      byte[] digest = new byte[32];
      input.readFully(digest);
      return new HistoryLocation(segment, offset, length, checksum, digest);
    } catch (IOException impossible) {
      throw new ArchivePersistenceException("Invalid State Archive library location", impossible);
    }
  }

  private static void rejectLegacySharedLibrary(Path archiveDirectory) throws IOException {
    Path history = archiveDirectory.resolve("history");
    if (!Files.isDirectory(history)) {
      return;
    }
    try (DirectoryStream<Path> segments = Files.newDirectoryStream(history,
        "history.*.dat")) {
      if (segments.iterator().hasNext()) {
        throw new ArchivePersistenceException(
            "Legacy shared history layout requires explicit offline migration");
      }
    }
  }

  private static void rejectLegacyAccountIndex(Path archiveDirectory) {
    if (Files.exists(archiveDirectory.resolve("account-change-index"))) {
      throw new ArchivePersistenceException(
          "Legacy account-change-index requires explicit offline removal");
    }
  }

  private static void validateExistingLibraryEntries(Path archiveDirectory) throws IOException {
    Path history = archiveDirectory.resolve("history");
    if (!Files.isDirectory(history)) {
      return;
    }
    List<String> expected = new ArrayList<>();
    expected.add(DEFAULT_LIBRARY);
    for (String store : DEDICATED_STORES) {
      expected.add(libraryName(store));
    }
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(history)) {
      for (Path entry : entries) {
        if (!Files.isDirectory(entry) || !expected.contains(entry.getFileName().toString())) {
          throw new ArchivePersistenceException(
              "Unknown State Archive history library entry: " + entry.getFileName());
        }
      }
    }
  }

  private static void closeOpened(Map<String, HistorySegmentStore> stores, Exception failure) {
    for (HistorySegmentStore store : stores.values()) {
      try {
        store.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static IOException merge(IOException current, IOException next) {
    if (current == null) {
      return next;
    }
    current.addSuppressed(next);
    return current;
  }

  private static final class DecodedEnvelope {
    private final List<DbGroup> defaultGroups;
    private final Map<String, HistoryLocation> locations;

    private DecodedEnvelope(List<DbGroup> defaultGroups,
        Map<String, HistoryLocation> locations) {
      this.defaultGroups = defaultGroups;
      this.locations = locations;
    }
  }
}
