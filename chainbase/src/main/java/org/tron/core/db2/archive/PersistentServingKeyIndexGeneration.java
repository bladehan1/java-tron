package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/** Persistent immutable exact-key serving generation backed by RocksDB. */
public final class PersistentServingKeyIndexGeneration implements ServingKeyIndex {

  private static final int MAGIC = 0x534b4947; // SKIG
  private static final short VERSION = 2;
  private static final short LEGACY_VERSION = 1;
  private static final int MAX_MANIFEST_SIZE = 1024 * 1024;
  private static final byte DATA_PREFIX = 1;
  private static final byte[] PRESENT = new byte[]{1};
  private static final String MANIFEST = "generation.meta";
  private static final String MANIFEST_TEMP = "generation.meta.tmp";
  private static final String DATABASE = "keys";

  static {
    RocksDB.loadLibrary();
  }

  private final Path directory;
  private final Descriptor descriptor;
  private final Options options;
  private final RocksDB database;
  private final Runnable release;
  private boolean closed;

  private PersistentServingKeyIndexGeneration(Path directory, Descriptor descriptor,
      Runnable release) throws IOException {
    this.directory = directory;
    this.descriptor = descriptor;
    this.release = Objects.requireNonNull(release, "release");
    this.options = new Options().setCreateIfMissing(false);
    try {
      this.database = RocksDB.openReadOnly(options, directory.resolve(DATABASE).toString());
    } catch (RocksDBException failure) {
      options.close();
      throw new IOException("Failed to open serving index generation", failure);
    }
  }

  public static PersistentServingKeyIndexGeneration build(Path directory, String generationId,
      long baseEpoch, byte[] baseHash, Iterable<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader reader,
      List<String> participatingDatabases) throws IOException {
    return build(directory, generationId, baseEpoch, baseHash, committed, reader,
        participatingDatabases, new byte[32]);
  }

  public static PersistentServingKeyIndexGeneration build(Path directory, String generationId,
      long baseEpoch, byte[] baseHash, Iterable<HistoryCommitMarker> committed,
      ServingKeyIndexGeneration.AuthoritativeIndexReader reader,
      List<String> participatingDatabases, byte[] latestSourceIdentityDigest) throws IOException {
    Objects.requireNonNull(directory, "directory");
    Objects.requireNonNull(committed, "committed");
    Objects.requireNonNull(reader, "reader");
    List<String> participants = sortedParticipants(participatingDatabases);
    if (generationId == null || generationId.isEmpty() || baseEpoch < 0) {
      throw new IllegalArgumentException("Invalid serving generation identity");
    }
    requireHash(baseHash, "baseHash");
    requireHash(latestSourceIdentityDigest, "latestSourceIdentityDigest");
    if (Files.exists(directory)) {
      throw new IllegalArgumentException("Serving generation directory already exists");
    }
    Files.createDirectories(directory);

    MessageDigest sourceDigest = sha256();
    updateLong(sourceDigest, baseEpoch);
    sourceDigest.update(baseHash);
    updateParticipantDigest(sourceDigest, participants);
    long previousEpoch = baseEpoch;
    long previousBlock = baseEpoch;
    byte[] previousHash = Arrays.copyOf(baseHash, baseHash.length);
    long keyChanges = 0;
    Options buildOptions = new Options().setCreateIfMissing(true);
    WriteOptions writes = new WriteOptions().setSync(false);
    try (RocksDB target = RocksDB.open(buildOptions, directory.resolve(DATABASE).toString())) {
      for (HistoryCommitMarker marker : committed) {
        BlockSnapshotMeta meta = marker.getMeta();
        validateNext(marker, previousEpoch, previousBlock, previousHash, participants);
        HistoryIndexRecord record = reader.read(marker.getIndexLocation());
        validateMarker(marker, record, participants);
        try (WriteBatch batch = new WriteBatch()) {
          for (HistoryIndexRecord.KeyGroup group : record.getGroups()) {
            for (byte[] key : group.getKeys()) {
              batch.put(dataKey(group.getDbName(), key, meta.getEpoch()), PRESENT);
              keyChanges++;
            }
          }
          target.write(writes, batch);
        }
        updateSourceDigest(sourceDigest, marker);
        previousEpoch = meta.getEpoch();
        previousBlock = meta.getBlockNumber();
        previousHash = meta.getBlockHash();
      }
      try (WriteOptions sync = new WriteOptions().setSync(true)) {
        target.put(sync, new byte[]{0}, new byte[]{1});
      }
    } catch (RocksDBException failure) {
      throw new IOException("Failed to build serving index generation", failure);
    } finally {
      writes.close();
      buildOptions.close();
    }

    Descriptor descriptor = new Descriptor(generationId, baseEpoch, previousEpoch, previousHash,
        sourceDigest.digest(), latestSourceIdentityDigest, participants, keyChanges);
    persistDescriptor(directory, descriptor);
    HistorySegmentStore.syncDirectory(directory);
    return open(directory);
  }

  public static PersistentServingKeyIndexGeneration open(Path directory) throws IOException {
    return open(directory, () -> { });
  }

  static PersistentServingKeyIndexGeneration open(Path directory, Runnable release)
      throws IOException {
    return new PersistentServingKeyIndexGeneration(directory, loadDescriptor(directory), release);
  }

  @Override
  public synchronized OptionalLong firstChangeAfter(String dbName, byte[] rawKey,
      long targetBlock, long upperBound) throws IOException {
    ensureOpen();
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(rawKey, "rawKey");
    validateCoverage(dbName, targetBlock, upperBound);
    if (targetBlock == Long.MAX_VALUE) {
      return OptionalLong.empty();
    }
    byte[] prefix = dataPrefix(dbName, rawKey);
    byte[] seek = ByteBuffer.allocate(prefix.length + Long.BYTES).put(prefix)
        .putLong(targetBlock + 1).array();
    try (RocksIterator iterator = database.newIterator()) {
      iterator.seek(seek);
      if (!iterator.isValid()) {
        return OptionalLong.empty();
      }
      byte[] found = iterator.key();
      if (found.length != prefix.length + Long.BYTES || !startsWith(found, prefix)) {
        return OptionalLong.empty();
      }
      long epoch = ByteBuffer.wrap(found, prefix.length, Long.BYTES).getLong();
      return epoch <= upperBound ? OptionalLong.of(epoch) : OptionalLong.empty();
    }
  }

  @Override
  public List<ServingKeyIndexGeneration.ChangedKey> changesInRange(String dbName,
      byte[] lowerInclusive, byte[] upperExclusive, long targetBlock, long upperBound,
      int maxChangedKeys) {
    throw new UnsupportedOperationException(
        "Persistent generic range serving is outside the Phase 1 point-query scope");
  }

  @Override
  public String getGenerationId() {
    return descriptor.generationId;
  }

  @Override
  public long getIndexedFrom() {
    return descriptor.indexedFrom;
  }

  @Override
  public long getIndexedThrough() {
    return descriptor.indexedThrough;
  }

  @Override
  public byte[] getHeadHash() {
    return Arrays.copyOf(descriptor.headHash, descriptor.headHash.length);
  }

  @Override
  public byte[] getAuthoritativePrefixDigest() {
    return Arrays.copyOf(descriptor.sourceDigest, descriptor.sourceDigest.length);
  }

  public List<String> getParticipatingDatabases() {
    return descriptor.participants;
  }

  public long getKeyChangeCount() {
    return descriptor.keyChanges;
  }

  public byte[] getLatestSourceIdentityDigest() {
    return Arrays.copyOf(descriptor.latestSourceIdentityDigest,
        descriptor.latestSourceIdentityDigest.length);
  }

  public boolean isLatestSourceIdentityBound() {
    for (byte value : descriptor.latestSourceIdentityDigest) {
      if (value != 0) {
        return true;
      }
    }
    return false;
  }

  Path getDirectory() {
    return directory;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      database.close();
      options.close();
      release.run();
    }
  }

  private void validateCoverage(String dbName, long targetBlock, long upperBound) {
    if (Collections.binarySearch(descriptor.participants, dbName) < 0) {
      throw new IllegalArgumentException("Database is outside serving index coverage: " + dbName);
    }
    if (targetBlock < descriptor.indexedFrom || targetBlock > upperBound
        || upperBound > descriptor.indexedThrough) {
      throw new IllegalArgumentException("Query range is outside serving index coverage");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Serving index generation is closed");
    }
  }

  private static byte[] dataKey(String dbName, byte[] rawKey, long epoch) {
    if (epoch < 0) {
      throw new IllegalArgumentException("Serving index epoch must not be negative");
    }
    byte[] prefix = dataPrefix(dbName, rawKey);
    return ByteBuffer.allocate(prefix.length + Long.BYTES).put(prefix).putLong(epoch).array();
  }

  private static byte[] dataPrefix(String dbName, byte[] rawKey) {
    byte[] database = dbName.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(1 + Integer.BYTES + database.length + Integer.BYTES + rawKey.length)
        .put(DATA_PREFIX).putInt(database.length).put(database).putInt(rawKey.length).put(rawKey)
        .array();
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    if (value.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (value[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  private static void validateNext(HistoryCommitMarker marker, long previousEpoch,
      long previousBlock, byte[] previousHash, List<String> participants) {
    BlockSnapshotMeta meta = marker.getMeta();
    if (marker.getPreviousEpoch() != previousEpoch || meta.getEpoch() != previousEpoch + 1
        || meta.getBlockNumber() != previousBlock + 1
        || !Arrays.equals(meta.getParentHash(), previousHash)
        || !participants.equals(marker.getDatabases())) {
      throw new IllegalArgumentException("Serving index source commit prefix is inconsistent");
    }
  }

  private static void validateMarker(HistoryCommitMarker marker, HistoryIndexRecord record,
      List<String> participants) {
    if (record == null || !marker.getMeta().equals(record.getMeta())
        || !same(marker.getHistoryLocation(), record.getHistoryLocation())) {
      throw new IllegalArgumentException(
          "Commit marker does not match authoritative history index record");
    }
    for (HistoryIndexRecord.KeyGroup group : record.getGroups()) {
      if (Collections.binarySearch(participants, group.getDbName()) < 0) {
        throw new IllegalArgumentException("History index contains an unknown database");
      }
    }
  }

  private static boolean same(HistoryLocation left, HistoryLocation right) {
    return left.getSegmentId() == right.getSegmentId()
        && left.getOffset() == right.getOffset()
        && left.getRecordLength() == right.getRecordLength()
        && left.getBodyChecksum() == right.getBodyChecksum()
        && Arrays.equals(left.getBodyDigest(), right.getBodyDigest());
  }

  private static List<String> sortedParticipants(List<String> databases) {
    List<String> result = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    Collections.sort(result);
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Serving index participant set must not be empty");
    }
    String previous = null;
    for (String database : result) {
      if (database == null || database.isEmpty() || database.equals(previous)) {
        throw new IllegalArgumentException("Serving index participant set is invalid");
      }
      previous = database;
    }
    return Collections.unmodifiableList(result);
  }

  private static void updateSourceDigest(MessageDigest digest, HistoryCommitMarker marker) {
    updateLong(digest, marker.getMeta().getEpoch());
    updateLong(digest, marker.getMeta().getBlockNumber());
    digest.update(marker.getMeta().getBlockHash());
    digest.update(marker.getMeta().getParentHash());
    updateLong(digest, marker.getIndexLocation().getOffset());
    updateLong(digest, marker.getIndexLocation().getRecordLength());
    digest.update(marker.getIndexLocation().getDigest());
    digest.update(marker.getHistoryLocation().getBodyDigest());
  }

  private static void updateParticipantDigest(MessageDigest digest, List<String> databases) {
    updateLong(digest, databases.size());
    for (String database : databases) {
      byte[] encoded = database.getBytes(StandardCharsets.UTF_8);
      updateLong(digest, encoded.length);
      digest.update(encoded);
    }
  }

  private static void updateLong(MessageDigest digest, long value) {
    digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void requireHash(byte[] hash, String name) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
  }

  private static void persistDescriptor(Path directory, Descriptor descriptor) throws IOException {
    byte[] encoded = encodeDescriptor(descriptor);
    Path temporary = directory.resolve(MANIFEST_TEMP);
    try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
    try {
      Files.move(temporary, directory.resolve(MANIFEST), StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException unsupported) {
      throw new ArchivePersistenceException(
          "Serving index filesystem does not support atomic manifests", unsupported);
    }
  }

  private static Descriptor loadDescriptor(Path directory) throws IOException {
    Path manifest = directory.resolve(MANIFEST);
    if (!Files.isRegularFile(manifest)) {
      throw new ArchivePersistenceException("Serving index generation manifest is missing");
    }
    try {
      return decodeDescriptor(Files.readAllBytes(manifest));
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Serving index generation manifest is corrupt",
          invalid);
    }
  }

  private static byte[] encodeDescriptor(Descriptor descriptor) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeUTF(descriptor.generationId);
      output.writeLong(descriptor.indexedFrom);
      output.writeLong(descriptor.indexedThrough);
      output.write(descriptor.headHash);
      output.write(descriptor.sourceDigest);
      output.write(descriptor.latestSourceIdentityDigest);
      output.writeLong(descriptor.keyChanges);
      output.writeInt(descriptor.participants.size());
      for (String participant : descriptor.participants) {
        output.writeUTF(participant);
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected serving manifest encoding failure", impossible);
    }
  }

  private static Descriptor decodeDescriptor(byte[] encoded) {
    if (encoded == null || encoded.length < 96 || encoded.length > MAX_MANIFEST_SIZE) {
      throw new IllegalArgumentException("Serving index manifest length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Serving index manifest checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC) {
        throw new IllegalArgumentException("Unsupported serving index manifest");
      }
      short version = input.readShort();
      if (version != VERSION && version != LEGACY_VERSION || input.readShort() != 0) {
        throw new IllegalArgumentException("Unsupported serving index manifest");
      }
      String generationId = input.readUTF();
      long from = input.readLong();
      long through = input.readLong();
      byte[] headHash = new byte[32];
      byte[] sourceDigest = new byte[32];
      input.readFully(headHash);
      input.readFully(sourceDigest);
      byte[] latestSourceIdentityDigest = new byte[32];
      if (version >= VERSION) {
        input.readFully(latestSourceIdentityDigest);
      }
      long keyChanges = input.readLong();
      int count = input.readInt();
      if (generationId.isEmpty() || from < 0 || through < from || keyChanges < 0
          || count <= 0 || count > ArchiveStoreScope.getStateDatabases().size() + 16) {
        throw new IllegalArgumentException("Invalid serving index manifest fields");
      }
      List<String> participants = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        participants.add(input.readUTF());
      }
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Serving index manifest payload mismatch");
      }
      return new Descriptor(generationId, from, through, headHash, sourceDigest,
          latestSourceIdentityDigest, sortedParticipants(participants), keyChanges);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Serving index manifest is truncated", invalid);
    }
  }

  private static final class Descriptor {
    private final String generationId;
    private final long indexedFrom;
    private final long indexedThrough;
    private final byte[] headHash;
    private final byte[] sourceDigest;
    private final byte[] latestSourceIdentityDigest;
    private final List<String> participants;
    private final long keyChanges;

    private Descriptor(String generationId, long indexedFrom, long indexedThrough,
        byte[] headHash, byte[] sourceDigest, byte[] latestSourceIdentityDigest,
        List<String> participants, long keyChanges) {
      this.generationId = generationId;
      this.indexedFrom = indexedFrom;
      this.indexedThrough = indexedThrough;
      this.headHash = Arrays.copyOf(headHash, headHash.length);
      this.sourceDigest = Arrays.copyOf(sourceDigest, sourceDigest.length);
      this.latestSourceIdentityDigest = Arrays.copyOf(latestSourceIdentityDigest,
          latestSourceIdentityDigest.length);
      this.participants = participants;
      this.keyChanges = keyChanges;
    }
  }
}
