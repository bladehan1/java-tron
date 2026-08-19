package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/** RocksDB participant whose business mutations and D[i] share one synced native WriteBatch. */
public final class RocksDbArchiveParticipant implements Closeable {

  private static final byte BUSINESS_PREFIX = 1;
  private static final byte[] PROGRESS_KEY = new byte[]{0, 'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};

  static {
    RocksDB.loadLibrary();
  }

  private final String participant;
  private final List<String> participants;
  private final Options options = new Options().setCreateIfMissing(true);
  private final WriteOptions syncWrites = new WriteOptions().setSync(true);
  private final RocksDB database;
  private final ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
  private final FaultHook faultHook;

  public RocksDbArchiveParticipant(Path directory, String participant,
      List<String> participants) throws IOException {
    this(directory, participant, participants, stage -> { });
  }

  RocksDbArchiveParticipant(Path directory, String participant, List<String> participants,
      FaultHook faultHook) throws IOException {
    this.participant = Objects.requireNonNull(participant, "participant");
    this.participants = validateParticipants(participants);
    if (!this.participants.contains(participant)) {
      throw new IllegalArgumentException("Archive participant is outside the exact set");
    }
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    try {
      Files.createDirectories(directory);
    } catch (IOException failure) {
      options.close();
      syncWrites.close();
      throw failure;
    }
    try {
      database = RocksDB.open(options, directory.toString());
    } catch (RocksDBException failure) {
      options.close();
      syncWrites.close();
      throw new IOException("Failed to open archive participant engine", failure);
    }
  }

  public synchronized void apply(List<Mutation> mutations, ArchiveProgressEnvelope progress)
      throws IOException {
    Objects.requireNonNull(mutations, "mutations");
    requireProgress(progress);
    try (WriteBatch batch = new WriteBatch()) {
      for (Mutation mutation : mutations) {
        Objects.requireNonNull(mutation, "mutation");
        if (mutation.value == null) {
          batch.delete(businessKey(mutation.key));
        } else {
          batch.put(businessKey(mutation.key), mutation.value);
        }
      }
      batch.put(PROGRESS_KEY, progressCodec.encode(progress));
      faultHook.atStage(Stage.BEFORE_WRITE);
      database.write(syncWrites, batch);
      faultHook.atStage(Stage.AFTER_WRITE);
    } catch (RocksDBException failure) {
      throw new IOException("Failed to apply archive participant batch", failure);
    }
  }

  public synchronized byte[] get(byte[] key) throws IOException {
    try {
      byte[] value = database.get(businessKey(key));
      return value == null ? null : Arrays.copyOf(value, value.length);
    } catch (RocksDBException failure) {
      throw new IOException("Failed to read archive participant business state", failure);
    }
  }

  public synchronized ArchiveProgressEnvelope loadProgress() throws IOException {
    try {
      byte[] encoded = database.get(PROGRESS_KEY);
      if (encoded == null) {
        throw new ArchivePersistenceException("Archive participant progress is missing");
      }
      ArchiveProgressEnvelope progress = progressCodec.decode(encoded);
      requireProgress(progress);
      return progress;
    } catch (RocksDBException failure) {
      throw new IOException("Failed to read archive participant progress", failure);
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive participant progress is corrupt", invalid);
    }
  }

  @Override
  public synchronized void close() {
    syncWrites.close();
    database.close();
    options.close();
  }

  private void requireProgress(ArchiveProgressEnvelope progress) {
    Objects.requireNonNull(progress, "progress");
    if (progress.getKind() != ArchiveProgressEnvelope.Kind.PARTICIPANT_PROGRESS
        || !participant.equals(progress.getParticipant())
        || !participants.equals(progress.getParticipants())) {
      throw new IllegalArgumentException("Archive participant progress identity mismatch");
    }
  }

  private static byte[] businessKey(byte[] key) {
    Objects.requireNonNull(key, "key");
    return ByteBuffer.allocate(1 + key.length).put(BUSINESS_PREFIX).put(key).array();
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Archive participant set must not be empty");
    }
    String previous = null;
    for (String current : copy) {
      if (current == null || current.isEmpty()
          || previous != null && previous.compareTo(current) >= 0) {
        throw new IllegalArgumentException(
            "Archive participants must be non-empty, unique, and sorted");
      }
      previous = current;
    }
    return Collections.unmodifiableList(copy);
  }

  public static final class Mutation {
    private final byte[] key;
    private final byte[] value;

    private Mutation(byte[] key, byte[] value) {
      this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public static Mutation put(byte[] key, byte[] value) {
      return new Mutation(key, Objects.requireNonNull(value, "value"));
    }

    public static Mutation delete(byte[] key) {
      return new Mutation(key, null);
    }
  }

  enum Stage {
    BEFORE_WRITE,
    AFTER_WRITE
  }

  @FunctionalInterface
  interface FaultHook {
    void atStage(Stage stage) throws IOException;
  }
}
