package org.tron.core.db2.archive;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

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
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

/** LevelDB participant whose business mutations and D[i] share one synced native WriteBatch. */
public final class LevelDbArchiveParticipant implements Closeable {

  private static final byte BUSINESS_PREFIX = 1;
  private static final byte[] PROGRESS_KEY = new byte[]{0, 'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};

  private final Path directory;
  private final String participant;
  private final List<String> participants;
  private final Options options = new Options().createIfMissing(true);
  private final WriteOptions syncWrites = new WriteOptions().sync(true);
  private final ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
  private final FaultHook faultHook;
  private DB database;

  public LevelDbArchiveParticipant(Path directory, String participant,
      List<String> participants) throws IOException {
    this(directory, participant, participants, stage -> { });
  }

  LevelDbArchiveParticipant(Path directory, String participant, List<String> participants,
      FaultHook faultHook) throws IOException {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.participant = Objects.requireNonNull(participant, "participant");
    this.participants = validateParticipants(participants);
    if (!this.participants.contains(participant)) {
      throw new IllegalArgumentException("Archive participant is outside the exact set");
    }
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
    Files.createDirectories(directory);
    database = open();
  }

  public synchronized void apply(List<Mutation> mutations, ArchiveProgressEnvelope progress)
      throws IOException {
    Objects.requireNonNull(mutations, "mutations");
    requireProgress(progress);
    try (WriteBatch batch = database.createWriteBatch()) {
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
      database.write(batch, syncWrites);
      faultHook.atStage(Stage.AFTER_WRITE);
    }
  }

  public synchronized byte[] get(byte[] key) {
    byte[] value = database.get(businessKey(key));
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  public synchronized ArchiveProgressEnvelope loadProgress() {
    byte[] encoded = database.get(PROGRESS_KEY);
    if (encoded == null) {
      throw new ArchivePersistenceException("Archive participant progress is missing");
    }
    try {
      ArchiveProgressEnvelope progress = progressCodec.decode(encoded);
      requireProgress(progress);
      return progress;
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive participant progress is corrupt", invalid);
    }
  }

  public synchronized void reset() throws IOException {
    database.close();
    database = null;
    try {
      factory.destroy(directory.toFile(), options);
    } finally {
      database = open();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (database != null) {
      database.close();
      database = null;
    }
  }

  private DB open() throws IOException {
    return factory.open(directory.toFile(), options);
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
