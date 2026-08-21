package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.RecoveryPlan;
import org.tron.core.db2.core.SnapshotManager;

/** Sole owner for exact-27 State Archive resources from recovered startup through shutdown. */
public final class StateArchiveRuntimeOwner implements Closeable {

  public enum State {
    RECOVERED,
    RUNNING,
    QUIESCING,
    CLOSED,
    FAILED_CLOSED
  }

  private final SnapshotManager snapshotManager;
  private final ArchiveRuntimeAttachment attachment;
  private final ArchiveRuntimeQueryGate queryGate;
  private final Closeable latestCoordinator;
  private final Closeable servingCatalog;
  private final List<Closeable> participants;
  private final Closeable sink;
  private final BlockSnapshotMeta recoveredHead;
  private final int startupRecoveryActionCount;
  private State state;
  private boolean detached;
  private IOException terminalFailure;

  public StateArchiveRuntimeOwner(SnapshotManager snapshotManager,
      ArchiveRuntimeAttachment attachment, ArchiveRuntimeQueryGate queryGate,
      Closeable latestCoordinator, Closeable servingCatalog,
      List<? extends Closeable> participants) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager");
    this.attachment = Objects.requireNonNull(attachment, "attachment");
    this.queryGate = Objects.requireNonNull(queryGate, "queryGate");
    this.latestCoordinator = Objects.requireNonNull(latestCoordinator, "latestCoordinator");
    this.servingCatalog = Objects.requireNonNull(servingCatalog, "servingCatalog");
    if (!(attachment.getSink() instanceof Closeable)) {
      throw new IllegalArgumentException("Attached archive sink must be Closeable");
    }
    this.sink = (Closeable) attachment.getSink();
    this.participants = immutableParticipants(participants);
    this.recoveredHead = null;
    this.startupRecoveryActionCount = 0;
    this.state = State.RUNNING;
    validateUniqueOwnership();
  }

  private StateArchiveRuntimeOwner(SnapshotManager snapshotManager,
      List<? extends Closeable> participants, BlockSnapshotMeta recoveredHead,
      int startupRecoveryActionCount) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager");
    this.attachment = null;
    this.queryGate = null;
    this.latestCoordinator = null;
    this.servingCatalog = null;
    this.participants = immutableParticipants(participants);
    this.sink = null;
    this.recoveredHead = Objects.requireNonNull(recoveredHead, "recoveredHead");
    this.startupRecoveryActionCount = startupRecoveryActionCount;
    this.state = State.RECOVERED;
  }

  /**
   * Opens the canonical exact-27 native participants and converges startup recovery before any
   * normal archive producer is attached to {@link SnapshotManager}.
   */
  public static StateArchiveRuntimeOwner recover(SnapshotManager snapshotManager,
      Path archiveDirectory, long maxSegmentSize, String databaseEngine) throws IOException {
    Objects.requireNonNull(snapshotManager, "snapshotManager");
    Path root = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    String engine = Objects.requireNonNull(databaseEngine, "databaseEngine")
        .toUpperCase(Locale.ROOT);
    if (!"LEVELDB".equals(engine) && !"ROCKSDB".equals(engine)) {
      throw new IllegalArgumentException("Unsupported State Archive database engine: " + engine);
    }
    List<String> names = ArchiveParticipantDescriptor.current().getParticipants();
    Map<String, ArchiveParticipant> openedByName = new LinkedHashMap<>();
    List<Closeable> opened = new ArrayList<>();
    try {
      for (String participant : names) {
        Closeable nativeEngine = openParticipant(root.resolve("participants").resolve(participant),
            participant, names, engine);
        opened.add(nativeEngine);
        openedByName.put(participant, (ArchiveParticipant) nativeEngine);
      }
      Path checkpoint = root.resolve("progress").resolve("checkpoint.progress");
      Path reader = root.resolve("progress").resolve("reader.progress");
      RecoveryPlan first;
      HistoryCommitMarker head;
      try (ArchiveParticipantRecoveryStorage recovery =
          new ArchiveParticipantRecoveryStorage(root, maxSegmentSize, checkpoint,
              openedByName, reader, names)) {
        first = new ArchiveRecoveryExecutor(recovery).recover();
        RecoveryPlan fixed = new ArchiveRecoveryExecutor(recovery).recover();
        if (!fixed.getActions().isEmpty()) {
          throw new ArchivePersistenceException(
              "State Archive second startup recovery was not zero-action");
        }
        head = recovery.committedHead();
      }
      if (head == null) {
        throw new ArchivePersistenceException("State Archive recovered H head is missing");
      }
      return new StateArchiveRuntimeOwner(snapshotManager, opened, head.getMeta(),
          first.getActions().size());
    } catch (IOException | RuntimeException failure) {
      closeReverse(opened, failure);
      throw failure;
    }
  }

  public synchronized State getState() {
    return state;
  }

  public BlockSnapshotMeta getRecoveredHead() {
    if (recoveredHead == null) {
      throw new IllegalStateException("State Archive runtime has no startup recovery head");
    }
    return recoveredHead;
  }

  public int getStartupRecoveryActionCount() {
    return startupRecoveryActionCount;
  }

  /** Quiesces, detaches and closes owned resources without waiting for active query leases. */
  @Override
  public synchronized void close() throws IOException {
    if (state == State.CLOSED) {
      return;
    }
    if (state == State.FAILED_CLOSED) {
      throw terminalFailure;
    }
    if (state == State.RECOVERED) {
      IOException failure = closeParticipants();
      if (failure == null) {
        state = State.CLOSED;
        return;
      }
      terminalFailure = failure;
      state = State.FAILED_CLOSED;
      throw failure;
    }
    state = State.QUIESCING;
    queryGate.quiesce();
    if (!detached) {
      ArchiveRuntimeAttachment returned = snapshotManager.detachArchiveRuntime(attachment);
      if (returned != attachment) {
        throw new IllegalStateException("SnapshotManager returned a foreign archive attachment");
      }
      detached = true;
    }
    if (!queryGate.isDrained()) {
      throw new IllegalStateException(
          "State Archive runtime still has active query leases: "
              + queryGate.getActiveLeaseCount());
    }
    queryGate.close();

    IOException failure = null;
    failure = closeOwned("latest coordinator", latestCoordinator, failure);
    failure = closeOwned("serving catalog", servingCatalog, failure);
    for (int i = participants.size() - 1; i >= 0; i--) {
      failure = closeOwned("archive participant " + i, participants.get(i), failure);
    }
    failure = closeOwned("archive history sink", sink, failure);
    if (failure == null) {
      state = State.CLOSED;
      return;
    }
    terminalFailure = failure;
    state = State.FAILED_CLOSED;
    throw failure;
  }

  private IOException closeParticipants() {
    IOException failure = null;
    for (int i = participants.size() - 1; i >= 0; i--) {
      failure = closeOwned("archive participant " + i, participants.get(i), failure);
    }
    return failure;
  }

  private static Closeable openParticipant(Path directory, String participant,
      List<String> participants, String engine) throws IOException {
    if ("ROCKSDB".equals(engine)) {
      return new RocksDbArchiveParticipant(directory, participant, participants);
    }
    return new LevelDbArchiveParticipant(directory, participant, participants);
  }

  private static void closeReverse(List<? extends Closeable> resources, Exception failure) {
    for (int i = resources.size() - 1; i >= 0; i--) {
      try {
        resources.get(i).close();
      } catch (IOException | RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static List<Closeable> immutableParticipants(
      List<? extends Closeable> participants) {
    List<? extends Closeable> source = Objects.requireNonNull(participants, "participants");
    List<Closeable> copy = new ArrayList<>(source.size());
    for (Closeable participant : source) {
      copy.add(Objects.requireNonNull(participant, "participant"));
    }
    return Collections.unmodifiableList(copy);
  }

  private void validateUniqueOwnership() {
    Set<Closeable> unique = Collections.newSetFromMap(new IdentityHashMap<Closeable, Boolean>());
    requireUnique(unique, latestCoordinator, "latestCoordinator");
    requireUnique(unique, servingCatalog, "servingCatalog");
    for (int i = 0; i < participants.size(); i++) {
      requireUnique(unique, participants.get(i), "participant[" + i + "]");
    }
    requireUnique(unique, sink, "sink");
  }

  private static void requireUnique(Set<Closeable> unique, Closeable resource, String name) {
    if (!unique.add(resource)) {
      throw new IllegalArgumentException("Archive runtime resource has multiple owners: " + name);
    }
  }

  private static IOException closeOwned(String name, Closeable resource, IOException current) {
    try {
      resource.close();
      return current;
    } catch (IOException failure) {
      return append(current, failure);
    } catch (RuntimeException failure) {
      return append(current, new IOException("Failed to close " + name, failure));
    }
  }

  private static IOException append(IOException current, IOException failure) {
    if (current == null) {
      return failure;
    }
    current.addSuppressed(failure);
    return current;
  }
}
