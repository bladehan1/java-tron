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
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
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
  private final Path archiveDirectory;
  private final long maxSegmentSize;
  private final List<Closeable> participants;
  private final Map<String, ArchiveParticipant> participantEngines;
  private final BlockSnapshotMeta recoveredHead;
  private final int startupRecoveryActionCount;
  private ArchiveRuntimeAttachment attachment;
  private ArchiveRuntimeQueryGate queryGate;
  private Closeable latestCoordinator;
  private Closeable servingCatalog;
  private Closeable sink;
  private ArchiveHistoryWriter historyWriter;
  private State state;
  private boolean detached;
  private IOException terminalFailure;

  public StateArchiveRuntimeOwner(SnapshotManager snapshotManager,
      ArchiveRuntimeAttachment attachment, ArchiveRuntimeQueryGate queryGate,
      Closeable latestCoordinator, Closeable servingCatalog,
      List<? extends Closeable> participants) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager");
    this.archiveDirectory = null;
    this.maxSegmentSize = 0;
    this.attachment = Objects.requireNonNull(attachment, "attachment");
    this.queryGate = Objects.requireNonNull(queryGate, "queryGate");
    this.latestCoordinator = Objects.requireNonNull(latestCoordinator, "latestCoordinator");
    this.servingCatalog = Objects.requireNonNull(servingCatalog, "servingCatalog");
    if (!(attachment.getSink() instanceof Closeable)) {
      throw new IllegalArgumentException("Attached archive sink must be Closeable");
    }
    this.sink = (Closeable) attachment.getSink();
    this.participants = immutableParticipants(participants);
    this.participantEngines = Collections.emptyMap();
    this.recoveredHead = null;
    this.startupRecoveryActionCount = 0;
    this.state = State.RUNNING;
    validateUniqueOwnership();
  }

  private StateArchiveRuntimeOwner(SnapshotManager snapshotManager,
      Path archiveDirectory, long maxSegmentSize, List<? extends Closeable> participants,
      Map<String, ? extends ArchiveParticipant> participantEngines,
      BlockSnapshotMeta recoveredHead, int startupRecoveryActionCount) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager");
    this.archiveDirectory = Objects.requireNonNull(archiveDirectory, "archiveDirectory");
    this.maxSegmentSize = maxSegmentSize;
    this.attachment = null;
    this.queryGate = null;
    this.latestCoordinator = null;
    this.servingCatalog = null;
    this.participants = immutableParticipants(participants);
    this.participantEngines = immutableParticipantEngines(participantEngines);
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
      return new StateArchiveRuntimeOwner(snapshotManager, root, maxSegmentSize, opened,
          openedByName, head.getMeta(), first.getActions().size());
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

  /** Continues this recovered owner into one atomically attached normal-write runtime. */
  public synchronized ArchiveHistoryWriter attachNormalWriter(OldValueCollector collector,
      ArchiveBlockProjectionPreparer projectionPreparer, int queueCapacity) throws IOException {
    if (state != State.RECOVERED) {
      throw new IllegalStateException("State Archive owner is not recovered");
    }
    ArchiveHistoryWriter writer = null;
    AsyncArchiveHistorySink asyncSink = null;
    ArchiveRuntimeAttachment candidate = null;
    boolean attached = false;
    try {
      writer = new ArchiveHistoryWriter(archiveDirectory, maxSegmentSize,
          new java.util.LinkedHashSet<>(participantEngines.keySet()));
      if (!recoveredHead.equals(writer.committedHeadMeta())) {
        throw new ArchivePersistenceException(
            "Recovered archive head changed before normal writer attachment");
      }
      asyncSink = new AsyncArchiveHistorySink(writer, queueCapacity);
      Path checkpoint = archiveDirectory.resolve("progress").resolve("checkpoint.progress");
      Path reader = archiveDirectory.resolve("progress").resolve("reader.progress");
      ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(writer,
          checkpoint, participantEngines, reader, new ArrayList<>(participantEngines.keySet()),
          snapshotManager::withArchiveStateBarrier);
      candidate = new ArchiveRuntimeAttachment(collector, projectionPreparer, asyncSink,
          (payloads, refresh) -> publishOneTarget(coordinator, payloads, refresh));
      snapshotManager.attachArchiveRuntime(candidate);
      attached = true;
      attachment = candidate;
      sink = asyncSink;
      historyWriter = writer;
      state = State.RUNNING;
      return writer;
    } catch (IOException | RuntimeException failure) {
      if (attached) {
        snapshotManager.detachArchiveRuntime(candidate);
      }
      if (asyncSink != null) {
        try {
          asyncSink.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      } else if (writer != null) {
        try {
          writer.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  public synchronized ArchiveHistoryWriter getHistoryWriter() {
    if (state != State.RUNNING || historyWriter == null) {
      throw new IllegalStateException("State Archive normal writer is not attached");
    }
    return historyWriter;
  }

  /** Machine-checks the current H=C=D[0..26]=R identity and retired mutation plan. */
  public synchronized BlockSnapshotMeta verifyNormalWriteFixedPoint() throws IOException {
    ArchiveHistoryWriter writer = getHistoryWriter();
    HistoryCommitMarker head = Objects.requireNonNull(writer.committedHead(),
        "archive history head");
    List<String> names = new ArrayList<>(participantEngines.keySet());
    Path checkpointPath = archiveDirectory.resolve("progress").resolve("checkpoint.progress");
    if (new ArchiveTargetMutationPlanFile(checkpointPath).loadIfPresent() != null) {
      throw new ArchivePersistenceException("Archive mutation plan is not retired");
    }
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    ArchiveProgressEnvelope checkpoint = new ArchiveProgressFile(checkpointPath, codec).load();
    ArchiveProgressEnvelope reader = new ArchiveProgressFile(
        archiveDirectory.resolve("progress").resolve("reader.progress"), codec).load();
    requireAuthority(checkpoint, Kind.APPLY_CHECKPOINT, null, head, names);
    requireAuthority(reader, Kind.READER_VISIBLE, null, head, names);
    if (!java.util.Arrays.equals(checkpoint.getMutationPlanDigest(),
        reader.getMutationPlanDigest())) {
      throw new ArchivePersistenceException("Archive C/R mutation-plan identity differs");
    }
    for (Map.Entry<String, ArchiveParticipant> entry : participantEngines.entrySet()) {
      ArchiveProgressEnvelope progress = entry.getValue().loadProgress();
      requireAuthority(progress, Kind.PARTICIPANT_PROGRESS, entry.getKey(), head, names);
      if (!java.util.Arrays.equals(checkpoint.getMutationPlanDigest(),
          progress.getMutationPlanDigest())) {
        throw new ArchivePersistenceException(
            "Archive participant mutation-plan identity differs: " + entry.getKey());
      }
    }
    if (snapshotManager.getArchiveReadableEpoch() != head.getMeta().getEpoch()) {
      throw new ArchivePersistenceException("SnapshotManager readable epoch differs from R");
    }
    return head.getMeta();
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
    if (queryGate != null) {
      queryGate.quiesce();
    }
    if (!detached) {
      ArchiveRuntimeAttachment returned = snapshotManager.detachArchiveRuntime(attachment);
      if (returned != attachment) {
        throw new IllegalStateException("SnapshotManager returned a foreign archive attachment");
      }
      detached = true;
    }
    if (queryGate != null && !queryGate.isDrained()) {
      throw new IllegalStateException(
          "State Archive runtime still has active query leases: "
              + queryGate.getActiveLeaseCount());
    }
    if (queryGate != null) {
      queryGate.close();
    }

    IOException failure = null;
    if (latestCoordinator != null) {
      failure = closeOwned("latest coordinator", latestCoordinator, failure);
    }
    if (servingCatalog != null) {
      failure = closeOwned("serving catalog", servingCatalog, failure);
    }
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

  private static Map<String, ArchiveParticipant> immutableParticipantEngines(
      Map<String, ? extends ArchiveParticipant> engines) {
    Map<String, ? extends ArchiveParticipant> source = Objects.requireNonNull(engines, "engines");
    Map<String, ArchiveParticipant> copy = new LinkedHashMap<>();
    source.forEach((name, engine) -> copy.put(Objects.requireNonNull(name, "participant name"),
        Objects.requireNonNull(engine, "participant engine")));
    return Collections.unmodifiableMap(copy);
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

  private static void publishOneTarget(ArchiveTargetApplyCoordinator coordinator,
      List<ArchiveBlockForwardPayload> payloads,
      ArchiveStateBarrier.ArchiveStateAction refresh) throws IOException {
    if (payloads.size() != 1) {
      throw new ArchivePersistenceException(
          "S1 archive runtime requires one forward payload per normal flush");
    }
    ArchiveBlockForwardPayload payload = payloads.get(0);
    ArchiveParticipantMutationBatch batch = new ArchiveParticipantMutationBatchCollector(
        payload.getAccountAssetManifest()).collect(payload.getMarker(), payload.getView());
    coordinator.apply(batch, refresh);
  }

  private static void requireAuthority(ArchiveProgressEnvelope envelope, Kind kind,
      String participant, HistoryCommitMarker marker, List<String> participants) {
    if (envelope == null) {
      throw new ArchivePersistenceException("Missing archive authority: " + kind);
    }
    envelope.requireIdentity(kind, participant, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), participants);
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
