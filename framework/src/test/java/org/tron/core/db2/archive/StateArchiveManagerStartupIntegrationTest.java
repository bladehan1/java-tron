package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.Storage;
import org.tron.core.db.Manager;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.StateArchiveRuntimeOwner.State;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.DynamicPropertiesStore;

public class StateArchiveManagerStartupIntegrationTest {

  private static final List<String> PARTICIPANTS = participants();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void managerRecoversExact27NativeFilesBeforeOpeningProducers() throws Exception {
    for (String engine : Arrays.asList("LEVELDB", "ROCKSDB")) {
      Path output = temporaryFolder.newFolder("manager-" + engine.toLowerCase()).toPath();
      Path archive = output.resolve("state-archive");
      HistoryCommitMarker head = initializeRecoverableTail(archive, engine);
      SnapshotManager snapshots = new SnapshotManager("");
      Manager manager = manager(snapshots, head);

      withArchiveConfig(output, engine, true, () -> invoke(manager, "initStateArchive"));

      assertEquals(State.RECOVERED, manager.getStateArchiveRuntime().getState());
      assertEquals(1, manager.getStateArchiveRuntime().getStartupRecoveryActionCount());
      assertEquals(head.getMeta(), manager.getStateArchiveRuntime().getRecoveredHead());
      assertNull(manager.getArchiveHistoryWriter());
      assertEquals(-1, snapshots.getArchiveReadableEpoch());

      invoke(manager, "closeStateArchive");
      assertNull(manager.getStateArchiveRuntime());
      assertNativeParticipantsReopen(archive, engine, PARTICIPANTS);
    }
  }

  @Test
  public void partialParticipantOpenRollsBackAndPreservesFailureEvidence() throws Exception {
    for (String engine : Arrays.asList("LEVELDB", "ROCKSDB")) {
      Path output = temporaryFolder.newFolder("partial-" + engine.toLowerCase()).toPath();
      Path archive = output.resolve("state-archive");
      HistoryCommitMarker head = initializeHistoryAndGlobalProgress(archive);
      int failureIndex = 3;
      List<String> openedNames = PARTICIPANTS.subList(0, failureIndex);
      initializeParticipants(archive, engine, openedNames, head);
      Path failurePath = archive.resolve("participants").resolve(PARTICIPANTS.get(failureIndex));
      byte[] evidence = new byte[]{4, 5, 6, 7};
      Files.write(failurePath, evidence);
      Manager manager = manager(new SnapshotManager(""), head);

      assertThrows(IllegalStateException.class,
          () -> withArchiveConfig(output, engine, true,
              () -> invoke(manager, "initStateArchive")));

      assertNull(manager.getStateArchiveRuntime());
      assertArrayEquals(evidence, Files.readAllBytes(failurePath));
      assertNativeParticipantsReopen(archive, engine, openedNames);
    }
  }

  @Test
  public void disabledManagerControlDoesNotInspectOrCreateArchiveRuntime() throws Exception {
    Path output = temporaryFolder.newFolder("disabled-manager").toPath();
    Path archive = output.resolve("state-archive");
    Files.createDirectories(archive);
    byte[] evidence = new byte[]{8, 9, 10};
    Files.write(archive.resolve("unexpected-evidence"), evidence);
    Manager manager = new Manager();

    withArchiveConfig(output, "LEVELDB", false, () -> invoke(manager, "initStateArchive"));

    assertNull(manager.getStateArchiveRuntime());
    assertArrayEquals(evidence, Files.readAllBytes(archive.resolve("unexpected-evidence")));
    assertFalse(Files.exists(archive.resolve("participants")));
  }

  private static Manager manager(SnapshotManager snapshots, HistoryCommitMarker head)
      throws Exception {
    DynamicPropertiesStore properties = mock(DynamicPropertiesStore.class);
    when(properties.getLatestBlockHeaderNumber()).thenReturn(head.getMeta().getBlockNumber());
    when(properties.getLatestBlockHeaderHash())
        .thenReturn(Sha256Hash.wrap(head.getMeta().getBlockHash()));
    ChainBaseManager chainBase = mock(ChainBaseManager.class);
    when(chainBase.getDynamicPropertiesStore()).thenReturn(properties);
    Manager manager = new Manager();
    setField(manager, "revokingStore", snapshots);
    setField(manager, "chainBaseManager", chainBase);
    return manager;
  }

  private static HistoryCommitMarker initializeRecoverableTail(Path archive, String engine)
      throws Exception {
    HistoryCommitMarker checkpoint;
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
        archive, 4096, ArchiveStoreScope.getStateDatabases())) {
      writer.accept(new BlockReverseDiff(
          new BlockSnapshotMeta(6, 6, hash(6), hash(5), 6_000L),
          Collections.emptyList()));
      writer.accept(new BlockReverseDiff(
          new BlockSnapshotMeta(7, 7, hash(7), hash(6), 7_000L),
          Collections.emptyList()));
      checkpoint = writer.get(6);
    }
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(archive.resolve("progress/checkpoint.progress"), codec)
        .store(global(Kind.APPLY_CHECKPOINT, checkpoint));
    new ArchiveProgressFile(archive.resolve("progress/reader.progress"), codec)
        .store(global(Kind.READER_VISIBLE, checkpoint));
    initializeParticipants(archive, engine, PARTICIPANTS, checkpoint);
    return checkpoint;
  }

  private static HistoryCommitMarker initializeHistoryAndGlobalProgress(Path archive)
      throws Exception {
    HistoryCommitMarker head;
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
        archive, 4096, ArchiveStoreScope.getStateDatabases())) {
      writer.accept(new BlockReverseDiff(
          new BlockSnapshotMeta(7, 7, hash(7), hash(6), 7_000L),
          Collections.emptyList()));
      head = writer.committedHead();
    }
    ArchiveProgressEnvelopeCodec codec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(archive.resolve("progress/checkpoint.progress"), codec)
        .store(global(Kind.APPLY_CHECKPOINT, head));
    new ArchiveProgressFile(archive.resolve("progress/reader.progress"), codec)
        .store(global(Kind.READER_VISIBLE, head));
    return head;
  }

  private static void initializeParticipants(Path archive, String engine, List<String> names,
      HistoryCommitMarker head) throws Exception {
    List<Closeable> opened = new ArrayList<>();
    try {
      for (String name : names) {
        ArchiveParticipant participant = openParticipant(archive, engine, name);
        opened.add((Closeable) participant);
        participant.apply(Collections.emptyList(), participant(name, head));
      }
    } finally {
      closeReverse(opened);
    }
  }

  private static void assertNativeParticipantsReopen(Path archive, String engine,
      List<String> names) throws Exception {
    List<Closeable> reopened = new ArrayList<>();
    try {
      for (String name : names) {
        reopened.add((Closeable) openParticipant(archive, engine, name));
      }
    } finally {
      closeReverse(reopened);
    }
  }

  private static ArchiveParticipant openParticipant(Path archive, String engine, String name)
      throws Exception {
    Path directory = archive.resolve("participants").resolve(name);
    return "ROCKSDB".equals(engine)
        ? new RocksDbArchiveParticipant(directory, name, PARTICIPANTS)
        : new LevelDbArchiveParticipant(directory, name, PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope participant(String name, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, name,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static void withArchiveConfig(Path output, String engine, boolean enabled,
      ThrowingRunnable action) throws Exception {
    CommonParameter args = CommonParameter.getInstance();
    Storage oldStorage = args.getStorage();
    Storage storage = oldStorage == null ? new Storage() : oldStorage;
    args.storage = storage;
    String oldOutput = args.outputDirectory;
    String oldDirectory = storage.getStateArchiveDirectory();
    String oldEngine = storage.getDbEngine();
    long oldSegmentSize = storage.getStateArchiveMaxSegmentSize();
    int oldQueueCapacity = storage.getStateArchiveQueueCapacity();
    boolean oldEnabled = storage.isStateArchiveEnabled();
    try {
      args.outputDirectory = output.toString();
      storage.setStateArchiveDirectory("state-archive");
      storage.setDbEngine(engine);
      storage.setStateArchiveMaxSegmentSize(4096);
      storage.setStateArchiveQueueCapacity(4);
      storage.setStateArchiveEnabled(enabled);
      action.run();
    } finally {
      args.outputDirectory = oldOutput;
      storage.setStateArchiveDirectory(oldDirectory);
      storage.setDbEngine(oldEngine);
      storage.setStateArchiveMaxSegmentSize(oldSegmentSize);
      storage.setStateArchiveQueueCapacity(oldQueueCapacity);
      storage.setStateArchiveEnabled(oldEnabled);
      args.storage = oldStorage;
    }
  }

  private static void closeReverse(List<? extends Closeable> resources) throws Exception {
    Exception failure = null;
    for (int i = resources.size() - 1; i >= 0; i--) {
      try {
        resources.get(i).close();
      } catch (Exception closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void invoke(Manager manager, String name) throws Exception {
    Method method = Manager.class.getDeclaredMethod(name);
    method.setAccessible(true);
    try {
      method.invoke(manager);
    } catch (InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw failure;
    }
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return Collections.unmodifiableList(participants);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
