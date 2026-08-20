package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveTargetApplyCoordinator.Stage;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;

/** End-to-end ownership and recovery test from block capture to durable mixed participants. */
public class ArchiveBlockForwardMutationRecoveryTest extends BaseMethodTest {

  private static final List<String> PARTICIPANTS = participants();

  @Test
  public void captureBatchRecoversOnlyRemainingParticipantsFromDurablePlan() throws Exception {
    Path archive = temporaryFolder.newFolder("capture-recovery").toPath();
    List<HistoryCommitMarker> markers = initializeHistory(archive, 1);
    HistoryCommitMarker initial = markers.get(0);
    HistoryCommitMarker target = markers.get(1);
    Path checkpointPath = archive.resolve("progress/checkpoint.progress");
    Path readerPath = archive.resolve("progress/reader.progress");
    ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(checkpointPath, progressCodec).store(global(Kind.APPLY_CHECKPOINT,
        initial));
    new ArchiveProgressFile(readerPath, progressCodec).store(global(Kind.READER_VISIBLE, initial));

    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] canonicalAccount = bytes(3, 3);
    byte[] assetKey = append(accountKey, 4);
    byte[] assetValue = bytes(3, 5);
    byte[] proposalKey = bytes(2, 6);
    byte[] proposalValue = bytes(3, 7);
    byte[] expectedAccountKey = copy(accountKey);
    byte[] expectedCanonicalAccount = copy(canonicalAccount);
    byte[] expectedAssetKey = copy(assetKey);
    byte[] expectedAssetValue = copy(assetValue);
    byte[] expectedProposalKey = copy(proposalKey);
    byte[] expectedProposalValue = copy(proposalValue);

    LevelDbArchiveParticipant account = new LevelDbArchiveParticipant(
        archive.resolve("participants/account"), "account", PARTICIPANTS);
    RocksDbArchiveParticipant accountAsset = new RocksDbArchiveParticipant(
        archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS);
    Map<String, CountingParticipant> counted = new LinkedHashMap<>();
    Map<String, MemoryParticipant> memory = new LinkedHashMap<>();
    Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();
    try {
      for (String participant : PARTICIPANTS) {
        ArchiveParticipant delegate;
        if ("account".equals(participant)) {
          delegate = account;
        } else if ("account-asset".equals(participant)) {
          delegate = accountAsset;
        } else {
          MemoryParticipant inMemory = new MemoryParticipant();
          memory.put(participant, inMemory);
          delegate = inMemory;
        }
        CountingParticipant engine = new CountingParticipant(delegate);
        engine.apply(Collections.emptyList(), participant(participant, initial));
        counted.put(participant, engine);
        engines.put(participant, engine);
      }

      ArchiveParticipantMutationBatch batch;
      try (ViewFixture viewFixture = new ViewFixture()) {
        ArchiveBlockForwardMutationCapture capture = new ArchiveBlockForwardMutationCapture(
            target.getMeta(), new ArchiveBlockForwardMutationLimits(
                10, 10, 1024, 1024, 1024 * 1024));
        capture.recordAccount(target.getMeta(), accountKey,
            BlockChangeView.PostValue.present(rawAccount),
            BlockChangeView.PostValue.present(canonicalAccount));
        capture.recordAssetPut(target.getMeta(), accountKey, assetKey, assetValue);
        BlockChangeView view = viewFixture.capture(target.getMeta(), databases -> {
          databases.get("account").put(accountKey, rawAccount);
          databases.get("proposal").put(proposalKey, proposalValue);
        });
        capture.attach(view);
        batch = capture.seal(target);
      }

      Arrays.fill(accountKey, (byte) 9);
      Arrays.fill(rawAccount, (byte) 9);
      Arrays.fill(canonicalAccount, (byte) 9);
      Arrays.fill(assetKey, (byte) 9);
      Arrays.fill(assetValue, (byte) 9);
      Arrays.fill(proposalKey, (byte) 9);
      Arrays.fill(proposalValue, (byte) 9);

      String firstParticipant = PARTICIPANTS.get(0);
      String secondParticipant = PARTICIPANTS.get(1);
      try (HistoryCommitStore history = new HistoryCommitStore(
          archive, new HistoryCommitMarkerCodec())) {
        ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
            checkpointPath, engines, readerPath, PARTICIPANTS, action -> action.run(),
            (stage, participant) -> {
              if (stage == Stage.AFTER_PARTICIPANT
                  && firstParticipant.equals(participant)) {
                throw new IOException("injected after first participant");
              }
            }, temporary -> { });
        assertThrows(IOException.class, () -> coordinator.apply(batch, () -> { }));
      }

      assertEquals(2, counted.get(firstParticipant).getApplyCount());
      assertEquals(1, counted.get(secondParticipant).getApplyCount());
      AtomicInteger refreshes = new AtomicInteger();
      try (ArchiveParticipantRecoveryStorage recovery =
          new ArchiveParticipantRecoveryStorage(archive, 4096, checkpointPath, engines,
              readerPath, PARTICIPANTS, action -> action.run(), refreshes::incrementAndGet)) {
        new ArchiveRecoveryExecutor(recovery).recover();
      }

      assertEquals(2, counted.get(firstParticipant).getApplyCount());
      for (String participant : PARTICIPANTS) {
        assertEquals(2, counted.get(participant).getApplyCount());
      }
      assertEquals(1, refreshes.get());
      assertArrayEquals(expectedCanonicalAccount, account.get(expectedAccountKey));
      assertArrayEquals(expectedAssetValue, accountAsset.get(expectedAssetKey));
      assertArrayEquals(expectedProposalValue,
          memory.get("proposal").get(expectedProposalKey));

      ArchiveProgressEnvelope checkpoint =
          new ArchiveProgressFile(checkpointPath, progressCodec).load();
      ArchiveProgressEnvelope reader =
          new ArchiveProgressFile(readerPath, progressCodec).load();
      byte[] planDigest = checkpoint.getMutationPlanDigest();
      assertArrayEquals(planDigest, account.loadProgress().getMutationPlanDigest());
      assertArrayEquals(planDigest, accountAsset.loadProgress().getMutationPlanDigest());
      assertArrayEquals(planDigest, reader.getMutationPlanDigest());
      assertEquals(target.getMeta().getEpoch(), reader.getEpoch());
      assertFalse(Files.exists(new ArchiveTargetMutationPlanFile(checkpointPath).getPath()));

      try (ArchiveParticipantRecoveryStorage fixed =
          new ArchiveParticipantRecoveryStorage(archive, 4096, checkpointPath, engines,
              readerPath, PARTICIPANTS)) {
        assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
      }
    } finally {
      accountAsset.close();
      account.close();
    }
  }

  @Test
  public void consecutiveCaptureTargetsReplaceDigestAndRecoverPutDelete() throws Exception {
    Path archive = temporaryFolder.newFolder("consecutive-capture-recovery").toPath();
    List<HistoryCommitMarker> markers = initializeHistory(archive, 2);
    HistoryCommitMarker initial = markers.get(0);
    HistoryCommitMarker firstTarget = markers.get(1);
    HistoryCommitMarker secondTarget = markers.get(2);
    Path checkpointPath = archive.resolve("progress/checkpoint.progress");
    Path readerPath = archive.resolve("progress/reader.progress");
    ArchiveProgressEnvelopeCodec progressCodec = new ArchiveProgressEnvelopeCodec();
    new ArchiveProgressFile(checkpointPath, progressCodec).store(global(Kind.APPLY_CHECKPOINT,
        initial));
    new ArchiveProgressFile(readerPath, progressCodec).store(global(Kind.READER_VISIBLE, initial));

    byte[] accountKey = bytes(2, 1);
    byte[] assetKey = append(accountKey, 4);
    byte[] proposalKey = bytes(2, 6);
    byte[] firstCanonical = bytes(3, 11);
    byte[] firstAsset = bytes(3, 12);
    byte[] firstProposal = bytes(3, 13);
    byte[] secondCanonical = bytes(3, 21);
    byte[] secondProposal = bytes(3, 23);

    try (ParticipantFixture participants = new ParticipantFixture(archive, initial)) {
      ArchiveParticipantMutationBatch firstBatch = capture(firstTarget, accountKey,
          bytes(3, 10), firstCanonical, assetKey, firstAsset, false,
          proposalKey, firstProposal);
      try (HistoryCommitStore history = new HistoryCommitStore(
          archive, new HistoryCommitMarkerCodec())) {
        new ArchiveTargetApplyCoordinator(history, checkpointPath, participants.engines,
            readerPath, PARTICIPANTS, action -> action.run()).apply(firstBatch, () -> { });
      }

      ArchiveProgressEnvelope firstCheckpoint =
          new ArchiveProgressFile(checkpointPath, progressCodec).load();
      byte[] firstDigest = firstCheckpoint.getMutationPlanDigest();
      assertArrayEquals(firstCanonical, participants.account.get(accountKey));
      assertArrayEquals(firstAsset, participants.accountAsset.get(assetKey));
      assertArrayEquals(firstProposal,
          participants.memory.get("proposal").get(proposalKey));
      assertFalse(Files.exists(new ArchiveTargetMutationPlanFile(checkpointPath).getPath()));
      for (String participant : PARTICIPANTS) {
        assertEquals(2, participants.counted.get(participant).getApplyCount());
      }

      ArchiveParticipantMutationBatch secondBatch = capture(secondTarget, accountKey,
          bytes(3, 20), secondCanonical, assetKey, null, true,
          proposalKey, secondProposal);
      String failureParticipant = "account-asset";
      int failureIndex = PARTICIPANTS.indexOf(failureParticipant);
      try (HistoryCommitStore history = new HistoryCommitStore(
          archive, new HistoryCommitMarkerCodec())) {
        ArchiveTargetApplyCoordinator coordinator = new ArchiveTargetApplyCoordinator(history,
            checkpointPath, participants.engines, readerPath, PARTICIPANTS,
            action -> action.run(),
            (stage, participant) -> failAfter(stage, participant, failureParticipant),
            temporary -> { });
        assertThrows(IOException.class, () -> coordinator.apply(secondBatch, () -> { }));
      }

      for (int index = 0; index < PARTICIPANTS.size(); index++) {
        int expected = index <= failureIndex ? 3 : 2;
        assertEquals(expected,
            participants.counted.get(PARTICIPANTS.get(index)).getApplyCount());
      }
      AtomicInteger refreshes = new AtomicInteger();
      try (ArchiveParticipantRecoveryStorage recovery =
          new ArchiveParticipantRecoveryStorage(archive, 4096, checkpointPath,
              participants.engines, readerPath, PARTICIPANTS, action -> action.run(),
              refreshes::incrementAndGet)) {
        new ArchiveRecoveryExecutor(recovery).recover();
      }

      for (String participant : PARTICIPANTS) {
        assertEquals(3, participants.counted.get(participant).getApplyCount());
      }
      assertEquals(1, refreshes.get());
      assertArrayEquals(secondCanonical, participants.account.get(accountKey));
      assertNull(participants.accountAsset.get(assetKey));
      assertArrayEquals(secondProposal,
          participants.memory.get("proposal").get(proposalKey));

      ArchiveProgressEnvelope secondCheckpoint =
          new ArchiveProgressFile(checkpointPath, progressCodec).load();
      ArchiveProgressEnvelope reader =
          new ArchiveProgressFile(readerPath, progressCodec).load();
      byte[] secondDigest = secondCheckpoint.getMutationPlanDigest();
      assertFalse(Arrays.equals(firstDigest, secondDigest));
      assertArrayEquals(secondDigest,
          participants.account.loadProgress().getMutationPlanDigest());
      assertArrayEquals(secondDigest,
          participants.accountAsset.loadProgress().getMutationPlanDigest());
      assertArrayEquals(secondDigest, reader.getMutationPlanDigest());
      assertEquals(secondTarget.getMeta().getEpoch(), reader.getEpoch());
      assertFalse(Files.exists(new ArchiveTargetMutationPlanFile(checkpointPath).getPath()));

      try (ArchiveParticipantRecoveryStorage fixed =
          new ArchiveParticipantRecoveryStorage(archive, 4096, checkpointPath,
              participants.engines, readerPath, PARTICIPANTS)) {
        assertEquals(0, new ArchiveRecoveryExecutor(fixed).recover().getActions().size());
      }
    }
  }

  private static ArchiveParticipantMutationBatch capture(HistoryCommitMarker target,
      byte[] accountKey, byte[] rawAccount, byte[] canonicalAccount, byte[] assetKey,
      byte[] assetValue, boolean deleteAsset, byte[] proposalKey, byte[] proposalValue) {
    try (ViewFixture viewFixture = new ViewFixture()) {
      ArchiveBlockForwardMutationCapture capture = new ArchiveBlockForwardMutationCapture(
          target.getMeta(), new ArchiveBlockForwardMutationLimits(
              10, 10, 1024, 1024, 1024 * 1024));
      capture.recordAccount(target.getMeta(), accountKey,
          BlockChangeView.PostValue.present(rawAccount),
          BlockChangeView.PostValue.present(canonicalAccount));
      if (deleteAsset) {
        capture.recordAssetDelete(target.getMeta(), accountKey, assetKey);
      } else {
        capture.recordAssetPut(target.getMeta(), accountKey, assetKey, assetValue);
      }
      BlockChangeView view = viewFixture.capture(target.getMeta(), databases -> {
        databases.get("account").put(accountKey, rawAccount);
        databases.get("proposal").put(proposalKey, proposalValue);
      });
      capture.attach(view);
      return capture.seal(target);
    }
  }

  private static void failAfter(Stage stage, String participant, String failureParticipant)
      throws IOException {
    if (stage == Stage.AFTER_PARTICIPANT && failureParticipant.equals(participant)) {
      throw new IOException("injected during second target");
    }
  }

  private static List<HistoryCommitMarker> initializeHistory(Path archive, int lastEpoch)
      throws Exception {
    List<HistoryCommitMarker> markers = new ArrayList<>();
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      for (int epoch = 0; epoch <= lastEpoch; epoch++) {
        BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, hash(epoch),
            hash(epoch - 1), epoch * 1_000L);
        BlockReverseDiff diff = new BlockReverseDiff(meta,
            Collections.singletonList(new BlockReverseDiff.DbGroup("account",
                Collections.singletonList(new BlockReverseDiff.Entry(bytes(2, epoch + 10),
                    OldValue.absent())))));
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation location = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(meta, epoch - 1L, body, location,
            bytes(16, epoch + 40), PARTICIPANTS));
      }
      bodies.sync();
      index.sync();
      commits.commitAll(markers);
      ArchiveRestartCheckpoint.persist(archive, commits.firstEpoch(), commits.size(),
          commits.getRecordLength(), commits.head(), new HistoryCommitMarkerCodec());
    }
    return markers;
  }

  private static ArchiveProgressEnvelope participant(String participant,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, participant,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
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

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static byte[] append(byte[] prefix, int suffix) {
    byte[] result = Arrays.copyOf(prefix, prefix.length + 1);
    result[result.length - 1] = (byte) suffix;
    return result;
  }

  private static byte[] copy(byte[] value) {
    return Arrays.copyOf(value, value.length);
  }

  @FunctionalInterface
  private interface Mutator {
    void mutate(Map<String, Chainbase> databases);
  }

  private static final class ViewFixture implements AutoCloseable {
    private final SnapshotManager manager = new SnapshotManager("");
    private final Map<String, Chainbase> databases = new LinkedHashMap<>();
    private final List<Chainbase> ordered = new ArrayList<>();

    private ViewFixture() {
      for (String participant : PARTICIPANTS) {
        Chainbase database = new Chainbase(new SnapshotRoot(new ViewMemoryDb(participant)));
        databases.put(participant, database);
        ordered.add(database);
        manager.add(database);
      }
      manager.enable();
    }

    private BlockChangeView capture(BlockSnapshotMeta meta, Mutator mutator) {
      try (ISession session = manager.buildSession()) {
        mutator.mutate(databases);
        return BlockChangeView.capture(meta, ordered);
      }
    }

    @Override
    public void close() {
      manager.shutdown();
    }
  }

  private static final class ParticipantFixture implements AutoCloseable {
    private final LevelDbArchiveParticipant account;
    private final RocksDbArchiveParticipant accountAsset;
    private final Map<String, CountingParticipant> counted = new LinkedHashMap<>();
    private final Map<String, MemoryParticipant> memory = new LinkedHashMap<>();
    private final Map<String, ArchiveParticipant> engines = new LinkedHashMap<>();

    private ParticipantFixture(Path archive, HistoryCommitMarker initial) throws IOException {
      account = new LevelDbArchiveParticipant(
          archive.resolve("participants/account"), "account", PARTICIPANTS);
      accountAsset = new RocksDbArchiveParticipant(
          archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS);
      for (String participant : PARTICIPANTS) {
        ArchiveParticipant delegate;
        if ("account".equals(participant)) {
          delegate = account;
        } else if ("account-asset".equals(participant)) {
          delegate = accountAsset;
        } else {
          MemoryParticipant inMemory = new MemoryParticipant();
          memory.put(participant, inMemory);
          delegate = inMemory;
        }
        CountingParticipant engine = new CountingParticipant(delegate);
        engine.apply(Collections.emptyList(), participant(participant, initial));
        counted.put(participant, engine);
        engines.put(participant, engine);
      }
    }

    @Override
    public void close() throws IOException {
      accountAsset.close();
      account.close();
    }
  }

  private static final class CountingParticipant implements ArchiveParticipant {
    private final ArchiveParticipant delegate;
    private int applyCount;

    private CountingParticipant(ArchiveParticipant delegate) {
      this.delegate = delegate;
    }

    @Override
    public void apply(List<ArchiveParticipantMutation> mutations,
        ArchiveProgressEnvelope progress) throws IOException {
      delegate.apply(mutations, progress);
      applyCount++;
    }

    @Override
    public ArchiveProgressEnvelope loadProgress() throws IOException {
      return delegate.loadProgress();
    }

    private int getApplyCount() {
      return applyCount;
    }
  }

  private static final class MemoryParticipant implements ArchiveParticipant {
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();
    private ArchiveProgressEnvelope progress;

    @Override
    public void apply(List<ArchiveParticipantMutation> mutations,
        ArchiveProgressEnvelope progress) {
      for (ArchiveParticipantMutation mutation : mutations) {
        byte[] value = mutation.getValue();
        WrappedByteArray key = WrappedByteArray.copyOf(mutation.getKey());
        if (value == null) {
          values.remove(key);
        } else {
          values.put(key, copy(value));
        }
      }
      this.progress = progress;
    }

    @Override
    public ArchiveProgressEnvelope loadProgress() {
      return progress;
    }

    private byte[] get(byte[] key) {
      byte[] value = values.get(WrappedByteArray.of(key));
      return value == null ? null : copy(value);
    }
  }

  private static final class ViewMemoryDb implements DB<byte[], byte[]>, Flusher {
    private final String name;
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();

    private ViewMemoryDb(String name) {
      this.name = name;
    }

    @Override
    public byte[] get(byte[] key) {
      byte[] value = values.get(WrappedByteArray.of(key));
      return value == null ? null : copy(value);
    }

    @Override
    public void put(byte[] key, byte[] value) {
      values.put(WrappedByteArray.copyOf(key), copy(value));
    }

    @Override
    public long size() {
      return values.size();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public void remove(byte[] key) {
      values.remove(WrappedByteArray.of(key));
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
      values.forEach((key, value) -> entries.add(new AbstractMap.SimpleImmutableEntry<>(
          key.getBytes(), copy(value))));
      return entries.iterator();
    }

    @Override
    public void close() {
      values.clear();
    }

    @Override
    public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
      batch.forEach((key, value) -> {
        if (value == null || value.getBytes() == null) {
          values.remove(key);
        } else {
          values.put(WrappedByteArray.copyOf(key.getBytes()), value.getBytes());
        }
      });
    }

    @Override
    public void reset() {
      values.clear();
    }

    @Override
    public String getDbName() {
      return name;
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], byte[]> newInstance() {
      return new ViewMemoryDb(name);
    }
  }
}
