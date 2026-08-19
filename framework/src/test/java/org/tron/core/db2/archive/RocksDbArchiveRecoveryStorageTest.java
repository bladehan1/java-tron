package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.RocksDbArchiveParticipant.Mutation;

public class RocksDbArchiveRecoveryStorageTest {

  private static final List<String> PARTICIPANTS =
      Arrays.asList("account", "account-asset");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void executorConvergesHistoryNativeParticipantsAndReaderHead() throws Exception {
    Path archive = temporaryFolder.newFolder("native-recovery").toPath();
    Path checkpointPath = archive.resolve("progress/checkpoint.progress");
    Path readerPath = archive.resolve("progress/reader.progress");
    List<HistoryCommitMarker> markers = initializeHistory(archive, 3);
    new ArchiveProgressFile(checkpointPath, new ArchiveProgressEnvelopeCodec())
        .store(global(Kind.APPLY_CHECKPOINT, markers.get(1)));
    new ArchiveProgressFile(readerPath, new ArchiveProgressEnvelopeCodec())
        .store(global(Kind.READER_VISIBLE, markers.get(0)));

    try (RocksDbArchiveParticipant account = new RocksDbArchiveParticipant(
        archive.resolve("participants/account"), "account", PARTICIPANTS);
        RocksDbArchiveParticipant asset = new RocksDbArchiveParticipant(
            archive.resolve("participants/account-asset"), "account-asset", PARTICIPANTS)) {
      account.apply(Collections.emptyList(), participant("account", markers.get(1)));
      asset.apply(Collections.emptyList(), participant("account-asset", markers.get(0)));
      Map<String, RocksDbArchiveParticipant> engines = engines(account, asset);

      try (RocksDbArchiveRecoveryStorage storage = new RocksDbArchiveRecoveryStorage(
          archive, 4096, checkpointPath, engines, readerPath, PARTICIPANTS,
          (name, first, last) -> Collections.singletonList(
              Mutation.put(bytes("replayed"), bytes(name + ":" + first + "-" + last))))) {
        assertEquals(3, new ArchiveRecoveryExecutor(storage).recover().getActions().size());
      }

      assertEquals(2, asset.loadProgress().getEpoch());
      assertArrayEquals(bytes("account-asset:2-2"), asset.get(bytes("replayed")));
      assertEquals(2, new ArchiveProgressFile(readerPath,
          new ArchiveProgressEnvelopeCodec()).load().getEpoch());
      assertEquals(2, ArchiveRestartCheckpoint.load(archive,
          new HistoryCommitMarkerCodec()).getMarker().getMeta().getEpoch());
      assertFalse(Files.exists(archive.resolve("truncation.intent")));

      try (RocksDbArchiveRecoveryStorage reopened = new RocksDbArchiveRecoveryStorage(
          archive, 4096, checkpointPath, engines, readerPath, PARTICIPANTS,
          (name, first, last) -> {
            throw new AssertionError("fixed-point recovery must not replay");
          })) {
        assertEquals(0, new ArchiveRecoveryExecutor(reopened).recover().getActions().size());
      }
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
      for (int epoch = 1; epoch <= lastEpoch; epoch++) {
        BlockReverseDiff diff = new BlockReverseDiff(
            new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L),
            Collections.singletonList(new BlockReverseDiff.DbGroup("account",
                Collections.singletonList(new BlockReverseDiff.Entry(bytes("key-" + epoch),
                    OldValue.present(bytes("old-" + epoch)))))));
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation location = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(diff.getMeta(), epoch - 1L, body, location,
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

  private static Map<String, RocksDbArchiveParticipant> engines(
      RocksDbArchiveParticipant account, RocksDbArchiveParticipant asset) {
    Map<String, RocksDbArchiveParticipant> engines = new LinkedHashMap<>();
    engines.put("account", account);
    engines.put("account-asset", asset);
    return engines;
  }

  private static ArchiveProgressEnvelope participant(String name,
      HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(Kind.PARTICIPANT_PROGRESS, name,
        marker.getMeta().getEpoch(), marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static ArchiveProgressEnvelope global(Kind kind, HistoryCommitMarker marker) {
    return new ArchiveProgressEnvelope(kind, null, marker.getMeta().getEpoch(),
        marker.getMeta().getBlockHash(), marker.getBatchId(),
        marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
