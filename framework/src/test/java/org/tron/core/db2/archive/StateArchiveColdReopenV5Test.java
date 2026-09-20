package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveReaderGenerationHolderV5.PinnedReader;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveColdReopenV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test
  public void coldReopensWithoutBodyScanAndRepublishesWhileOldPinDrains()
      throws Exception {
    Path root = temporaryFolder.newFolder("v5-cold-reopen").toPath();
    byte[] format = hash(70);
    byte[] baseline = hash(99);
    List<BlockReverseDiff> initial = Arrays.asList(diff(100, true), diff(101, false));

    try (StateArchiveAppendCheckpointMaterializerV5 archive = fresh(
        root, format, baseline)) {
      publish(archive, format, initial);
    }

    assertThrows(IOException.class, () -> reopened(root, format, hash(98),
        new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));

    List<FileChannel> channels = new ArrayList<>();
    List<Integer> metadataReads = new ArrayList<>();
    List<Long> admissions = new ArrayList<>();
    StateArchiveAppendCheckpointMaterializerV5 archive = reopened(root, format, baseline,
        channels, metadataReads, admissions);
    assertEquals(Collections.singletonList(10L), admissions);
    assertEquals(5, channels.size());
    assertMetadataOnly(metadataReads);

    PinnedReader oldReader = archive.pinReader();
    assertEquals(101, oldReader.getCommittedBlockNumber());
    assertEquals(OldValue.present(bytes(101)),
        oldReader.readCommittedOldValue("code", bytes(1), 101));

    CommonCheckpointTarget successorTarget = publish(archive, format,
        Collections.singletonList(diff(102, false)));
    assertEquals(Arrays.asList(10L, 10L), admissions);
    assertEquals(10, channels.size());
    assertMetadataOnly(metadataReads);
    assertAllOpen(channels.subList(0, 5));
    assertAllOpen(channels.subList(5, 10));

    try (PinnedReader newReader = archive.pinReader()) {
      assertEquals(102, newReader.getCommittedBlockNumber());
      assertEquals(OldValue.present(bytes(102)),
          newReader.readCommittedOldValue("code", bytes(1), 102));
    }
    assertEquals(OldValue.present(bytes(101)),
        oldReader.readCommittedOldValue("code", bytes(1), 101));
    oldReader.close();
    assertAllClosed(channels.subList(0, 5));
    assertAllOpen(channels.subList(5, 10));

    archive.close();
    assertAllClosed(channels.subList(5, 10));

    byte[] uninterruptedDigest;
    Path oracleRoot = temporaryFolder.newFolder("v5-uninterrupted-oracle").toPath();
    try (StateArchiveFiveLaneWriterV5 oracle = new StateArchiveFiveLaneWriterV5(
        oracleRoot, 100, baseline)) {
      oracle.append(diff(100, true));
      oracle.append(diff(101, false));
      oracle.append(diff(102, false));
      uninterruptedDigest = oracle.getResultHistoryDigest();
    }
    Path index = root.resolve(StateArchiveServingIndexBuildCoordinatorV3.DIRECTORY)
        .resolve("single-v1");
    try (PersistentServingKeyIndexGeneration.MutableIndex persisted =
        new PersistentServingKeyIndexGeneration.MutableIndex(index, Engine.LEVELDB)) {
      assertArrayEquals(uninterruptedDigest,
          persisted.archiveTailV5(successorTarget).getResultHistoryDigest());
    }
  }

  private static StateArchiveAppendCheckpointMaterializerV5 fresh(Path root,
      byte[] format, byte[] baseline) throws IOException {
    return new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
        100, baseline, StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
        (required, reserve) -> { });
  }

  private static StateArchiveAppendCheckpointMaterializerV5 reopened(Path root,
      byte[] format, byte[] baseline, List<FileChannel> channels,
      List<Integer> metadataReads, List<Long> admissions) throws IOException {
    return new StateArchiveAppendCheckpointMaterializerV5(root, format, Engine.LEVELDB,
        100, baseline, StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES,
        (required, reserve) -> admissions.add(required), path -> {
      FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
      channels.add(channel);
      return channel;
    }, (path, position, length) -> metadataReads.add(length),
        (stage, laneId) -> { }, () -> { });
  }

  private static CommonCheckpointTarget publish(
      StateArchiveAppendCheckpointMaterializerV5 archive, byte[] format,
      List<BlockReverseDiff> diffs) throws IOException {
    StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
    CommonCheckpointPayload payload = payload(format, diffs, descriptor);
    CommonCheckpointTarget target = archive.prepare(
        CommonCheckpointCapture.create(payload, diffs, descriptor));
    archive.materialize(payload, target);
    archive.publish(target);
    return target;
  }

  private static void assertMetadataOnly(List<Integer> reads) {
    assertFalse(reads.isEmpty());
    for (int length : reads) {
      assertTrue("unexpected segment body read: " + length,
          length == StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
              || length == StateArchiveFileFormatV3.HASH_LENGTH);
    }
  }

  private static void assertAllOpen(List<FileChannel> channels) {
    for (FileChannel channel : channels) {
      assertTrue(channel.isOpen());
    }
  }

  private static void assertAllClosed(List<FileChannel> channels) {
    for (FileChannel channel : channels) {
      assertFalse(channel.isOpen());
    }
  }

  private static CommonCheckpointPayload payload(byte[] format,
      List<BlockReverseDiff> diffs, StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      PathStateFlushTarget.BlockBinding binding =
          org.mockito.Mockito.mock(PathStateFlushTarget.BlockBinding.class);
      org.mockito.Mockito.when(binding.getMeta()).thenReturn(meta);
      org.mockito.Mockito.when(binding.getParentStateRoot())
          .thenReturn(hash(30 + (int) meta.getBlockNumber()));
      org.mockito.Mockito.when(binding.getStateRoot())
          .thenReturn(hash(31 + (int) meta.getBlockNumber()));
      org.mockito.Mockito.when(binding.getTransitionPayloadDigest()).thenReturn(hash(90));
      bindings.add(binding);
    }
    PathStateFlushTarget path = org.mockito.Mockito.mock(PathStateFlushTarget.class);
    byte[] parentStateRoot = bindings.get(0).getParentStateRoot();
    byte[] stateRoot = bindings.get(bindings.size() - 1).getStateRoot();
    org.mockito.Mockito.when(path.getBlocks()).thenReturn(bindings);
    org.mockito.Mockito.when(path.getParentStateRoot()).thenReturn(parentStateRoot);
    org.mockito.Mockito.when(path.getStateRoot()).thenReturn(stateRoot);
    org.mockito.Mockito.when(path.getStores()).thenReturn(Collections.emptyList());
    org.mockito.Mockito.when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.createV2(format, path, descriptor, Collections.emptyList());
  }

  private static BlockReverseDiff diff(int blockNumber, boolean largeUnrelatedValue) {
    List<Entry> entries = new ArrayList<>();
    entries.add(new Entry(bytes(1), OldValue.present(bytes(blockNumber))));
    if (largeUnrelatedValue) {
      entries.add(new Entry(bytes(2), OldValue.present(new byte[1024 * 1024])));
    }
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L),
        Collections.singletonList(new DbGroup("code", entries)));
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int index = 0; index < values.length; index++) {
      result[index] = (byte) values[index];
    }
    return result;
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
