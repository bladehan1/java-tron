package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveCataloglessWriterV4Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void rotatesBothMembersReadsAndReopensWithLazyRecoveredStats() throws Exception {
    Path root = temporaryFolder.newFolder("catalogless-v4").toPath().resolve("v4");
    byte[] baseline = hash(90);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    EncodedBundle first = codec.encode(diff(1, 1_400), baseline,
        StateArchiveFileFormatV3.COMPRESSION_NONE);
    EncodedBundle second = codec.encode(diff(2, 0), first.getResultHistoryDigest(),
        StateArchiveFileFormatV3.COMPRESSION_NONE);
    StateArchiveTailV4 tail;

    try (StateArchiveCataloglessWriterV4 writer = writer(root, baseline, null, null)) {
      writer.append(first, true);
      writer.append(second, true);
      tail = writer.force(target(2));
      writer.publish(tail);
      assertDiffs(writer.readCommittedDiffs(0, 2, Long.MAX_VALUE),
          diff(1, 1_400), diff(2, 0));
      assertArrayEquals(new byte[1_400],
          writer.readCommittedOldValue("code", new byte[]{1}, 1).getValue());
      assertThrows(java.io.IOException.class,
          () -> writer.readCommittedOldValue("code", new byte[]{2}, 1));

      StateArchiveSegmentNamesV4.SegmentPair sealed = StateArchiveSegmentNamesV4.sealed(
          root.resolve("segments"), 0, 0, 1, 1);
      StateArchiveSegmentNamesV4.SegmentPair current = StateArchiveSegmentNamesV4.current(
          root.resolve("segments"), 1, 0, 2);
      assertTrue(Files.isRegularFile(sealed.getData()));
      assertTrue(Files.isRegularFile(sealed.getIndex()));
      assertTrue(Files.isRegularFile(current.getData()));
      assertTrue(Files.isRegularFile(current.getIndex()));
      assertFalse(Files.exists(StateArchiveSegmentNamesV4.current(
          root.resolve("segments"), 0, 0, 1).getData()));
    }

    StateArchiveTailV4 tail4;
    try (StateArchiveCataloglessWriterV4 reopened = writer(root, baseline, target(2), tail)) {
      assertEquals(2, reopened.getAppendHead().getBlockNumber());
      EncodedBundle third = codec.encode(diff(3, 0), reopened.getResultHistoryDigest(),
          StateArchiveFileFormatV3.COMPRESSION_NONE);
      reopened.append(third, true);
      EncodedBundle fourth = codec.encode(diff(4, 0), third.getResultHistoryDigest(),
          StateArchiveFileFormatV3.COMPRESSION_NONE);
      reopened.append(fourth, true);
      tail4 = reopened.force(target(4));
      reopened.publish(tail4);
      assertEquals(4, reopened.readCommittedDiffs(3, 4, Long.MAX_VALUE)
          .get(0).getMeta().getBlockNumber());
      assertEquals(10, files(root, ".dat").size());
      assertTrue(files(root, ".dat").stream()
          .anyMatch(path -> path.getFileName().toString()
              .equals("account_00000000000000000001-00000000000000000003.dat")));
    }

    try (StateArchiveCataloglessWriterV4 reopened = writer(root, baseline, target(4), tail4)) {
      assertDiffs(reopened.readCommittedDiffs(0, 4, Long.MAX_VALUE),
          diff(1, 1_400), diff(2, 0), diff(3, 0), diff(4, 0));
    }
  }

  @Test
  public void truncatesUnpublishedCurrentSuffixToTail() throws Exception {
    Path root = temporaryFolder.newFolder("catalogless-v4-tail").toPath().resolve("v4");
    byte[] baseline = hash(91);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    StateArchiveTailV4 tail;
    try (StateArchiveCataloglessWriterV4 writer = writer(root, baseline, null, null)) {
      EncodedBundle first = codec.encode(diff(1, 0), baseline,
          StateArchiveFileFormatV3.COMPRESSION_NONE);
      writer.append(first, true);
      tail = writer.force(target(1));
      writer.publish(tail);
      writer.append(codec.encode(diff(2, 0), first.getResultHistoryDigest(),
          StateArchiveFileFormatV3.COMPRESSION_NONE), false);
      assertEquals(2, writer.getAppendHead().getBlockNumber());
    }

    try (StateArchiveCataloglessWriterV4 reopened = writer(root, baseline, target(1), tail)) {
      assertEquals(1, reopened.getAppendHead().getBlockNumber());
      assertDiffs(reopened.readCommittedDiffs(0, 1, Long.MAX_VALUE), diff(1, 0));
    }
  }

  @Test
  public void rejectsCorruptIndexAndLegacyV3Root() throws Exception {
    Path parent = temporaryFolder.newFolder("catalogless-v4-corrupt").toPath();
    Path root = parent.resolve("v4");
    byte[] baseline = hash(92);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    try (StateArchiveCataloglessWriterV4 writer = writer(root, baseline, null, null)) {
      writer.append(codec.encode(diff(1, 0), baseline,
          StateArchiveFileFormatV3.COMPRESSION_NONE), true);
      StateArchiveTailV4 tail = writer.force(target(1));
      writer.publish(tail);
      Path index = StateArchiveSegmentNamesV4.current(root.resolve("segments"), 0, 0, 1)
          .getIndex();
      try (FileChannel channel = FileChannel.open(index, StandardOpenOption.WRITE)) {
        ByteBuffer wrongBlock = ByteBuffer.allocate(Long.BYTES);
        wrongBlock.putLong(2).flip();
        channel.write(wrongBlock,
            StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH);
      }
      assertThrows(java.io.IOException.class,
          () -> writer.readCommittedDiffs(0, 1, Long.MAX_VALUE));
    }

    Path legacyParent = temporaryFolder.newFolder("catalogless-v4-legacy").toPath();
    Files.createDirectories(legacyParent.resolve("v3"));
    assertThrows(java.io.IOException.class,
        () -> writer(legacyParent.resolve("v4"), baseline, null, null));
  }

  @Test
  public void rejectsUnpublishedFilesMetaSuffixAndCommonTailMismatch() throws Exception {
    Path root = temporaryFolder.newFolder("catalogless-v4-unpublished-seal").toPath()
        .resolve("v4");
    byte[] baseline = hash(93);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    StateArchiveTailV4 tail;
    EncodedBundle previous;
    try (StateArchiveCataloglessWriterV4 writer = writer(root, baseline, null, null)) {
      previous = codec.encode(diff(1, 0), baseline,
          StateArchiveFileFormatV3.COMPRESSION_NONE);
      writer.append(previous, true);
      tail = writer.force(target(1));
      writer.publish(tail);
      for (int block = 2; block <= 4; block++) {
        previous = codec.encode(diff(block, 0), previous.getResultHistoryDigest(),
            StateArchiveFileFormatV3.COMPRESSION_NONE);
        writer.append(previous, true);
      }
    }

    assertThrows(IllegalArgumentException.class, () -> writer(root, baseline, target(1), tail));
    assertThrows(IllegalArgumentException.class,
        () -> writer(root, baseline, target(2), tail));
  }

  @Test
  public void rejectsOneSidedSealAfterPublishedTail() throws Exception {
    Path root = temporaryFolder.newFolder("catalogless-v4-partial-seal").toPath()
        .resolve("v4");
    byte[] baseline = hash(94);
    StateArchiveTailV4 tail;
    try (StateArchiveCataloglessWriterV4 writer = writer(root, baseline, null, null)) {
      writer.append(new StateArchiveFiveLaneBlockCodecV3().encode(diff(1, 0), baseline,
          StateArchiveFileFormatV3.COMPRESSION_NONE), true);
      tail = writer.force(target(1));
      writer.publish(tail);
    }

    Path segments = root.resolve("segments");
    StateArchiveSegmentNamesV4.SegmentPair current = StateArchiveSegmentNamesV4.current(
        segments, 0, 0, 1);
    StateArchiveSegmentNamesV4.SegmentPair sealed = StateArchiveSegmentNamesV4.sealed(
        segments, 0, 0, 1, 1);
    Files.move(current.getIndex(), sealed.getIndex());

    assertThrows(java.io.IOException.class, () -> writer(root, baseline, target(1), tail));
  }

  private static StateArchiveCataloglessWriterV4 writer(Path root, byte[] baseline,
      CommonCheckpointTarget target, StateArchiveTailV4 tail) throws Exception {
    return new StateArchiveCataloglessWriterV4(root, baseline,
        StateArchiveFileFormatV3.COMPRESSION_NONE, 1_500, target, tail);
  }

  private static void assertDiffs(List<BlockReverseDiff> actual,
      BlockReverseDiff... expected) {
    assertEquals(expected.length, actual.size());
    BlockHistoryCodec codec = new BlockHistoryCodec();
    for (int index = 0; index < expected.length; index++) {
      assertArrayEquals(codec.encode(expected[index]), codec.encode(actual.get(index)));
    }
  }

  private static BlockReverseDiff diff(int block, int valueBytes) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3_000L);
    if (valueBytes == 0) {
      return new BlockReverseDiff(meta, Collections.emptyList());
    }
    return new BlockReverseDiff(meta, Collections.singletonList(new DbGroup("code",
        Collections.singletonList(new Entry(new byte[]{1},
            OldValue.present(new byte[valueBytes]))))));
  }

  private static CommonCheckpointTarget target(int block) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3_000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + block), meta, meta,
        hash(90), hash(91));
  }

  private static List<Path> files(Path root, String suffix) throws Exception {
    try (Stream<Path> stream = Files.walk(root)) {
      return stream.filter(path -> Files.isRegularFile(path)
          && path.getFileName().toString().endsWith(suffix))
          .collect(java.util.stream.Collectors.toList());
    }
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
