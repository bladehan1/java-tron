package org.tron.core.db2.archive.history.v5;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.history.format.V5Format;
import org.tron.core.db2.archive.history.format.V5HistoryTail;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

public class HistoryTailStoreTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void tornPreparedSlotCannotEraseTheCommittedBoundary() throws Exception {
    Path root = temporary.newFolder().toPath();
    V5HistoryTail committed;
    V5HistoryTail prepared;
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, hash(99));
        HistoryTailStore tails = new HistoryTailStore(root, hash(70), 100, hash(99))) {
      writer.append(diff(100));
      committed = writer.forceTailReady(target(100));
      tails.publish(committed, null, () -> { });
      writer.append(diff(101));
      prepared = writer.forceTailReady(target(101));
      tails.publish(prepared, target(100), () -> { });
    }
    // Slot 0 is the prepared boundary; slot 1 must remain independently readable.
    try (FileChannel file = FileChannel.open(root.resolve(HistoryTailStore.FILE_NAME),
        StandardOpenOption.WRITE)) {
      file.write(ByteBuffer.wrap(new byte[]{7}), 20);
      file.force(true);
    }
    try (HistoryTailStore tails = new HistoryTailStore(root, hash(70), 100, hash(99))) {
      assertArrayEquals(committed.encode(), tails.require(target(100)).encode());
      assertThrows(IOException.class, () -> tails.require(target(101)));
    }
  }

  @Test
  public void commonCommitSelectsOlderSlotAndTrimsOnlyTheUncommittedSuffix() throws Exception {
    Path root = temporary.newFolder().toPath();
    V5HistoryTail committed;
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, hash(99));
        HistoryTailStore tails = new HistoryTailStore(root, hash(70), 100, hash(99))) {
      writer.append(diff(100));
      committed = writer.forceTailReady(target(100));
      tails.publish(committed, null, () -> { });
      writer.append(diff(101));
      tails.publish(writer.forceTailReady(target(101)), target(100), () -> { });
    }
    try (HistoryTailStore tails = new HistoryTailStore(root, hash(70), 100, hash(99))) {
      HistorySuffixRecovery.trim(root, tails.require(target(100)), target(100));
    }
    HistorySuffixRecovery.trim(root, committed, target(100)); // interrupted recovery is retryable.
    try (StateArchiveFiveLaneWriterV5 reopened = StateArchiveFiveLaneWriterV5.reopen(root,
        committed, target(100), hash(99), V5Format.SEGMENT_TARGET_BYTES)) {
      assertEquals(100, reopened.getAppendHead().getBlockNumber());
      reopened.append(diff(101));
    }
  }

  @Test
  public void corruptCommittedFrameIsRefusedBeforeAnyTrim() throws Exception {
    Path root = temporary.newFolder().toPath();
    V5HistoryTail committed;
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, hash(99))) {
      writer.append(diff(100));
      committed = writer.forceTailReady(target(100));
      writer.append(diff(101));
    }
    Path data = StateArchiveFiveLaneWriterV5.dataPath(root, 0, 0);
    long previousLength = Files.size(data);
    try (FileChannel file = FileChannel.open(data, StandardOpenOption.WRITE)) {
      file.write(ByteBuffer.wrap(new byte[]{99}), V5Format.SEGMENT_HEADER_LENGTH
          + V5Format.FRAME_HEADER_LENGTH + 80);
    }
    assertThrows(IOException.class,
        () -> HistorySuffixRecovery.trim(root, committed, target(100)));
    assertEquals(previousLength, Files.size(data));
  }

  @Test
  public void firstUncommittedRangeCanReturnToTheProvedBaseline() throws Exception {
    Path root = temporary.newFolder().toPath();
    try (StateArchiveFiveLaneWriterV5 writer = new StateArchiveFiveLaneWriterV5(
        root, 100, hash(99));
        HistoryTailStore tails = new HistoryTailStore(root, hash(70), 100, hash(99))) {
      writer.append(diff(100));
      tails.publish(writer.forceTailReady(target(100)), null, () -> { });
    }
    HistorySuffixRecovery.resetUncommittedBaseline(root, 100, hash(99));
    try (StateArchiveFiveLaneWriterV5 writer = StateArchiveFiveLaneWriterV5.reopenBaseline(root,
        100, hash(99), V5Format.SEGMENT_TARGET_BYTES)) {
      writer.append(diff(100));
      assertEquals(100, writer.forceTailReady(target(100)).getCommonBlockNumber());
    }
  }

  private static BlockReverseDiff diff(int block) {
    return new BlockReverseDiff(
        BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block),
        Collections.singletonList(new BlockReverseDiff.DbGroup("code",
            Collections.singletonList(new BlockReverseDiff.Entry(new byte[]{1},
                OldValue.present(new byte[]{(byte) block}))))));
  }

  private static CommonCheckpointTarget target(int block) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block);
    return CommonCheckpointTarget.restore(
        hash(70), hash(80 + block), meta, meta, hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    byte[] hash = new byte[32];
    hash[31] = (byte) value;
    return hash;
  }
}
