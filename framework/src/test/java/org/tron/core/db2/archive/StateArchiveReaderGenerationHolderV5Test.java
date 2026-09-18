package org.tron.core.db2.archive;

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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveReaderGenerationHolderV5.PinnedReader;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveReaderGenerationHolderV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void atomicallyPublishesAndLetsPinnedOldGenerationDrain() throws Exception {
    Fixture oldFixture = createFixture("old-generation", 100);
    Fixture newFixture = createFixture("new-generation", 101);
    List<FileChannel> oldChannels = new ArrayList<>();
    List<FileChannel> newChannels = new ArrayList<>();
    StateArchiveReaderGenerationV5 oldGeneration = open(oldFixture, oldChannels);
    StateArchiveReaderGenerationHolderV5 holder =
        new StateArchiveReaderGenerationHolderV5(oldGeneration);
    PinnedReader oldReader = holder.pin();

    holder.publish(open(newFixture, newChannels));
    try (PinnedReader newReader = holder.pin()) {
      assertEquals(101, newReader.getCommittedBlockNumber());
      assertEquals(OldValue.present(bytes(101)),
          newReader.readCommittedOldValue("code", bytes(1), 101));
    }
    assertEquals(100, oldReader.getCommittedBlockNumber());
    assertEquals(OldValue.present(bytes(100)),
        oldReader.readCommittedOldValue("code", bytes(1), 100));
    assertAllOpen(oldChannels);
    assertAllOpen(newChannels);

    oldReader.close();
    assertAllClosed(oldChannels);
    assertAllOpen(newChannels);
    holder.close();
    assertAllClosed(newChannels);
  }

  @Test
  public void failedBuildAndNonAdvancingPublishPreserveCurrentGeneration() throws Exception {
    Fixture currentFixture = createFixture("current-generation", 100);
    List<FileChannel> currentChannels = new ArrayList<>();
    StateArchiveReaderGenerationHolderV5 holder = new StateArchiveReaderGenerationHolderV5(
        open(currentFixture, currentChannels));

    Fixture failedFixture = createFixture("failed-generation", 101);
    List<FileChannel> partialChannels = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(IOException.class, () -> StateArchiveReaderGenerationV5.open(
        failedFixture.root, failedFixture.tail, failedFixture.target,
        (required, reserve) -> { }, path -> {
          if (attempts.incrementAndGet() == 3) {
            throw new IOException("injected generation build failure");
          }
          FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
          partialChannels.add(channel);
          return channel;
        }));
    assertEquals(3, attempts.get());
    assertAllClosed(partialChannels);

    Fixture staleFixture = createFixture("stale-generation", 100);
    List<FileChannel> staleChannels = new ArrayList<>();
    StateArchiveReaderGenerationV5 stale = open(staleFixture, staleChannels);
    assertThrows(IOException.class, () -> holder.publish(stale));
    assertAllClosed(staleChannels);

    try (PinnedReader reader = holder.pin()) {
      assertEquals(100, reader.getCommittedBlockNumber());
      assertEquals(OldValue.present(bytes(100)),
          reader.readCommittedOldValue("code", bytes(1), 100));
    }
    assertAllOpen(currentChannels);
    holder.close();
    assertAllClosed(currentChannels);
  }

  @Test
  public void closeWinsPublishRaceAndWaitsForPinnedGeneration() throws Exception {
    Fixture currentFixture = createFixture("close-current", 100);
    Fixture replacementFixture = createFixture("close-replacement", 101);
    List<FileChannel> currentChannels = new ArrayList<>();
    List<FileChannel> replacementChannels = new ArrayList<>();
    StateArchiveReaderGenerationHolderV5 holder = new StateArchiveReaderGenerationHolderV5(
        open(currentFixture, currentChannels));
    PinnedReader pinned = holder.pin();
    StateArchiveReaderGenerationV5 replacement = open(replacementFixture,
        replacementChannels);

    FutureTask<Void> closing = new FutureTask<>(() -> {
      holder.close();
      return null;
    });
    Thread closer = new Thread(closing, "archive-v5-generation-holder-close");
    closer.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!holder.isClosed() && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(holder.isClosed());
    assertThrows(TimeoutException.class, () -> closing.get(200, TimeUnit.MILLISECONDS));
    assertThrows(IOException.class, () -> holder.publish(replacement));
    assertAllClosed(replacementChannels);
    assertAllOpen(currentChannels);

    pinned.close();
    closing.get(2, TimeUnit.SECONDS);
    closer.join();
    assertAllClosed(currentChannels);
    assertThrows(IOException.class, holder::pin);
  }

  private Fixture createFixture(String name, long lastBlock) throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    CommonCheckpointTarget target = target((int) lastBlock);
    StateArchiveTailV5 tail;
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      for (long block = 100; block <= lastBlock; block++) {
        writer.append(diff(block));
      }
      tail = writer.forceTailReady(target);
    }
    return new Fixture(root, tail, target);
  }

  private static StateArchiveReaderGenerationV5 open(Fixture fixture,
      List<FileChannel> channels) throws IOException {
    return StateArchiveReaderGenerationV5.open(fixture.root, fixture.tail, fixture.target,
        (required, reserve) -> { }, path -> {
          FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
          channels.add(channel);
          return channel;
        });
  }

  private static void assertAllOpen(List<FileChannel> channels) {
    assertFalse(channels.isEmpty());
    for (FileChannel channel : channels) {
      assertTrue(channel.isOpen());
    }
  }

  private static void assertAllClosed(List<FileChannel> channels) {
    assertFalse(channels.isEmpty());
    for (FileChannel channel : channels) {
      assertFalse(channel.isOpen());
    }
  }

  private static BlockReverseDiff diff(long blockNumber) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber,
        hash((int) blockNumber), hash((int) blockNumber - 1), blockNumber * 3_000),
        Arrays.asList(
            new DbGroup("code", Collections.singletonList(
                new Entry(bytes(1), OldValue.present(bytes((int) blockNumber))))),
            new DbGroup("account", Collections.singletonList(
                new Entry(fixedKey(21, 4), OldValue.present(bytes(4))))),
            new DbGroup("account-asset", Collections.singletonList(
                new Entry(bytes(5), OldValue.present(bytes(5))))),
            new DbGroup("delegation", Collections.singletonList(
                new Entry(bytes(13), OldValue.present(bytes(13))))),
            new DbGroup("storage-row", Collections.singletonList(
                new Entry(fixedKey(32, 22), OldValue.present(bytes(22)))))));
  }

  private static CommonCheckpointTarget target(int blockNumber) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + blockNumber), meta, meta,
        hash(90), hash(91));
  }

  private static byte[] fixedKey(int length, int suffix) {
    byte[] value = new byte[length];
    value[length - 1] = (byte) suffix;
    return value;
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

  private static final class Fixture {
    private final Path root;
    private final StateArchiveTailV5 tail;
    private final CommonCheckpointTarget target;

    private Fixture(Path root, StateArchiveTailV5 tail, CommonCheckpointTarget target) {
      this.root = root;
      this.tail = tail;
      this.target = target;
    }
  }
}
