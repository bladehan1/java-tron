package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.Lease;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveDataHandlePoolV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void admitsBeforeOpenPreopensAllAndReusesPositionedHandles() throws Exception {
    Fixture fixture = createFixture("preopen", 102, 650);
    List<String> events = new ArrayList<>();
    List<FileChannel> channels = new ArrayList<>();
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.openForPreopen(
        fixture.root, fixture.tail, fixture.target,
        (required, reserve) -> events.add("budget:" + required + ":" + reserve));
        StateArchiveDataHandlePoolV5 pool = StateArchiveDataHandlePoolV5.open(view, path -> {
          events.add("open:" + path.getFileName());
          FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
          channels.add(channel);
          return channel;
        })) {
      assertEquals("budget:15:8192", events.get(0));
      assertEquals(15, pool.getHandleCount());
      assertEquals(16, events.size());

      Path source = StateArchiveFiveLaneWriterV5.dataPath(fixture.root, 0, 2);
      Path hidden = source.resolveSibling(source.getFileName() + ".hidden");
      Files.move(source, hidden);
      try {
        StateArchivePointReaderV5 reader = new StateArchivePointReaderV5(view, pool);
        assertEquals(OldValue.present(bytes(102)),
            reader.readCommittedOldValue("code", bytes(1), 102));

        FutureTask<OldValue> first = new FutureTask<>(
            () -> reader.readCommittedOldValue("code", bytes(1), 101));
        FutureTask<OldValue> second = new FutureTask<>(
            () -> reader.readCommittedOldValue("code", bytes(2), 101));
        Thread left = new Thread(first, "archive-v5-positioned-left");
        Thread right = new Thread(second, "archive-v5-positioned-right");
        left.start();
        right.start();
        assertEquals(OldValue.present(bytes(101)), first.get(2, TimeUnit.SECONDS));
        assertEquals(OldValue.absent(), second.get(2, TimeUnit.SECONDS));
        left.join();
        right.join();
      } finally {
        Files.move(hidden, source);
      }
      assertEquals(16, events.size());
      for (FileChannel channel : channels) {
        assertTrue(channel.isOpen());
      }
    }
    for (FileChannel channel : channels) {
      assertFalse(channel.isOpen());
    }
  }

  @Test
  public void rejectsBudgetAndApplicationCapBeforeDataOpen() throws Exception {
    Fixture fixture = createFixture("budget", 102, 650);
    AtomicInteger admissionCalls = new AtomicInteger();
    IOException failure = assertThrows(IOException.class,
        () -> StateArchiveCommittedViewV5.openForPreopen(
            fixture.root, fixture.tail, fixture.target, (required, reserve) -> {
              admissionCalls.incrementAndGet();
              assertEquals(15, required);
              assertEquals(8192, reserve);
              throw new IOException("injected process FD rejection");
            }));
    assertEquals("injected process FD rejection", failure.getMessage());
    assertEquals(1, admissionCalls.get());

    StateArchiveDataHandlePoolV5.requireApplicationCap(50_000);
    assertThrows(IOException.class,
        () -> StateArchiveDataHandlePoolV5.requireApplicationCap(50_001));

    List<LaneTerminal> excessive = new ArrayList<>();
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      excessive.add(new LaneTerminal(laneId, StateArchiveTailV5.LANE_ACTIVE, 10_000,
          StateArchiveLaneIndexV5.expectedLength(1), 608, hash(laneId), hash(20 + laneId)));
    }
    StateArchiveTailV5 excessiveTail = StateArchiveTailV5.forTarget(100, target(100),
        hash(100), excessive);
    AtomicInteger excessiveAdmission = new AtomicInteger();
    assertThrows(IOException.class, () -> StateArchiveCommittedViewV5.openForPreopen(
        temporaryFolder.getRoot().toPath().resolve("must-not-open"), excessiveTail,
        target(100), (required, reserve) -> excessiveAdmission.incrementAndGet()));
    assertEquals(0, excessiveAdmission.get());

    AtomicInteger opens = new AtomicInteger();
    try (StateArchiveCommittedViewV5 unadmitted = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      assertThrows(IllegalStateException.class,
          () -> StateArchiveDataHandlePoolV5.open(unadmitted, path -> {
            opens.incrementAndGet();
            return FileChannel.open(path, StandardOpenOption.READ);
          }));
    }
    assertEquals(0, opens.get());
  }

  @Test
  public void closesEveryHandleAfterPartialOpenFailure() throws Exception {
    Fixture fixture = createFixture("partial", 102, 650);
    List<FileChannel> opened = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.openForPreopen(
        fixture.root, fixture.tail, fixture.target, (required, reserve) -> { })) {
      assertThrows(IOException.class, () -> StateArchiveDataHandlePoolV5.open(view, path -> {
        if (attempts.incrementAndGet() == 4) {
          throw new IOException("injected fourth open failure");
        }
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        opened.add(channel);
        return channel;
      }));
    }
    assertEquals(4, attempts.get());
    assertEquals(3, opened.size());
    for (FileChannel channel : opened) {
      assertFalse(channel.isOpen());
    }
  }

  @Test
  public void closeWaitsForInflightLeaseThenRejectsNewReads() throws Exception {
    Fixture fixture = createFixture("inflight", 100,
        StateArchiveGethFormatV5.SEGMENT_TARGET_BYTES);
    List<FileChannel> channels = new ArrayList<>();
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.openForPreopen(
        fixture.root, fixture.tail, fixture.target, (required, reserve) -> { })) {
      StateArchiveDataHandlePoolV5 pool = StateArchiveDataHandlePoolV5.open(view, path -> {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        channels.add(channel);
        return channel;
      });
      Lease lease = pool.acquire(view.capture(0, 100));
      FutureTask<Void> closing = new FutureTask<>(() -> {
        pool.close();
        return null;
      });
      Thread closer = new Thread(closing, "archive-v5-handle-pool-close");
      closer.start();
      assertThrows(TimeoutException.class, () -> closing.get(200, TimeUnit.MILLISECONDS));
      lease.close();
      closing.get(2, TimeUnit.SECONDS);
      closer.join();

      assertThrows(IOException.class, () -> pool.acquire(view.capture(0, 100)));
      pool.close();
    }
    assertEquals(5, channels.size());
    for (FileChannel channel : channels) {
      assertFalse(channel.isOpen());
    }
  }

  private Fixture createFixture(String name, long lastBlock, long rotationBytes)
      throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    CommonCheckpointTarget target = target((int) lastBlock);
    StateArchiveTailV5 tail;
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99), rotationBytes)) {
      for (long block = 100; block <= lastBlock; block++) {
        writer.append(diff(block));
      }
      tail = writer.forceTailReady(target);
    }
    return new Fixture(root, tail, target);
  }

  private static BlockReverseDiff diff(long blockNumber) {
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber,
        hash((int) blockNumber), hash((int) blockNumber - 1), blockNumber * 3_000),
        Arrays.asList(
            new DbGroup("code", Arrays.asList(
                new Entry(bytes(1), OldValue.present(bytes((int) blockNumber))),
                new Entry(bytes(2), OldValue.absent()))),
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
