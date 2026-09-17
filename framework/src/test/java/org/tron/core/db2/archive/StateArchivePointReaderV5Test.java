package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchivePointReaderV5.ReadObserver;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchivePointReaderV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void readsVariableAndFixedKeysAndPreservesOldValueStates() throws Exception {
    Fixture fixture = createFixture("values");
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      assertEquals(OldValue.present(bytes(7, 8)),
          new StateArchivePointReaderV5(view).readCommittedOldValue("code", bytes(1), 100));
      assertEquals(OldValue.absent(), new StateArchivePointReaderV5(view)
          .readCommittedOldValue("account", fixedKey(21, 1), 100));
      assertEquals(OldValue.present(new byte[0]), new StateArchivePointReaderV5(view)
          .readCommittedOldValue("account-asset", bytes(5), 100));
      assertEquals(OldValue.present(bytes(13)), new StateArchivePointReaderV5(view)
          .readCommittedOldValue("delegation", bytes(13), 100));
      assertEquals(OldValue.present(bytes(22)), new StateArchivePointReaderV5(view)
          .readCommittedOldValue("storage-row", fixedKey(32, 22), 100));
    }
  }

  @Test
  public void opensReadsAndDecodesOnlyTargetLaneAndSkipsLargeUnrelatedValue() throws Exception {
    Fixture fixture = createFixture("target-lane");
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      RecordingObserver largeObserver = new RecordingObserver();
      assertEquals(OldValue.present(bytes(7, 8)),
          new StateArchivePointReaderV5(view, largeObserver)
              .readCommittedOldValue("code", bytes(1), 100));
      assertEquals(Collections.singleton(0), largeObserver.openedLanes);
      assertTrue(largeObserver.readBytes < 512);

      List<Path[]> hidden = hideOtherLaneData(fixture.root, 4);
      RecordingObserver observer = new RecordingObserver();
      try {
        OldValue value = new StateArchivePointReaderV5(view, observer)
            .readCommittedOldValue("account", fixedKey(21, 1), 100);
        assertEquals(OldValue.absent(), value);
        assertEquals(Collections.singleton(4), observer.openedLanes);
        assertEquals(Collections.singleton(4), observer.readLanes);
        assertEquals(Collections.singleton(4), observer.decodedLanes);
        assertEquals(1, observer.openCount);
        assertTrue(observer.readBytes < 512);
      } finally {
        restore(hidden);
      }
    }
  }

  @Test
  public void failsClosedForMissingKeyAndAccessedHeaderCorruption() throws Exception {
    Fixture fixture = createFixture("corruption");
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        fixture.root, fixture.tail, fixture.target)) {
      StateArchivePointReaderV5 reader = new StateArchivePointReaderV5(view);
      assertThrows(IOException.class,
          () -> reader.readCommittedOldValue("code", bytes(99), 100));

      Path data = StateArchiveFiveLaneWriterV5.dataPath(fixture.root, 0, 0);
      writeAt(data, StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH, new byte[]{0});
      assertThrows(IOException.class,
          () -> reader.readCommittedOldValue("code", bytes(1), 100));
    }

    Fixture keyOffsets = createFixture("key-offset-corruption");
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        keyOffsets.root, keyOffsets.tail, keyOffsets.target)) {
      Path data = StateArchiveFiveLaneWriterV5.dataPath(keyOffsets.root, 0, 0);
      long firstKeyOffset = StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
          + StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
          + StateArchiveGethFormatV5.SECTION_HEADER_LENGTH;
      writeAt(data, firstKeyOffset, new byte[]{0x7f});
      assertThrows(IOException.class, () -> new StateArchivePointReaderV5(view)
          .readCommittedOldValue("code", bytes(1), 100));
    }

    Fixture valueOffsets = createFixture("value-offset-corruption");
    try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
        valueOffsets.root, valueOffsets.tail, valueOffsets.target)) {
      Path data = StateArchiveFiveLaneWriterV5.dataPath(valueOffsets.root, 4, 0);
      long secondValueOffsetLastByte = StateArchiveGethFormatV5.SEGMENT_HEADER_LENGTH
          + StateArchiveGethFormatV5.FRAME_HEADER_LENGTH
          + StateArchiveGethFormatV5.SECTION_HEADER_LENGTH + 2L * 21 + 1 + 7;
      writeAt(data, secondValueOffsetLastByte, new byte[]{1});
      assertThrows(IOException.class, () -> new StateArchivePointReaderV5(view)
          .readCommittedOldValue("account", fixedKey(21, 1), 100));
    }
  }

  @Test
  public void readsWhileWriterMonitorIsHeldElsewhere() throws Exception {
    Path root = temporaryFolder.newFolder("writer-monitor").toPath();
    CommonCheckpointTarget target = target(100);
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      writer.append(diff(100));
      StateArchiveTailV5 tail = writer.forceTailReady(target);
      try (StateArchiveCommittedViewV5 view = StateArchiveCommittedViewV5.open(
          root, tail, target)) {
        StateArchivePointReaderV5 reader = new StateArchivePointReaderV5(view);
        FutureTask<OldValue> task = new FutureTask<>(
            () -> reader.readCommittedOldValue("code", bytes(1), 100));
        Thread thread = new Thread(task, "archive-v5-point-reader-test");
        synchronized (writer) {
          thread.start();
          assertEquals(OldValue.present(bytes(7, 8)), task.get(2, TimeUnit.SECONDS));
        }
        thread.join();
      }
    }
  }

  private Fixture createFixture(String name) throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    CommonCheckpointTarget target = target(100);
    StateArchiveTailV5 tail;
    try (StateArchiveFiveLaneWriterV5 writer =
        new StateArchiveFiveLaneWriterV5(root, 100, hash(99))) {
      writer.append(diff(100));
      tail = writer.forceTailReady(target);
    }
    return new Fixture(root, tail, target);
  }

  private static BlockReverseDiff diff(long blockNumber) {
    byte[] large = new byte[1024 * 1024];
    large[large.length - 1] = 1;
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber,
        hash((int) blockNumber), hash((int) blockNumber - 1), blockNumber * 3_000),
        Arrays.asList(
            new DbGroup("code", Arrays.asList(
                new Entry(bytes(1), OldValue.present(bytes(7, 8))),
                new Entry(bytes(2), OldValue.present(new byte[0])),
                new Entry(bytes(3), OldValue.present(large)))),
            new DbGroup("account", Arrays.asList(
                new Entry(fixedKey(21, 1), OldValue.absent()),
                new Entry(fixedKey(21, 2), OldValue.present(new byte[0])))),
            new DbGroup("account-asset", Collections.singletonList(
                new Entry(bytes(5), OldValue.present(new byte[0])))),
            new DbGroup("delegation", Collections.singletonList(
                new Entry(bytes(13), OldValue.present(bytes(13))))),
            new DbGroup("storage-row", Collections.singletonList(
                new Entry(fixedKey(32, 22), OldValue.present(bytes(22)))))));
  }

  private static List<Path[]> hideOtherLaneData(Path root, int retainedLane)
      throws Exception {
    List<Path[]> hidden = new ArrayList<>();
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      if (laneId == retainedLane) {
        continue;
      }
      Path source = StateArchiveFiveLaneWriterV5.dataPath(root, laneId, 0);
      Path destination = source.resolveSibling(source.getFileName() + ".hidden");
      Files.move(source, destination);
      hidden.add(new Path[]{source, destination});
    }
    return hidden;
  }

  private static void restore(List<Path[]> hidden) throws Exception {
    for (Path[] pair : hidden) {
      Files.move(pair[1], pair[0]);
    }
  }

  private static void writeAt(Path path, long position, byte[] value) throws Exception {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(value);
      while (buffer.hasRemaining()) {
        channel.write(buffer, position + buffer.position());
      }
    }
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

  private static final class RecordingObserver implements ReadObserver {
    private final Set<Integer> openedLanes = new HashSet<>();
    private final Set<Integer> readLanes = new HashSet<>();
    private final Set<Integer> decodedLanes = new HashSet<>();
    private int openCount;
    private long readBytes;

    @Override
    public void opened(int laneId, Path path) {
      openedLanes.add(laneId);
      openCount++;
    }

    @Override
    public void read(int laneId, long position, int bytes) {
      readLanes.add(laneId);
      readBytes += bytes;
    }

    @Override
    public void decodedValue(int laneId, int storeId, int valueBytes) {
      decodedLanes.add(laneId);
    }
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
