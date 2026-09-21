package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.MutableIndex;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveTailPersistenceV5Test {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test
  public void syncsDistinctTailIdempotentlyAndReopensIt() throws Exception {
    Path indexPath = temporaryFolder.newFolder("v5-tail").toPath();
    CommonCheckpointTarget target100 = target(100);
    CommonCheckpointTarget target101 = target(101);
    StateArchiveTailV5 first = tail(100, 100, target100);
    StateArchiveTailV5 second = tail(100, 101, target101);

    try (MutableIndex index = new MutableIndex(indexPath, Engine.LEVELDB)) {
      assertThrows(ArchivePersistenceException.class,
          () -> index.archiveTailV5(target100));
      index.publishArchiveTailV5(first);
      index.publishArchiveTailV5(first);
      assertEquals(100, index.archiveTailV5(target100).getCommonBlockNumber());
      assertThrows(IllegalArgumentException.class, () -> index.archiveTailV5(target101));
      assertThrows(ArchivePersistenceException.class,
          () -> index.publishArchiveTailV5(tail(99, 101, target101)));
      index.publishArchiveTailV5(second);
      assertThrows(ArchivePersistenceException.class,
          () -> index.publishArchiveTailV5(first));
    }

    try (MutableIndex reopened = new MutableIndex(indexPath, Engine.LEVELDB)) {
      assertEquals(101, reopened.archiveTailV5(target101).getCommonBlockNumber());
    }
  }

  @Test
  public void rejectsWrongVersionChecksumAndCommonIdentity() throws Exception {
    CommonCheckpointTarget target = target(100);
    StateArchiveTailV5 tail = tail(100, 100, target);

    Path versionPath = temporaryFolder.newFolder("wrong-version").toPath();
    writeRawTail(versionPath, mutate(tail.encode(), 5));
    assertThrows(IllegalArgumentException.class,
        () -> new MutableIndex(versionPath, Engine.LEVELDB));

    Path checksumPath = temporaryFolder.newFolder("wrong-checksum").toPath();
    writeRawTail(checksumPath, mutate(tail.encode(), 100));
    assertThrows(IllegalArgumentException.class,
        () -> new MutableIndex(checksumPath, Engine.LEVELDB));

    Path commonPath = temporaryFolder.newFolder("wrong-common").toPath();
    try (MutableIndex index = new MutableIndex(commonPath, Engine.LEVELDB)) {
      index.publishArchiveTailV5(tail);
      assertThrows(IllegalArgumentException.class,
          () -> index.archiveTailV5(target(101)));
    }
  }

  private static void writeRawTail(Path indexPath, byte[] encoded) throws IOException {
    try (MutableIndex ignored = new MutableIndex(indexPath, Engine.LEVELDB)) {
      // Establish the engine manifest before writing the malformed reserved record.
    }
    try (StateArchiveIndexDatabase.Writer writer = StateArchiveIndexDatabase.openWriter(
        indexPath.resolve("keys"), Engine.LEVELDB)) {
      writer.write(Collections.singletonList(
          StateArchiveIndexDatabase.put(StateArchiveTailV5.key(), encoded)), true);
    }
  }

  private static byte[] mutate(byte[] source, int offset) {
    byte[] result = source.clone();
    result[offset] ^= 1;
    return result;
  }

  private static StateArchiveTailV5 tail(long firstBlock, long lastBlock,
      CommonCheckpointTarget target) {
    long frames = lastBlock - firstBlock + 1;
    List<LaneTerminal> terminals = new ArrayList<>();
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      terminals.add(new LaneTerminal(laneId, StateArchiveTailV5.LANE_ACTIVE, 0,
          StateArchiveLaneIndexV5.expectedLength(frames), 608 + laneId,
          hash(10 + laneId), hash(20 + laneId)));
    }
    return StateArchiveTailV5.forTarget(firstBlock, target, hash(30), terminals);
  }

  private static CommonCheckpointTarget target(int blockNumber) {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L);
    return CommonCheckpointTarget.restore(hash(70), hash(80 + blockNumber), meta, meta,
        hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    return ByteBuffer.allocate(32).putInt(value).array();
  }
}
