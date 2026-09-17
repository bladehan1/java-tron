package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.tron.core.db2.archive.StateArchiveTailV5.LaneTerminal;
import org.tron.core.db2.core.CommonCheckpointTarget;

public class StateArchiveTailV5Test {

  private static final String TAIL_GOLDEN_HASH =
      "bac3f0e53752dee8cda8de870927909361b07e6faab660235fbc54f55db46296";

  @Test
  public void freezesExactTailAndBindsCommonAndFiveLanes() {
    CommonCheckpointTarget target = target(101);
    StateArchiveTailV5 tail = StateArchiveTailV5.forTarget(100, target, hash(99),
        terminals(100, 101));

    byte[] encoded = tail.encode();
    assertEquals(672, encoded.length);
    assertArrayEquals(new byte[]{0x7f, 0x53, 0x41, 0x05}, StateArchiveTailV5.key());
    assertEquals(TAIL_GOLDEN_HASH, hex(StateArchiveFileFormatV3.sha256(encoded)));
    StateArchiveTailV5 decoded = StateArchiveTailV5.decode(encoded);
    decoded.requireTarget(target);
    assertEquals(100, decoded.getFirstBlockNumber());
    assertEquals(101, decoded.getCommonBlockNumber());
    assertArrayEquals(hash(99), decoded.getResultHistoryDigest());
    assertEquals(5, decoded.getLanes().size());
    assertEquals(22, decoded.getLanes().get(4).getLaneId());
    assertEquals(152, decoded.getLanes().get(0).getCommittedIndexBytes());
  }

  @Test
  public void rejectsHeaderLaneAndDerivedBoundaryCorruption() {
    byte[] encoded = StateArchiveTailV5.forTarget(100, target(101), hash(99),
        terminals(100, 101)).encode();
    byte[] headerCorrupt = encoded.clone();
    headerCorrupt[80] ^= 1;
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV5.decode(headerCorrupt));

    byte[] laneCorrupt = encoded.clone();
    laneCorrupt[StateArchiveGethFormatV5.TAIL_HEADER_LENGTH + 24] ^= 1;
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV5.decode(laneCorrupt));

    List<LaneTerminal> wrongOrder = terminals(100, 101);
    Collections.swap(wrongOrder, 0, 1);
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV5.forTarget(100, target(101), hash(99), wrongOrder));

    List<LaneTerminal> wrongIndexLength = terminals(100, 101);
    wrongIndexLength.set(2, terminal(5, 144, 600));
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV5.forTarget(100, target(101), hash(99), wrongIndexLength));
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveTailV5.forTarget(102, target(101), hash(99),
            terminals(102, 101)));
  }

  private static List<LaneTerminal> terminals(long firstBlock, long endBlock) {
    long frames = endBlock - firstBlock + 1;
    long indexBytes = StateArchiveLaneIndexV5.expectedLength(frames);
    List<LaneTerminal> result = new ArrayList<>();
    for (int laneId : StateArchiveGethFormatV5.laneIds()) {
      result.add(terminal(laneId, indexBytes, 600 + laneId));
    }
    return result;
  }

  private static LaneTerminal terminal(int laneId, long indexBytes, long dataEnd) {
    return new LaneTerminal(laneId, StateArchiveTailV5.LANE_ACTIVE, laneId + 1,
        indexBytes, dataEnd, hash(10 + laneId), hash(20 + laneId));
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

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte element : value) {
      encoded.append(String.format("%02x", element));
    }
    return encoded.toString();
  }
}
