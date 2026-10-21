package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.Boundary;
import org.tron.core.db2.archive.StateArchiveLaneIndexV5.FrameRange;

public class StateArchiveLaneIndexV5Test {

  private static final String FORMAT_DESCRIPTOR_HEX =
      "464d54350005000000000080008000080200003c0024005000c0006002a00005"
          + "0400000000000000800000005341493553414435534146355341543500000001"
          + "00010001a26b589aac877d7497185994b2f7e94df4aeacf1c695a4d968783ccb"
          + "e454dd9b00000000000000000000000000000000000000000000000000000000";
  private static final String STORE_MAPPING_DIGEST_HEX =
      "a26b589aac877d7497185994b2f7e94df4aeacf1c695a4d968783ccbe454dd9b";
  private static final String FORMAT_DIGEST_HEX =
      "a59e7b0995536b7e5374fde86ab55cbe12b9ecb4d56aa8abecb3b03ed65a9460";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void keepsFormatIdentityStable() {
    assertEquals(STORE_MAPPING_DIGEST_HEX,
        toHex(StateArchiveGethFormatV5.storeMappingDigest()));
    assertEquals(FORMAT_DIGEST_HEX, toHex(StateArchiveGethFormatV5.formatDigest()));
    assertArrayEquals(fromHex(FORMAT_DESCRIPTOR_HEX),
        StateArchiveGethFormatV5.formatDescriptor());
    assertEquals("c7d0673cc2e21c2e507e3f9f1383095cc19184b21a419a574daa1aedc3d9ec8a",
        toHex(StateArchiveAppendCheckpointMaterializerV5.baselineHistoryDigest(
            100, ByteBuffer.allocate(32).putInt(99).array())));
  }

  @Test
  public void initializesAppendsLocatesAndReopensAcrossRotation() throws Exception {
    Path index = temporaryFolder.newFolder("lane-index").toPath().resolve("blocks.idx");
    try (StateArchiveLaneIndexV5 writer = StateArchiveLaneIndexV5.create(index, 4, 100)) {
      assertEquals(0, writer.getFrameCount());
      assertEquals(136, Files.size(index));
      assertThrows(IllegalArgumentException.class, () -> writer.locate(100));

      writer.append(100, 0, 608);
      writer.append(101, 0, 704);
      writer.append(102, 1, 640);
      writer.force();

      assertRange(writer.locate(100), 0, 512, 608);
      assertRange(writer.locate(101), 0, 608, 704);
      assertRange(writer.locate(102), 1, 512, 640);
      assertEquals(160, Files.size(index));
    }

    try (StateArchiveLaneIndexV5 reader = StateArchiveLaneIndexV5.openCommitted(index, 3)) {
      assertEquals(4, reader.getLaneId());
      assertEquals(100, reader.getFirstBlockNumber());
      assertEquals(3, reader.getFrameCount());
      assertRange(reader.locate(102), 1, 512, 640);
      assertThrows(IllegalStateException.class, () -> reader.append(103, 1, 736));
    }
  }

  @Test
  public void rejectsOrdinalBoundaryAndCommittedLengthDrift() throws Exception {
    Path index = temporaryFolder.newFolder("drift").toPath().resolve("blocks.idx");
    try (StateArchiveLaneIndexV5 writer = StateArchiveLaneIndexV5.create(index, 0, 7)) {
      assertThrows(IllegalArgumentException.class, () -> writer.append(8, 0, 608));
      writer.append(7, 0, 608);
      assertThrows(IllegalArgumentException.class, () -> writer.append(8, 0, 608));
      assertThrows(IllegalArgumentException.class,
          () -> writer.append(8, 0, 608L + StateArchiveGethFormatV5.MAX_FRAME_BYTES + 1));
      assertThrows(IllegalArgumentException.class, () -> writer.append(8, 2, 640));
      assertThrows(IllegalArgumentException.class, () -> writer.append(8, 1, 512));
      assertThrows(IllegalArgumentException.class,
          () -> writer.append(8, 1, 512L + StateArchiveGethFormatV5.MAX_FRAME_BYTES + 1));
      writer.append(8, 1, 608);
    }

    assertThrows(IOException.class, () -> StateArchiveLaneIndexV5.openCommitted(index, 1));
    try (FileChannel channel = FileChannel.open(index, StandardOpenOption.WRITE)) {
      channel.truncate(StateArchiveLaneIndexV5.expectedLength(1));
    }
    try (StateArchiveLaneIndexV5 reader = StateArchiveLaneIndexV5.openCommitted(index, 1)) {
      assertRange(reader.locate(7), 0, 512, 608);
    }
    assertThrows(IOException.class, () -> StateArchiveLaneIndexV5.openCommitted(index, 2));
  }

  @Test
  public void keepsEmptyOrdinalU32AndLongIndexPositionsExact() throws Exception {
    Path index = temporaryFolder.newFolder("boundaries").toPath().resolve("blocks.idx");
    try (StateArchiveLaneIndexV5 empty = StateArchiveLaneIndexV5.create(index, 22, 9_000)) {
      empty.force();
    }
    try (StateArchiveLaneIndexV5 reopened = StateArchiveLaneIndexV5.openCommitted(index, 0)) {
      assertEquals(0, reopened.getFrameCount());
      assertThrows(IllegalArgumentException.class, () -> reopened.locate(9_000));
    }

    byte[] maximum = StateArchiveLaneIndexV5.encodeBoundary(0xffffffffL, 0xffffffffL);
    Boundary decoded = StateArchiveLaneIndexV5.decodeBoundary(maximum);
    assertEquals(0xffffffffL, decoded.getFileId());
    assertEquals(0xffffffffL, decoded.getEndOffset());
    assertArrayEquals(ByteBuffer.allocate(8).putInt(-1).putInt(-1).array(), maximum);

    long ordinal = (Integer.MAX_VALUE / 8L) + 100;
    long position = StateArchiveLaneIndexV5.boundaryPosition(ordinal);
    assertEquals(128L + ordinal * 8L, position);
    assertTrue(position > Integer.MAX_VALUE);
  }

  @Test
  public void rejectsHeaderCorruptionAndWrongFormatIdentity() throws Exception {
    Path index = temporaryFolder.newFolder("header").toPath().resolve("blocks.idx");
    try (StateArchiveLaneIndexV5 writer = StateArchiveLaneIndexV5.create(index, 5, 1)) {
      writer.force();
    }
    try (FileChannel channel = FileChannel.open(index, StandardOpenOption.WRITE)) {
      ByteBuffer one = ByteBuffer.wrap(new byte[]{0});
      channel.write(one, 0);
    }
    assertThrows(IOException.class, () -> StateArchiveLaneIndexV5.openCommitted(index, 0));
  }

  private static void assertRange(FrameRange range, long fileId, long start, long end) {
    assertEquals(fileId, range.getFileId());
    assertEquals(start, range.getStartOffset());
    assertEquals(end, range.getEndOffset());
  }

  private static byte[] fromHex(String value) {
    byte[] decoded = new byte[value.length() / 2];
    for (int index = 0; index < decoded.length; index++) {
      int offset = index * 2;
      decoded[index] = (byte) Integer.parseInt(value.substring(offset, offset + 2), 16);
    }
    return decoded;
  }

  private static String toHex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte element : value) {
      encoded.append(String.format("%02x", element));
    }
    return encoded.toString();
  }
}
