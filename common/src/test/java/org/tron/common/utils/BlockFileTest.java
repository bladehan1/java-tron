package org.tron.common.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.BlockHeader;

public class BlockFileTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldRoundTripConsecutiveBlocks() throws Exception {
    Path output = temporaryFolder.getRoot().toPath().resolve("blocks.dat");
    BlockFile.Header written = BlockFile.write(output, 10, 12, false,
        height -> record(height));

    assertEquals(10, written.getStart());
    assertEquals(12, written.getEnd());
    assertEquals(3, written.getCount());
    try (BlockFile.Reader reader = BlockFile.open(output)) {
      assertEquals(3, reader.getHeader().getCount());
      for (long height = 10; height <= 12; height++) {
        BlockFile.Record record = reader.next();
        assertEquals(height, record.getHeight());
        assertArrayEquals(blockId(height), record.getBlockId());
        assertEquals(height, record.getBlock().getBlockHeader().getRawData().getNumber());
      }
      assertFalse(reader.hasNext());
    }
  }

  @Test
  public void shouldRejectCorruptedBlockData() throws Exception {
    Path output = temporaryFolder.getRoot().toPath().resolve("corrupted.dat");
    BlockFile.write(output, 10, 10, false, BlockFileTest::record);
    byte[] bytes = Files.readAllBytes(output);
    int firstBlockByte = 8 + Integer.BYTES + Long.BYTES * 3
        + Long.BYTES + 32 + Integer.BYTES;
    bytes[firstBlockByte] ^= 1;
    Files.write(output, bytes);

    try (BlockFile.Reader reader = BlockFile.open(output)) {
      assertThrows(IOException.class, reader::next);
    }
  }

  @Test
  public void shouldNotOverwriteByDefault() throws Exception {
    Path output = temporaryFolder.newFile("existing.dat").toPath();
    assertThrows(IOException.class,
        () -> BlockFile.write(output, 1, 1, false, BlockFileTest::record));
  }

  private static BlockFile.Record record(long height) {
    byte[] parent = height == 10 ? new byte[32] : blockId(height - 1);
    Block block = Block.newBuilder()
        .setBlockHeader(BlockHeader.newBuilder()
            .setRawData(BlockHeader.raw.newBuilder()
                .setNumber(height)
                .setTimestamp(height * 3000)
                .setParentHash(ByteString.copyFrom(parent))))
        .build();
    return new BlockFile.Record(height, blockId(height), block.toByteArray());
  }

  private static byte[] blockId(long height) {
    byte[] value = new byte[32];
    byte[] heightBytes = ByteArray.fromLong(height);
    System.arraycopy(heightBytes, 0, value, 0, heightBytes.length);
    value[31] = (byte) height;
    return value;
  }
}
