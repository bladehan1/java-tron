package org.tron.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.protobuf.ByteString;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.utils.BlockFile;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DbTool;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.BlockHeader;
import picocli.CommandLine;

public class DbBlockExportTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    DbTool.close();
  }

  @Test
  public void shouldExportBlocksFromDatabase() throws Exception {
    Path database = temporaryFolder.newFolder("database").toPath();
    DBInterface blockIndex = DbTool.getDB(database.toString(), "block-index",
        DbTool.DbType.RocksDB);
    DBInterface blockDb = DbTool.getDB(database.toString(), "block", DbTool.DbType.RocksDB);
    byte[] previousId = new byte[32];
    for (long height = 20; height <= 21; height++) {
      byte[] id = blockId(height);
      Block block = block(height, previousId);
      blockIndex.put(ByteArray.fromLong(height), id);
      blockDb.put(id, block.toByteArray());
      previousId = id;
    }
    DbTool.closeDB(database.toString(), "block-index");
    DbTool.closeDB(database.toString(), "block");

    Path output = temporaryFolder.getRoot().toPath().resolve("blocks.dat");
    CommandLine cli = new CommandLine(new Toolkit());
    assertEquals(0, cli.execute("db", "block", "export",
        "-d", database.toString(), "--start", "20", "--end", "21",
        "-o", output.toString()));

    try (BlockFile.Reader reader = BlockFile.open(output)) {
      assertEquals(2, reader.getHeader().getCount());
      assertEquals(20, reader.next().getHeight());
      assertEquals(21, reader.next().getHeight());
      assertFalse(reader.hasNext());
    }
  }

  private static Block block(long height, byte[] parent) {
    return Block.newBuilder()
        .setBlockHeader(BlockHeader.newBuilder()
            .setRawData(BlockHeader.raw.newBuilder()
                .setNumber(height)
                .setTimestamp(height * 3000)
                .setParentHash(ByteString.copyFrom(parent))))
        .build();
  }

  private static byte[] blockId(long height) {
    byte[] value = new byte[32];
    byte[] heightBytes = ByteArray.fromLong(height);
    System.arraycopy(heightBytes, 0, value, 0, heightBytes.length);
    value[31] = (byte) height;
    return value;
  }
}
