package org.tron.plugins;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.tron.common.utils.BlockFile;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@CommandLine.Command(name = "export",
    mixinStandardHelpOptions = true,
    description = "Export an inclusive range of consecutive blocks from a stopped node.")
public class DbBlockExport implements Callable<Integer> {

  private static final String BLOCK_DB_NAME = "block";
  private static final String BLOCK_INDEX_DB_NAME = "block-index";

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"-d", "--database-directory"}, required = true,
      description = "Node database directory containing block and block-index.")
  private Path databaseDirectory;

  @CommandLine.Option(names = "--start", required = true,
      description = "First block height, inclusive.")
  private long start;

  @CommandLine.Option(names = "--end", required = true,
      description = "Last block height, inclusive.")
  private long end;

  @CommandLine.Option(names = {"-o", "--output"}, required = true,
      description = "Destination block file.")
  private Path output;

  @CommandLine.Option(names = "--overwrite",
      description = "Replace an existing destination file.")
  private boolean overwrite;

  @Override
  public Integer call() throws Exception {
    Path database = databaseDirectory.toAbsolutePath().normalize();
    requireExistingDatabase(database, BLOCK_DB_NAME);
    requireExistingDatabase(database, BLOCK_INDEX_DB_NAME);

    String databasePath = database.toString();
    DBInterface blockIndex = null;
    DBInterface block = null;
    try {
      blockIndex = DbTool.getDB(databasePath, BLOCK_INDEX_DB_NAME);
      block = DbTool.getDB(databasePath, BLOCK_DB_NAME);
      DBInterface sourceBlockIndex = blockIndex;
      DBInterface sourceBlock = block;
      BlockFile.Header header = BlockFile.write(output, start, end, overwrite, height -> {
        byte[] blockId = sourceBlockIndex.get(ByteArray.fromLong(height));
        if (blockId == null) {
          throw new IOException("Block index is missing height " + height);
        }
        byte[] blockData = sourceBlock.get(blockId);
        if (blockData == null) {
          throw new IOException("Block data is missing height " + height);
        }
        return new BlockFile.Record(height, blockId, blockData);
      });
      spec.commandLine().getOut().printf(
          "Exported %d blocks [%d, %d] to %s%n",
          header.getCount(), header.getStart(), header.getEnd(),
          output.toAbsolutePath().normalize());
      return 0;
    } finally {
      close(databasePath, BLOCK_DB_NAME, block);
      close(databasePath, BLOCK_INDEX_DB_NAME, blockIndex);
    }
  }

  private void requireExistingDatabase(Path database, String name) {
    Path path = database.resolve(name);
    if (!Files.isDirectory(path)) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "Database does not exist: " + path);
    }
  }

  private static void close(String database, String name, DBInterface db) throws IOException {
    if (db != null) {
      DbTool.closeDB(database, name);
    }
  }
}
