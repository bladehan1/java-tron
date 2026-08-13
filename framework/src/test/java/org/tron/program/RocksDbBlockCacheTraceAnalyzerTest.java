package org.tron.program;

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RocksDbBlockCacheTraceAnalyzerTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void aggregatesLevelsAndGetPaths() throws Exception {
    Path input = temporaryFolder.newFolder("input").toPath();
    Path output = temporaryFolder.newFolder("output").toPath();
    Files.write(input.resolve("account.csv"), ("timestamp_us,get_id,level,sst_file,caller,"
        + "block_type,cache_hit,no_insert,key_exists,block_size,cf_id,cf_name\n"
        + "1,42,1,10,1,9,0,0,0,4096,0,default\n"
        + "2,42,3,11,1,9,1,0,1,4096,0,default\n"
        + "3,0,2,12,10,9,0,0,0,8192,0,default\n").getBytes(StandardCharsets.UTF_8));
    RocksDbBlockCacheTraceAnalyzer.analyze(input, output);
    String gets = new String(Files.readAllBytes(output.resolve("get-path.csv")),
        StandardCharsets.UTF_8);
    assertTrue(gets.contains("account,1,2,1,1,1,0"));
    String blocks = new String(Files.readAllBytes(output.resolve("block-access.csv")),
        StandardCharsets.UTF_8);
    assertTrue(blocks.contains("account,1,get,data,miss,1,4096"));
    assertTrue(blocks.contains("account,2,compaction,data,miss,1,8192"));
  }
}
