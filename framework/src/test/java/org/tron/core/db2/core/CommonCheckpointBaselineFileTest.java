package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockSnapshotMeta;

public class CommonCheckpointBaselineFileTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void publishesOnceAndRejectsDriftOrCorruption() throws Exception {
    Path root = temporaryFolder.newFolder("baseline").toPath();
    CommonCheckpointBaselineFile file = new CommonCheckpointBaselineFile(root);
    CommonCheckpointBaseline expected = baseline(7);

    CommonCheckpointBaseline first = file.openOrCreate(expected);
    CommonCheckpointBaseline second = file.openOrCreate(expected);
    assertEquals(first.getHead(), second.getHead());
    assertArrayEquals(first.getStateRoot(), second.getStateRoot());
    assertThrows(IOException.class, () -> file.openOrCreate(baseline(8)));

    Path authority = root.resolve(CommonCheckpointBaselineFile.FILE_NAME);
    byte[] corrupt = Files.readAllBytes(authority);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(authority, corrupt);
    assertThrows(IOException.class, file::load);
  }

  private static CommonCheckpointBaseline baseline(int seed) {
    return new CommonCheckpointBaseline(CommonCheckpointFormat.identity(),
        BlockSnapshotMeta.forBlock(seed, hash(seed), hash(seed - 1), seed * 3_000L),
        hash(seed + 20));
  }

  private static byte[] hash(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }
}
