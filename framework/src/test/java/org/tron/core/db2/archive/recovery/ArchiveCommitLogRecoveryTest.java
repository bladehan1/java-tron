package org.tron.core.db2.archive.recovery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchivePersistenceException;

public class ArchiveCommitLogRecoveryTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void validatesMarkerBeforeShrinkingAndPreservesTheCommittedPrefix() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path log = writeLog(root, bytes(32));
    byte[] before = Files.readAllBytes(log);

    ArchiveCommitLogRecovery.shrink(root, 16, 8, Arrays.copyOfRange(before, 8, 16));

    assertArrayEquals(Arrays.copyOf(before, 16), Files.readAllBytes(log));
  }

  @Test
  public void mismatchedMarkerLeavesTheWholeLogUntouched() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path log = writeLog(root, bytes(32));
    byte[] before = Files.readAllBytes(log);
    byte[] marker = Arrays.copyOfRange(before, 8, 16);
    marker[0] ^= 1;

    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveCommitLogRecovery.shrink(root, 16, 8, marker));

    assertArrayEquals(before, Files.readAllBytes(log));
  }

  @Test
  public void shortLogIsRejectedWithoutExtendingOrRewritingIt() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    byte[] before = bytes(8);
    Path log = writeLog(root, before);

    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveCommitLogRecovery.shrink(root, 16, 8, bytes(8)));

    assertArrayEquals(before, Files.readAllBytes(log));
  }

  @Test
  public void missingLogDoesNotCreateTheCommitDirectory() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();

    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveCommitLogRecovery.shrink(root, 16, 8, bytes(8)));

    assertFalse(Files.exists(root.resolve("commits")));
  }

  private static Path writeLog(Path root, byte[] bytes) throws Exception {
    Path log = root.resolve("commits/commit.log");
    Files.createDirectories(log.getParent());
    return Files.write(log, bytes);
  }

  private static byte[] bytes(int length) {
    byte[] result = new byte[length];
    for (int i = 0; i < result.length; i++) {
      result[i] = (byte) i;
    }
    return result;
  }
}
