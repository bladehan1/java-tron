package org.tron.core.db2.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SnapshotManagerCommonCheckpointRecoveryTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void legacyRecoveryStopsAfterCommonAuthorityIsDurable() throws Exception {
    Path directory = temporaryFolder.newFolder("common-checkpoint").toPath();
    assertFalse(SnapshotManager.hasDurableCommonCheckpointAuthority(true, directory));

    Path redo = directory.resolve(CommonCheckpointFile.FILE_NAME);
    Files.write(redo, new byte[] {1});
    assertTrue(SnapshotManager.hasDurableCommonCheckpointAuthority(true, directory));
    assertFalse(SnapshotManager.hasDurableCommonCheckpointAuthority(false, directory));

    Files.delete(redo);
    Files.write(directory.resolve(ChainbaseCheckpointMaterializer.CURRENT_FILE), new byte[] {1});
    assertTrue(SnapshotManager.hasDurableCommonCheckpointAuthority(true, directory));
  }
}
