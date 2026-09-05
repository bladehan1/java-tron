package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveIndexDatabaseTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void supportsConfiguredEngineAndRejectsEngineDrift() throws Exception {
    for (Engine engine : Engine.values()) {
      Path root = temporaryFolder.newFolder(engine.name().toLowerCase()).toPath();
      Path database = root.resolve("keys");
      StateArchiveIndexEngineManifest.openOrCreate(root, engine);
      try (StateArchiveIndexDatabase.Writer writer =
          StateArchiveIndexDatabase.openWriter(database, engine)) {
        writer.write(Arrays.asList(
            StateArchiveIndexDatabase.put(new byte[]{1}, new byte[]{11}),
            StateArchiveIndexDatabase.put(new byte[]{3}, new byte[]{33})));
      }
      try (StateArchiveIndexDatabase.Reader reader =
          StateArchiveIndexDatabase.openReader(database, engine)) {
        assertArrayEquals(new byte[]{11}, reader.get(new byte[]{1}));
        StateArchiveIndexDatabase.KeyValue found = reader.seek(new byte[]{2});
        assertNotNull(found);
        assertArrayEquals(new byte[]{3}, found.getKey());
        assertArrayEquals(new byte[]{33}, found.getValue());
      }
      Engine other = engine == Engine.LEVELDB ? Engine.ROCKSDB : Engine.LEVELDB;
      assertThrows(IOException.class, () -> StateArchiveIndexEngineManifest.require(root, other));
    }
  }

  @Test
  public void rejectsExistingDatabaseWithoutEngineIdentity() throws Exception {
    Path root = temporaryFolder.newFolder("missing-manifest").toPath();
    Files.createDirectory(root.resolve("keys"));
    assertThrows(IOException.class,
        () -> StateArchiveIndexEngineManifest.openOrCreate(root, Engine.LEVELDB));
  }
}
