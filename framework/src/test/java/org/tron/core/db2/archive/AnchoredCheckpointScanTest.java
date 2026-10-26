package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.tron.core.db2.archive.SharedCheckpointDatabaseTest.payload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.checkpoint.storage.SharedCheckpointDatabase;
import org.tron.core.db2.archive.storage.index.ArchiveIndexStorage;
import org.tron.core.db2.checkpoint.common.CommonCheckpointPayloadCodec;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class AnchoredCheckpointScanTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void repeatedPruneAndReopenKeepTheSameLiveRedo() throws Exception {
    for (Engine engine : engines()) {
      Path root = temporary.newFolder().toPath();
      try (SharedCheckpointDatabase db = new SharedCheckpointDatabase(root, engine)) {
        assertEquals(0, db.retainedForDurableBase().size());
        for (int block = 1; block <= 24; block++) {
          db.publish(payload(block));
          db.applied();
          assertEquals(db.retained().size(), db.retainedForDurableBase().size());
          if (block % 4 == 0) {
            db.advanceDurableBase();
            assertEquals(1, db.retainedForDurableBase().size());
            assertEquals(block, last(db));
          }
        }
      }
      try (SharedCheckpointDatabase reopened = new SharedCheckpointDatabase(root, engine)) {
        assertEquals(24, last(reopened));
        reopened.publish(payload(25));
        reopened.applied();
        assertEquals(2, reopened.retainedForDurableBase().size());
      }
    }
  }

  @Test
  public void startPointerCannotSkipRequiredRedoDuringRuntime() throws Exception {
    for (Engine engine : engines()) {
      Path root = temporary.newFolder().toPath();
      try (SharedCheckpointDatabase db = new SharedCheckpointDatabase(root, engine);
          ArchiveIndexStorage.Writer tamper = ArchiveIndexStorage.openWriter(root,
              engine)) {
        db.publish(payload(1));
        db.applied();
        db.publish(payload(2));
        db.applied();
        tamper.write(Collections.singletonList(ArchiveIndexStorage.put(
            key("first-retained/v1"), generation(2))), true);
        assertThrows(IOException.class, db::retainedForDurableBase);
        assertThrows(IOException.class, db::advanceDurableBase);
        assertArrayEquals(generation(2), tamper.get(key("first-retained/v1")));
        // Failed validation must leave the required first redo body intact.
        assertEquals(1, new CommonCheckpointPayloadCodec().decode(tamper.get(generation(1)))
            .getBlocks().get(0).getMeta().getBlockNumber());
      }
    }
  }

  @Test
  public void missingAnchorFailsBeforePruningAnotherGeneration() throws Exception {
    for (Engine engine : engines()) {
      Path root = temporary.newFolder().toPath();
      try (SharedCheckpointDatabase db = new SharedCheckpointDatabase(root, engine);
          ArchiveIndexStorage.Writer tamper = ArchiveIndexStorage.openWriter(root,
              engine)) {
        db.publish(payload(1));
        db.applied();
        db.publish(payload(2));
        db.applied();
        tamper.write(Collections.singletonList(ArchiveIndexStorage.delete(generation(1))),
            true);
        assertThrows(IOException.class, db::retainedForDurableBase);
        assertThrows(IOException.class, db::advanceDurableBase);
        assertArrayEquals(generation(1), tamper.get(key("first-retained/v1")));
        assertEquals(2, new CommonCheckpointPayloadCodec().decode(tamper.get(generation(2)))
            .getBlocks().get(0).getMeta().getBlockNumber());
      }
    }
  }

  @Test
  public void startupStillRejectsUnexpectedLiveRecordsBeforeStart() throws Exception {
    for (Engine engine : engines()) {
      Path root = temporary.newFolder().toPath();
      try (SharedCheckpointDatabase db = new SharedCheckpointDatabase(root, engine)) {
        db.publish(payload(1));
        db.applied();
        db.publish(payload(2));
        db.applied();
        db.advanceDurableBase();
      }
      try (ArchiveIndexStorage.Writer writer = ArchiveIndexStorage.openWriter(root,
          engine)) {
        writer.write(Collections.singletonList(ArchiveIndexStorage.put(generation(1),
            new CommonCheckpointPayloadCodec().encode(payload(1)))), true);
      }
      assertThrows(IOException.class, () -> new SharedCheckpointDatabase(root, engine));
    }
  }

  @Test
  public void runtimeStillRejectsCorruptBodyAndForgedGenerationIdentity() throws Exception {
    for (Engine engine : engines()) {
      for (boolean corruptBody : new boolean[]{true, false}) {
        Path root = temporary.newFolder().toPath();
        try (SharedCheckpointDatabase db = new SharedCheckpointDatabase(root, engine);
            ArchiveIndexStorage.Writer tamper = ArchiveIndexStorage.openWriter(root,
                engine)) {
          db.publish(payload(1));
          db.applied();
          byte[] body = new CommonCheckpointPayloadCodec().encode(payload(corruptBody ? 1 : 2));
          if (corruptBody) {
            body[body.length - 1] ^= 1;
          }
          tamper.write(Collections.singletonList(ArchiveIndexStorage.put(generation(1),
              body)), true);
          assertThrows(IOException.class, db::retainedForDurableBase);
        }
      }
    }
  }

  /** Allows native backends to be validated in separate JVMs. */
  protected Engine[] engines() {
    return Engine.values();
  }

  private static long last(SharedCheckpointDatabase db) throws IOException {
    return db.retainedForDurableBase().get(db.retainedForDurableBase().size() - 1)
        .getBlocks().get(0).getMeta().getBlockNumber();
  }

  private static byte[] generation(long block) {
    byte[] prefix = key("generation/v1/");
    return ByteBuffer.allocate(prefix.length + Long.BYTES).put(prefix).putLong(block).array();
  }

  private static byte[] key(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }
}
