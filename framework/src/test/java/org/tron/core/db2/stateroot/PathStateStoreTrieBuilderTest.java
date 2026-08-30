package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateStoreTrieBuilderTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void diskSortsUnorderedRowsAndBoundsPendingValues() throws Exception {
    Path spool = temporaryFolder.newFolder("store-spool").toPath();
    PathMerkleTrie reference = new PathMerkleTrie(new MemoryNodeStore());
    Random random = new Random(71L);
    int count = PathStateStoreTrieBuilder.DEFAULT_WRITE_BATCH_ROWS * 3 + 17;

    try (PathStateStoreTrieBuilder builder = new PathStateStoreTrieBuilder(spool,
        Engine.ROCKSDB, (path, encoded) -> { })) {
      for (int index = count - 1; index >= 0; index--) {
        byte[] key = new byte[PathMerkleTrie.SECURE_KEY_LENGTH];
        ByteBuffer.wrap(key, key.length - Integer.BYTES, Integer.BYTES).putInt(index);
        byte[] value = new byte[64 + random.nextInt(128)];
        random.nextBytes(value);
        builder.put(key, value);
        reference.put(key, value);
        assertTrue(builder.getPendingRows() < PathStateStoreTrieBuilder.DEFAULT_WRITE_BATCH_ROWS);
      }

      assertArrayEquals(reference.rootHash(), builder.build());
      assertEquals(count, builder.getInputRows());
      assertEquals(count, builder.getSortedRows());
      assertEquals(0, builder.getPendingRows());
    }
  }

  private static final class MemoryNodeStore implements PathNodeStore {

    @Override
    public byte[] get(byte[] path) {
      return null;
    }

    @Override
    public void put(byte[] path, byte[] encodedNode) {
    }

    @Override
    public void delete(byte[] path) {
    }
  }
}
