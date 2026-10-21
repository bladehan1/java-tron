package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.tron.core.db2.archive.ServingIndexStoreTest.hash;
import static org.tron.core.db2.archive.ServingIndexStoreTest.key;

import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.history.codec.BlockHistoryCodec;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/**
 * T5 / S-INV-11 (ch04 §3.5.2): a block whose re-encoded, compressed bytes exceed the old 64 MiB
 * record cap plans and commits, because the step digest only combines frame digests.
 *
 * <p>Holds a 70 MiB block in memory, so it runs in the {@code archiveLargeBlock} task with its
 * own heap instead of the shared 512 MiB test JVM, where it ran out of memory depending on order.
 */
public class ServingIndexStoreLargeBlockTest {

  private static final long B0 = 101;
  private static final ServingIndexStore.WriteBudget TINY =
      new ServingIndexStore.WriteBudget(64, 64 * 1024);

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 120_000)
  public void blockBeyondTheOldEncodingCapIsIndexable() throws Exception {
    byte[] incompressible = new byte[70 * 1024 * 1024];
    new java.util.Random(7).nextBytes(incompressible);
    BlockReverseDiff huge = new BlockReverseDiff(BlockSnapshotMeta.forBlock(B0, hash(B0),
        hash(B0 - 1), B0 * 3000L), Collections.singletonList(new BlockReverseDiff.DbGroup(
            "code", Collections.singletonList(new BlockReverseDiff.Entry(key(1),
                OldValue.present(incompressible))))));
    assertThrows(IllegalArgumentException.class, () -> new BlockHistoryCodec().encode(huge));
    Path directory = temporaryFolder.newFolder().toPath().resolve("single-v1");
    try (ServingIndexStore store = ServingIndexStore.open(directory, Engine.ROCKSDB, B0,
        hash(B0 - 1), false, 1)) {
      ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.planCommittedDiffs(
          store.indexedThrough(), store.headHash(), Collections.singletonList(huge),
          Collections.singletonList(ServingIndexIncrementalPlan.stepDigest(huge.getMeta(),
              Collections.singletonList(hash(9)))));
      store.commitSolo(plan, TINY, false, ServingIndexStore.ChunkHook.NONE);
      assertEquals(B0, store.indexedThrough());
    }
  }
}
