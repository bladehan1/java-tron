package org.tron.core.db2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db2.RevokingDbWithCacheNewValueTest.TestRevokingTronStore;
import org.tron.core.db2.SnapshotRootTest.ProtoCapsuleTest;
import org.tron.core.db2.core.SnapshotManager;

/**
 * The active session head carries a block-scoped read-through cache (SnapshotImpl). These
 * tests pin the put/delete/negative-cache shadowing rules and the concurrent-read path of the
 * parallel signature pool through the public session API.
 */
@Slf4j
public class SnapshotBlockReadCacheTest extends BaseMethodTest {

  private SnapshotManager revokingDatabase;
  private TestRevokingTronStore tronDatabase;

  @Override
  protected void beforeDestroy() {
    if (tronDatabase != null) {
      tronDatabase.close();
    }
  }

  private void init(String name) {
    revokingDatabase = context.getBean(SnapshotManager.class);
    revokingDatabase.enable();
    tronDatabase = new TestRevokingTronStore(name);
    revokingDatabase.add(tronDatabase.getRevokingDB());
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }
  }

  @Test
  public synchronized void testPutShadowsCachedRead() {
    init("SnapshotBlockReadCacheTest-put");
    byte[] key = "key".getBytes();
    ProtoCapsuleTest v1 = new ProtoCapsuleTest("v1".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      tronDatabase.put(key, v1);
      session.commit();
    }

    ProtoCapsuleTest v2 = new ProtoCapsuleTest("v2".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      // descends through the committed layer and fills the block read cache
      Assert.assertEquals(v1, tronDatabase.get(key));
      Assert.assertEquals(v1, tronDatabase.get(key));
      tronDatabase.put(key, v2);
      // the session head's own HashDB is probed before the cache
      Assert.assertEquals(v2, tronDatabase.get(key));
      session.commit();
    }

    try (ISession session = revokingDatabase.buildSession()) {
      Assert.assertEquals(v2, tronDatabase.get(key));
      session.commit();
    }
  }

  @Test
  public synchronized void testDeleteShadowsCacheAndCachedMiss() {
    init("SnapshotBlockReadCacheTest-delete");
    byte[] key = "key".getBytes();
    byte[] missing = "missing".getBytes();
    ProtoCapsuleTest v1 = new ProtoCapsuleTest("v1".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      tronDatabase.put(key, v1);
      session.commit();
    }

    ProtoCapsuleTest v2 = new ProtoCapsuleTest("v2".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      // negative cache entry for an absent key
      Assert.assertNull(tronDatabase.get(missing));
      // cached value for a key that only lives below the session head
      Assert.assertEquals(v1, tronDatabase.get(key));
      tronDatabase.delete(key);
      // the DELETE value in the head's HashDB shadows both the cache and the layers below
      Assert.assertNull(tronDatabase.get(key));
      tronDatabase.put(missing, v2);
      // a put shadows the cached miss
      Assert.assertEquals(v2, tronDatabase.get(missing));
      session.commit();
    }

    try (ISession session = revokingDatabase.buildSession()) {
      Assert.assertNull(tronDatabase.get(key));
      Assert.assertEquals(v2, tronDatabase.get(missing));
      session.commit();
    }
  }

  @Test
  public synchronized void testNestedSessionMergeAndRevoke() {
    init("SnapshotBlockReadCacheTest-merge");
    byte[] key = "key".getBytes();
    ProtoCapsuleTest v1 = new ProtoCapsuleTest("v1".getBytes());
    ProtoCapsuleTest v2 = new ProtoCapsuleTest("v2".getBytes());
    ProtoCapsuleTest v3 = new ProtoCapsuleTest("v3".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      tronDatabase.put(key, v1);
      session.commit();
    }

    ISession outer = revokingDatabase.buildSession();
    try {
      // fills the read cache of the outer session head
      Assert.assertEquals(v1, tronDatabase.get(key));
      try (ISession inner = revokingDatabase.buildSession()) {
        // advancing covered the outer head and dropped its cache
        tronDatabase.put(key, v2);
        Assert.assertEquals(v2, tronDatabase.get(key));
        inner.merge();
      }
      // the merged write is visible on the outer head again
      Assert.assertEquals(v2, tronDatabase.get(key));
      try (ISession inner = revokingDatabase.buildSession()) {
        tronDatabase.put(key, v3);
        Assert.assertEquals(v3, tronDatabase.get(key));
        inner.revoke();
      }
      Assert.assertEquals(v2, tronDatabase.get(key));
      outer.commit();
    } finally {
      outer.close();
    }

    try (ISession session = revokingDatabase.buildSession()) {
      Assert.assertEquals(v2, tronDatabase.get(key));
      session.commit();
    }
  }

  @Test
  public synchronized void testConcurrentReadsOnSessionHead() throws Exception {
    init("SnapshotBlockReadCacheTest-concurrent");
    byte[] key = "key".getBytes();
    byte[] missing = "missing".getBytes();
    ProtoCapsuleTest v1 = new ProtoCapsuleTest("v1".getBytes());
    try (ISession session = revokingDatabase.buildSession()) {
      tronDatabase.put(key, v1);
      session.commit();
    }

    // mirrors preValidateTransactionSign: pool threads read the active session head while
    // the block thread waits; reads of the same keys race to fill the block read cache
    try (ISession session = revokingDatabase.buildSession()) {
      ExecutorService pool = Executors.newFixedThreadPool(4);
      try {
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
          futures.add(pool.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() {
              for (int j = 0; j < 200; j++) {
                if (!v1.equals(tronDatabase.get(key))) {
                  return false;
                }
                if (tronDatabase.get(missing) != null) {
                  return false;
                }
              }
              return true;
            }
          }));
        }
        for (Future<Boolean> future : futures) {
          Assert.assertTrue(future.get());
        }
      } finally {
        pool.shutdownNow();
      }
      session.commit();
    }
  }
}
