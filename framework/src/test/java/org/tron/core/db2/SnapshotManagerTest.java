package org.tron.core.db2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db2.RevokingDbWithCacheNewValueTest.TestRevokingTronStore;
import org.tron.core.db2.SnapshotRootTest.ProtoCapsuleTest;
import org.tron.core.db2.archive.OldValueCollector;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.CommonCheckpointRuntimeAttachment;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.stateroot.PathStateRuntimeAttachment;
import org.tron.core.db2.stateroot.PathStateTransitionCollector;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.exception.TronError;

@Slf4j
public class SnapshotManagerTest extends BaseMethodTest {

  private SnapshotManager revokingDatabase;
  private TestRevokingTronStore tronDatabase;

  @Override
  protected void afterInit() {
    revokingDatabase = context.getBean(SnapshotManager.class);
    revokingDatabase.enable();
    tronDatabase = new TestRevokingTronStore("testSnapshotManager-test");
    revokingDatabase.add(tronDatabase.getRevokingDB());
  }

  @Override
  protected void beforeDestroy() {
    tronDatabase.close();
  }

  @Test
  public synchronized void testRefresh()
      throws BadItemException, ItemNotFoundException {
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }

    revokingDatabase.setMaxFlushCount(0);
    revokingDatabase.setUnChecked(false);
    revokingDatabase.setMaxSize(5);
    List<Chainbase> dbList = revokingDatabase.getDbs();
    Map<String, Chainbase> dbMap = dbList.stream()
        .map(db -> Maps.immutableEntry(db.getDbName(), db))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ProtoCapsuleTest protoCapsule = new ProtoCapsuleTest("refresh".getBytes());
    for (int i = 1; i < 11; i++) {
      ProtoCapsuleTest testProtoCapsule = new ProtoCapsuleTest(("refresh" + i).getBytes());
      try (ISession tmpSession = revokingDatabase.buildSession()) {
        tronDatabase.put(protoCapsule.getData(), testProtoCapsule);
        BlockCapsule blockCapsule = new BlockCapsule(i, Sha256Hash.ZERO_HASH,
            System.currentTimeMillis(), ByteString.EMPTY);
        dbMap.get("block").put(Longs.toByteArray(i), blockCapsule.getData());
        tmpSession.commit();
      }
    }

    revokingDatabase.flush();
    Assert.assertEquals(new ProtoCapsuleTest("refresh10".getBytes()),
        tronDatabase.get(protoCapsule.getData()));
  }

  @Test
  public synchronized void testClose() {
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }

    revokingDatabase.setMaxFlushCount(0);
    revokingDatabase.setUnChecked(false);
    revokingDatabase.setMaxSize(5);
    ProtoCapsuleTest protoCapsule = new ProtoCapsuleTest("close".getBytes());
    for (int i = 1; i < 11; i++) {
      ProtoCapsuleTest testProtoCapsule = new ProtoCapsuleTest(("close" + i).getBytes());
      try (ISession tmpSession = revokingDatabase.buildSession()) {
        tronDatabase.put(protoCapsule.getData(), testProtoCapsule);
      }
    }
    Assert.assertEquals(null,
        tronDatabase.get(protoCapsule.getData()));

  }

  @Test
  public synchronized void testFlushPendingBelowBatchThreshold() {
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }

    revokingDatabase.setUnChecked(false);
    revokingDatabase.setMaxSize(5);
    revokingDatabase.setMaxFlushCount(20);
    ProtoCapsuleTest key = new ProtoCapsuleTest("flush-pending".getBytes());
    for (int i = 1; i <= 12; i++) {
      try (ISession session = revokingDatabase.buildSession()) {
        tronDatabase.put(key.getData(), new ProtoCapsuleTest(("value" + i).getBytes()));
        session.commit();
      }
    }

    Assert.assertFalse(revokingDatabase.shouldBeRefreshed());
    revokingDatabase.setMaxFlushCount(1);
    Assert.assertTrue(revokingDatabase.shouldBeRefreshed());
    revokingDatabase.setMaxFlushCount(20);

    revokingDatabase.flushPending();

    revokingDatabase.setMaxFlushCount(1);
    Assert.assertFalse(revokingDatabase.shouldBeRefreshed());
  }

  @Test
  public void testCheckError() {
    SnapshotManager manager = spy(new SnapshotManager(""));
    when(manager.getCheckpointList()).thenReturn(Arrays.asList("check1", "check2"));
    TronError thrown = Assert.assertThrows(TronError.class, manager::check);
    Assert.assertEquals(TronError.ErrCode.CHECKPOINT_VERSION, thrown.getErrCode());
  }

  @Test
  public void testFlushError() {
    SnapshotManager manager = spy(new SnapshotManager(""));
    manager.setUnChecked(false);
    when(manager.getCheckpointList()).thenReturn(Arrays.asList("check1", "check2"));
    when(manager.shouldBeRefreshed()).thenReturn(true);
    TronError thrown = Assert.assertThrows(TronError.class, manager::flush);
    Assert.assertEquals(TronError.ErrCode.DB_FLUSH, thrown.getErrCode());
  }

  @Test
  public void commonCheckpointFlushIsExclusiveAndResetsPrefix() throws Exception {
    SnapshotManager manager = new SnapshotManager("");
    CommonCheckpointRuntimeAttachment common = mock(CommonCheckpointRuntimeAttachment.class);
    when(common.isEnabled()).thenReturn(true);
    manager.installCommonCheckpointArchiveCollector(mock(OldValueCollector.class));
    PathStateRuntimeAttachment path = PathStateRuntimeAttachment.commonCheckpoint(
        mock(PathStateTransitionCollector.class), transition -> { }, null, null);
    manager.attachPathStateRuntime(path);
    manager.attachCommonCheckpointRuntime(common);
    manager.setUnChecked(false);
    manager.setMaxFlushCount(1);
    Field count = SnapshotManager.class.getDeclaredField("flushCount");
    count.setAccessible(true);
    count.setInt(manager, 1);

    manager.flush();

    verify(common).checkpointAndRebase(1);
    Assert.assertFalse(manager.shouldBeRefreshed());
    Assert.assertSame(common, manager.detachCommonCheckpointRuntime(common));
    Assert.assertSame(path, manager.detachPathStateRuntime(path));
  }

  @Test
  public void archiveStateBarrierBlocksSessionAdvanceAndFlush() throws Exception {
    SnapshotManager manager = new SnapshotManager("");
    manager.enable();
    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ISession acquired = null;
    try {
      Future<?> barrier = executor.submit(() -> {
        manager.withArchiveStateBarrier(() -> {
          entered.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IOException("Timed out waiting to release archive state barrier");
            }
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted inside archive state barrier", interrupted);
          }
        });
        return null;
      });
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));

      Future<ISession> session = executor.submit(() -> manager.buildSession());
      Future<?> flush = executor.submit(manager::flush);
      Assert.assertThrows(TimeoutException.class,
          () -> session.get(100, TimeUnit.MILLISECONDS));
      Assert.assertThrows(TimeoutException.class,
          () -> flush.get(100, TimeUnit.MILLISECONDS));

      release.countDown();
      barrier.get(5, TimeUnit.SECONDS);
      acquired = session.get(5, TimeUnit.SECONDS);
      flush.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      if (acquired != null) {
        acquired.close();
      }
      executor.shutdownNow();
    }
  }

  @Test
  public void archiveStateBarrierReleasesMonitorAfterFailure() throws Exception {
    SnapshotManager manager = new SnapshotManager("");
    manager.enable();

    IOException failure = Assert.assertThrows(IOException.class,
        () -> manager.withArchiveStateBarrier(() -> {
          throw new IOException("injected barrier failure");
        }));
    Assert.assertEquals("injected barrier failure", failure.getMessage());
    try (ISession ignored = manager.buildSession()) {
      Assert.assertEquals(1, manager.size());
    }
    Assert.assertEquals(0, manager.size());
  }

  @Test
  public void archiveStateBarrierBlocksNestedSessionMerge() throws Exception {
    SnapshotManager manager = new SnapshotManager("");
    manager.enable();
    ISession parent = manager.buildSession();
    ISession child = manager.buildSession();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      Future<?> barrier = executor.submit(() -> {
        manager.withArchiveStateBarrier(() -> {
          entered.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IOException("Timed out waiting to release archive state barrier");
            }
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted inside archive state barrier", interrupted);
          }
        });
        return null;
      });
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      Future<?> merge = executor.submit(child::merge);
      Assert.assertThrows(TimeoutException.class,
          () -> merge.get(100, TimeUnit.MILLISECONDS));

      release.countDown();
      barrier.get(5, TimeUnit.SECONDS);
      merge.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(1, manager.size());
    } finally {
      release.countDown();
      child.close();
      parent.close();
      executor.shutdownNow();
    }
    Assert.assertEquals(0, manager.size());
  }
}
