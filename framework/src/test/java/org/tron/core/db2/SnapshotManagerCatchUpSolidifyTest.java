package org.tron.core.db2;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db2.RevokingDbWithCacheNewValueTest.TestRevokingTronStore;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRuntimeAttachment;
import org.tron.core.db2.core.SnapshotManager;

/**
 * Catch-up single-phase solidify: per-block session merge and WAL barriers run while the common
 * checkpoint is skipped entirely; leaving the mode publishes exactly one transition checkpoint.
 */
@Slf4j
public class SnapshotManagerCatchUpSolidifyTest extends BaseMethodTest {

  private SnapshotManager revokingDatabase;
  private TestRevokingTronStore tronDatabase;
  private CommonCheckpointRuntimeAttachment attachment;

  @Override
  protected void afterInit() {
    revokingDatabase = context.getBean(SnapshotManager.class);
    revokingDatabase.enable();
    tronDatabase = new TestRevokingTronStore("testCatchUpSolidify-test");
    revokingDatabase.add(tronDatabase.getRevokingDB());
    attachment = mock(CommonCheckpointRuntimeAttachment.class);
    try {
      Field field = SnapshotManager.class.getDeclaredField("commonCheckpointRuntimeAttachment");
      field.setAccessible(true);
      field.set(revokingDatabase, attachment);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void beforeDestroy() {
    tronDatabase.close();
  }

  @Test
  public synchronized void solidifyMergesPerBlockAndSkipsCheckpoint() throws Exception {
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }
    revokingDatabase.setMaxSize(1);
    revokingDatabase.setCatchUpSolidify(true);
    for (int i = 0; i < 6; i++) {
      revokingDatabase.buildSession();
    }
    // Six sessions with a one-block window: every overflow solidified instead of counting
    // toward a checkpoint, so no common checkpoint publication ever ran.
    verify(attachment, never()).checkpointAndRebase(anyInt());
    // The session stack stayed bounded: per-block merge kept size at the window.
    Assert.assertTrue(revokingDatabase.size() <= 2);
    // Leaving catch-up is the transition gate: exactly one checkpoint publishes the remainder.
    revokingDatabase.setCatchUpSolidify(false);
    verify(attachment, times(1)).checkpointAndRebase(anyInt());
    Assert.assertFalse(revokingDatabase.isCatchUpSolidify());
  }

  @Test
  public synchronized void steadyModeStillFlushesByCount() throws Exception {
    while (revokingDatabase.size() != 0) {
      revokingDatabase.pop();
    }
    revokingDatabase.setMaxSize(1);
    revokingDatabase.setMaxFlushCount(3);
    // Overflow is checked before the new session advances, so 8 sessions drive flushCount to
    // the 3-block threshold exactly twice.
    for (int i = 0; i < 8; i++) {
      revokingDatabase.buildSession();
    }
    // Without solidify the count cadence drives checkpoints: 7 blocks over a 3-block flush
    // count must have published at least twice.
    verify(attachment, times(2)).checkpointAndRebase(3);
  }

  @Test
  public synchronized void togglingSolidifyOnIsIdempotent() throws Exception {
    revokingDatabase.setCatchUpSolidify(true);
    revokingDatabase.setCatchUpSolidify(true);
    Assert.assertTrue(revokingDatabase.isCatchUpSolidify());
    verify(attachment, never()).checkpointAndRebase(anyInt());
  }
}
