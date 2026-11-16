package org.tron.core.db2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import org.junit.Test;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRuntimeAttachment;
import org.tron.core.db2.checkpoint.common.CommonCheckpointServingException;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.exception.TronError;

public class SnapshotManagerServingFailureTest {

  @Test
  public void shutdownDoesNotRecaptureAlreadyPublishedAndRebasedPrefix() throws Exception {
    SnapshotManager snapshots = new SnapshotManager("unused-checkpoint-path");
    snapshots.setUnChecked(false);
    CommonCheckpointRuntimeAttachment common = mock(CommonCheckpointRuntimeAttachment.class);
    field("commonCheckpointRuntimeAttachment").set(snapshots, common);
    field("flushCount").setInt(snapshots, 1);
    byte[] digest = new byte[32];
    BlockSnapshotMeta block = BlockSnapshotMeta.forBlock(1, digest, digest, 3_000);
    CommonCheckpointTarget target = CommonCheckpointTarget.restore(
        digest, digest, block, block, digest, digest);
    when(common.checkpointAndRebase(1)).thenThrow(
        new CommonCheckpointServingException(target, new IOException("index sync failed")));

    assertThrows(TronError.class, snapshots::flushPending);
    assertEquals(0, field("flushCount").getInt(snapshots));
    assertTrue(field("hitDown").getBoolean(snapshots));
    snapshots.flushPending();
    verify(common, times(1)).checkpointAndRebase(1);
  }

  private static Field field(String name) throws Exception {
    Field field = SnapshotManager.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
