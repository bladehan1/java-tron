package org.tron.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db2.archive.HistoricalQueryException;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.db2.archive.HistoricalQuerySession;
import org.tron.core.db2.archive.StateArchiveCheckpointReadSnapshot;
import org.tron.core.db2.core.CommonCheckpointRuntimeAttachment;

public class ManagerArchiveHistoricalQueryAdmissionTest {

  private final int originalCapacity = CommonParameter.getInstance().getMaxHttpConnectNumber();
  private Manager manager;
  private ChainBaseManager chainBaseManager;

  @Before
  public void setUp() {
    CommonParameter.getInstance().setMaxHttpConnectNumber(1);
    manager = new Manager();
    chainBaseManager = mock(ChainBaseManager.class);
    ReflectionTestUtils.setField(manager, "chainBaseManager", chainBaseManager);
  }

  @After
  public void tearDown() {
    CommonParameter.getInstance().setMaxHttpConnectNumber(originalCapacity);
  }

  @Test
  public void unavailableRuntimeFailureReleasesTheAdmissionSlot() {
    byte[] expected = new byte[32];
    HistoricalQueryException first = assertThrows(HistoricalQueryException.class,
        () -> manager.openArchiveHistoricalQuery(10, expected));
    assertEquals(Reason.UNAVAILABLE, first.getReason());
    // capacity 1: a leaked slot would surface as OVERLOADED here instead
    HistoricalQueryException second = assertThrows(HistoricalQueryException.class,
        () -> manager.openArchiveHistoricalQuery(10, expected));
    assertEquals(Reason.UNAVAILABLE, second.getReason());
  }

  @Test
  public void nonCanonicalFailureReleasesTheAdmissionSlot() throws Exception {
    BlockCapsule block = new BlockCapsule(10, Sha256Hash.ZERO_HASH, 99999, ByteString.EMPTY);
    when(chainBaseManager.getBlockByNum(anyLong())).thenReturn(block);
    ReflectionTestUtils.setField(manager, "commonCheckpointRuntime",
        mock(CommonCheckpointRuntimeAttachment.class));

    HistoricalQueryException rejected = assertThrows(HistoricalQueryException.class,
        () -> manager.openArchiveHistoricalQuery(10, new byte[32]));
    assertEquals(Reason.NON_CANONICAL, rejected.getReason());

    CommonCheckpointRuntimeAttachment common = (CommonCheckpointRuntimeAttachment)
        ReflectionTestUtils.getField(manager, "commonCheckpointRuntime");
    StateArchiveCheckpointReadSnapshot snapshot =
        mock(StateArchiveCheckpointReadSnapshot.class);
    when(snapshot.getTargetBlock()).thenReturn(10L);
    when(common.pinPoint(anyLong(), any())).thenReturn(snapshot);
    try (HistoricalQuerySession session = manager.openArchiveHistoricalQuery(10,
        block.getBlockId().getBytes())) {
      assertEquals(10, session.getTargetBlock());
    }
  }
}
