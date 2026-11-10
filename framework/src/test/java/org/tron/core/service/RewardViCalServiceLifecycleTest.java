package org.tron.core.service;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.tron.common.TestConstants;
import org.tron.common.utils.ReflectUtils;
import org.tron.core.config.args.Args;
import org.tron.core.db2.common.DB;
import org.tron.core.store.DelegationStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.WitnessStore;

public class RewardViCalServiceLifecycleTest {

  @Test
  public void shouldInterruptAndAwaitWorkerBeforeReturningFromStop() throws Exception {
    Args.setParam(new String[0], TestConstants.TEST_CONF);
    RewardViCalService service = null;
    try {
      DynamicPropertiesStore propertiesStore = mock(DynamicPropertiesStore.class);
      DelegationStore delegationStore = mock(DelegationStore.class);
      WitnessStore witnessStore = mock(WitnessStore.class);
      DB<byte[], byte[]> db = mockDb();
      when(propertiesStore.getDb()).thenReturn(db);
      when(delegationStore.getDb()).thenReturn(db);
      when(witnessStore.getDb()).thenReturn(db);
      service = new RewardViCalService(propertiesStore, delegationStore, witnessStore);
      ScheduledExecutorService executor = ReflectUtils.getFieldValue(service, "es");
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch interrupted = new CountDownLatch(1);
      executor.execute(() -> {
        started.countDown();
        try {
          new CountDownLatch(1).await();
        } catch (InterruptedException e) {
          interrupted.countDown();
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));

      service.stop();
      service.stop();

      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertTrue(executor.isTerminated());
    } finally {
      if (service != null) {
        service.stop();
      }
      Args.clearParam();
    }
  }

  @SuppressWarnings("unchecked")
  private static DB<byte[], byte[]> mockDb() {
    return mock(DB.class);
  }
}
