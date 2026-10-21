package org.tron.core.consensus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.args.GenesisBlock;
import org.tron.consensus.ConsensusDelegate;
import org.tron.consensus.base.Param;
import org.tron.consensus.dpos.DposService;
import org.tron.consensus.dpos.DposSlot;
import org.tron.consensus.dpos.DposTask;
import org.tron.consensus.dpos.MaintenanceManager;
import org.tron.consensus.dpos.StateManager;
import org.tron.core.capsule.WitnessCapsule;

public class GenesisScheduleMaintenanceTest {
  @Test
  public void earlyRegisteredWriterAndNormalStartProduceIdenticalSchedule() {
    Fixture normal = new Fixture(0);
    normal.service.start(parameters());
    Fixture early = new Fixture(0);
    early.service.initGenesisSchedule();
    early.service.start(parameters());
    early.service.initGenesisSchedule();
    assertEquals(normal.active, early.active);
    assertEquals(27, early.active.size());
    for (Map.Entry<ByteString, WitnessCapsule> witness : normal.witnesses.entrySet()) {
      assertEquals(witness.getValue().getInstance(),
          early.witnesses.get(witness.getKey()).getInstance());
    }
    for (ByteString address : early.active) {
      assertTrue(early.witnesses.get(address).getIsJobs());
    }
  }

  @Test
  public void existingChainDoesNotRewriteGenesisSchedule() {
    Fixture existing = new Fixture(1);
    existing.service.initGenesisSchedule();
    existing.service.start(parameters());
    assertTrue(existing.active.isEmpty());
    assertTrue(existing.witnesses.values().stream().noneMatch(WitnessCapsule::getIsJobs));
  }

  private static Param parameters() {
    Param param = mock(Param.class);
    GenesisBlock genesis = mock(GenesisBlock.class);
    when(genesis.getTimestamp()).thenReturn("0");
    when(param.getGenesisBlock()).thenReturn(genesis);
    when(param.getMiners()).thenReturn(Collections.emptyList());
    return param;
  }

  private static final class Fixture {
    private final DposService service = new DposService();
    private final List<ByteString> active = new ArrayList<>();
    private final Map<ByteString, WitnessCapsule> witnesses = new LinkedHashMap<>();

    private Fixture(long head) {
      ConsensusDelegate delegate = mock(ConsensusDelegate.class);
      ReflectionTestUtils.setField(service, "consensusDelegate", delegate);
      ReflectionTestUtils.setField(service, "dposTask", mock(DposTask.class));
      ReflectionTestUtils.setField(service, "dposSlot", mock(DposSlot.class));
      ReflectionTestUtils.setField(service, "stateManager", mock(StateManager.class));
      ReflectionTestUtils.setField(service, "maintenanceManager", mock(MaintenanceManager.class));
      for (int number = 1; number <= 30; number++) {
        ByteString address = ByteString.copyFrom(new byte[]{(byte) number});
        witnesses.put(address, new WitnessCapsule(address, number, ""));
      }
      when(delegate.getLatestBlockHeaderNumber()).thenReturn(head);
      when(delegate.getAllWitnesses()).thenAnswer(call -> new ArrayList<>(witnesses.values()));
      when(delegate.getActiveWitnesses()).thenAnswer(call -> new ArrayList<>(active));
      when(delegate.getWitness(org.mockito.ArgumentMatchers.any(byte[].class)))
          .thenAnswer(call -> witnesses.get(ByteString.copyFrom((byte[]) call.getArgument(0))));
      doAnswer(call -> {
        List<ByteString> addresses = call.getArgument(0);
        addresses.sort((left, right) -> Long.compare(witnesses.get(right).getVoteCount(),
            witnesses.get(left).getVoteCount()));
        return null;
      }).when(delegate).sortWitness(anyList());
      doAnswer(call -> {
        active.clear();
        active.addAll(call.getArgument(0));
        return null;
      }).when(delegate).saveActiveWitnesses(anyList());
    }
  }
}
