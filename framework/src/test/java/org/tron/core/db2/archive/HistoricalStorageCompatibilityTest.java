package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.tron.common.runtime.vm.DataWord;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.core.vm.program.Storage;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

public class HistoricalStorageCompatibilityTest {

  @Test
  public void matchesDevelopStorageReadsForPresentZeroAndMissingRows() throws Exception {
    byte[] address = HistoricalTestState.address(60);
    byte[] slot = new byte[32];
    slot[31] = 1;
    for (int version : new int[]{0, 1}) {
      for (int shape = 0; shape < 3; shape++) {
        HistoricalTestState state = new HistoricalTestState();
        state.contract(address, new byte[]{0}, SmartContract.newBuilder()
            .setVersion(version).build());
        Map<String, byte[]> rows = new HashMap<>();
        byte[] physicalKey = StorageRowKeyCodec.physicalKey(address, slot, version, null);
        if (shape != 2) {
          byte[] value = new byte[32];
          value[31] = (byte) (shape == 0 ? 42 : 0);
          state.put("storage-row", physicalKey, value);
          rows.put(Hex.toHexString(physicalKey), value);
        }
        Storage develop = new Storage(address, null, key -> {
          byte[] value = rows.get(Hex.toHexString(key));
          return value == null ? null : new StorageRowCapsule(key, value);
        });
        develop.setContractVersion(version);
        try (HistoricalQuerySession session = state.open()) {
          DataWord expected = develop.getValue(new DataWord(slot));
          assertArrayEquals(expected == null ? new byte[32] : expected.getData(),
              session.getStorage(address, slot).orElse(new byte[32]));
        }
      }
    }
  }

  @Test
  public void evaluatesDevelopForkAtHistoricalStateWithoutLatestController() throws Exception {
    ForkBlockVersionEnum fork = ForkBlockVersionEnum.VERSION_4_8_2_2;
    HistoricalTestState before = new HistoricalTestState().vmDefaults();
    HistoricalTestState after = new HistoricalTestState().vmDefaults()
        .property("latest_block_header_timestamp", fork.getHardForkTime() + 43_200_000L);
    byte[] stats = new byte[27];
    java.util.Arrays.fill(stats, (byte) 1);
    after.put("properties",
        HistoricalTestState.bytes("FORK_VERSION_" + fork.getValue()), stats);
    try (HistoricalQuerySession old = before.open();
        HistoricalQuerySession current = after.open()) {
      assertEquals(false, old.passesFork(fork));
      assertEquals(true, current.passesFork(fork));
    }
  }
}
