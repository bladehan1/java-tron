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
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.core.vm.program.Storage;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

public class HistoricalStorageCompatibilityTest {

  @Test
  public void matchesUpstreamDualKeyReadsIncludingAuthoritativeZero() throws Exception {
    byte[] address = HistoricalTestState.address(60);
    byte[] slot = new byte[32];
    slot[31] = 1;
    for (int version : new int[]{0, 1}) {
      for (boolean optimize : new boolean[]{false, true}) {
        for (int shape = 0; shape < 4; shape++) {
          HistoricalTestState state = new HistoricalTestState().property(
              "ALLOW_OPTIMIZE_TVM_STORAGE", optimize ? 1 : 0);
          state.contract(address, new byte[]{0}, SmartContract.newBuilder()
              .setVersion(version).build());
          Map<String, byte[]> rows = new HashMap<>();
          byte[] legacyKey = StorageRowKeyCodec.physicalKey(address, slot, version, null);
          byte[] optimizedKey = StorageRowKeyCodec.optimizedKey(address, slot, null);
          byte[] oldValue = new byte[32];
          oldValue[31] = 42;
          if (shape != 3) {
            state.put("storage-row", legacyKey, oldValue);
            rows.put(Hex.toHexString(legacyKey), oldValue);
          }
          if (shape == 1 || shape == 2) {
            byte[] value = new byte[32];
            value[31] = (byte) (shape == 1 ? 43 : 0);
            state.put("storage-row", optimizedKey, value);
            rows.put(Hex.toHexString(optimizedKey), value);
          }
          Storage upstream = new Storage(address, null, key -> {
            byte[] value = rows.get(Hex.toHexString(key));
            return value == null ? null : new StorageRowCapsule(key, value);
          }, optimize, false);
          upstream.setContractVersion(version);
          try (HistoricalQuerySession session = state.open()) {
            DataWord expected = upstream.getValue(new DataWord(slot));
            assertArrayEquals(expected == null ? new byte[32] : expected.getData(),
                session.getStorage(address, slot).orElse(new byte[32]));
          }
        }
      }
    }
  }

  @Test
  public void evaluatesFork38AtHistoricalStateWithoutLatestController() throws Exception {
    ForkBlockVersionEnum fork = ForkBlockVersionEnum.VERSION_4_8_2_3;
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
