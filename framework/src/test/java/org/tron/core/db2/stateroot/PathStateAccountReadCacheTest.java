package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateAccountReadCacheTest {

  private static final String ACCOUNT_FLAG = "tron.pathstate.accountReadCacheBenchmark";
  private static final String LARGE_FLAG = "tron.pathstate.largeReadCacheBenchmark";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void candidateOnlySelectsAccountRocksDb() {
    for (int storeId = 0; storeId <= 27; storeId++) {
      assertEquals(storeId == 4,
          PathStatePhysicalStoreSet.useAccountReadCache(storeId, Engine.ROCKSDB, true));
      assertFalse(PathStatePhysicalStoreSet.useAccountReadCache(storeId, Engine.ROCKSDB, false));
      assertFalse(PathStatePhysicalStoreSet.useAccountReadCache(storeId, Engine.LEVELDB, true));
    }
  }

  @Test
  public void physicalOpenKeepsShardsAndIndependentFlagsAcrossReopen() throws Exception {
    String previousAccount = System.getProperty(ACCOUNT_FLAG);
    String previousLarge = System.getProperty(LARGE_FLAG);
    Path root = temporaryFolder.newFolder("account-cache").toPath();
    PathStateParticipantScope scope = new PathStateCanonicalizer().participantScope();
    byte[] key = new byte[32];
    byte[] value = new byte[]{1, 2, 3};
    // Absent flags, account only, both flags, 5/22 only, and rollback to absent flags.
    boolean[] accountFlags = {false, true, true, false, false};
    boolean[] largeFlags = {false, false, true, true, false};
    try {
      for (int pass = 0; pass < accountFlags.length; pass++) {
        restoreProperty(ACCOUNT_FLAG, accountFlags[pass] ? "true" : null);
        restoreProperty(LARGE_FLAG, largeFlags[pass] ? "true" : null);
        try (PathStatePhysicalStoreSet stores = pass == 0
            ? PathStatePhysicalStoreSet.open(root, scope, Engine.ROCKSDB)
            : PathStatePhysicalStoreSet.openExisting(root, scope, Engine.ROCKSDB)) {
          for (PathStateParticipant participant : scope.getParticipants()) {
            PathStatePhysicalStoreSet.PhysicalStore store =
                stores.participant(participant.getDbName());
            if (pass == 0) {
              store.putFlat(key, value);
              store.nodeStore().put(key, value);
              store.putMetadata(key, value);
            }
            assertArrayEquals(value, store.getFlat(key));
            assertArrayEquals(value, store.nodeStore().get(key));
            assertArrayEquals(value, store.getMetadata(key));
          }
        }
        for (PathStateParticipant participant : scope.getParticipants()) {
          int id = participant.getStoreId();
          boolean giant = id == 4 || id == 5 || id == 22;
          long bytes = giant ? 64L << 20 : 32L << 20;
          int bits = id == 4 ? 4 : 6;
          if (id == 4 && accountFlags[pass]) {
            bytes = 256L << 20;
          } else if ((id == 5 || id == 22) && largeFlags[pass]) {
            bytes = 256L << 20;
            bits = 2;
          }
          assertNativeCache(root.resolve("stores").resolve(String.format(Locale.ROOT, "%02d-%s", id,
              participant.getDbName())).resolve("nodes"), bytes, bits);
        }
        assertNativeCache(root.resolve("super/nodes"), 32L << 20, 6);
      }
    } finally {
      restoreProperty(ACCOUNT_FLAG, previousAccount);
      restoreProperty(LARGE_FLAG, previousLarge);
    }
  }

  private static void assertNativeCache(Path directory, long bytes, int bits) throws Exception {
    String log = new String(Files.readAllBytes(directory.resolve("LOG")),
        StandardCharsets.UTF_8);
    assertTrue(directory.toString(), log.contains("capacity : " + bytes));
    assertTrue(directory.toString(), log.contains("num_shard_bits : " + bits));
  }

  private static void restoreProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }
}
