package org.tron.common.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EngineSourceIdentityFileTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void identityIsStableAcrossReopenAndBoundToEngineAndDbName() throws Exception {
    Path database = temporaryFolder.newFolder("stable").toPath();
    String first = EngineSourceIdentityFile.loadOrCreate(database, "rocksdb", "account");
    String reopened = EngineSourceIdentityFile.loadOrCreate(database, "ROCKSDB", "account");

    assertEquals(first, reopened);
    assertTrue(first.startsWith("rocksdb:account:"));
    assertThrows(IOException.class,
        () -> EngineSourceIdentityFile.loadOrCreate(database, "LEVELDB", "account"));
    assertThrows(IOException.class,
        () -> EngineSourceIdentityFile.loadOrCreate(database, "ROCKSDB", "properties"));
  }

  @Test
  public void missingIdentityCreatesANewDurableUuid() throws Exception {
    Path database = temporaryFolder.newFolder("recreated").toPath();
    String first = EngineSourceIdentityFile.loadOrCreate(database, "LEVELDB", "account");
    Files.delete(EngineSourceIdentityFile.identityPath(database));
    String recreated = EngineSourceIdentityFile.loadOrCreate(database, "LEVELDB", "account");

    assertNotEquals(first, recreated);
    assertEquals(recreated,
        EngineSourceIdentityFile.loadOrCreate(database, "LEVELDB", "account"));
  }

  @Test
  public void corruptionFailsClosedWithoutReplacingIdentity() throws Exception {
    Path database = temporaryFolder.newFolder("corrupt").toPath();
    EngineSourceIdentityFile.loadOrCreate(database, "ROCKSDB", "account");
    Path identity = EngineSourceIdentityFile.identityPath(database);
    byte[] corrupt = Files.readAllBytes(identity);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(identity, corrupt);

    assertThrows(IOException.class,
        () -> EngineSourceIdentityFile.loadOrCreate(database, "ROCKSDB", "account"));
    assertEquals(corrupt.length, Files.size(identity));
  }

  @Test
  public void concurrentOpenersEstablishOneIdentity() throws Exception {
    Path database = temporaryFolder.newFolder("concurrent").toPath();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    Set<String> identities = ConcurrentHashMap.newKeySet();
    try {
      Future<?>[] opens = new Future<?>[8];
      for (int i = 0; i < opens.length; i++) {
        opens[i] = executor.submit(() -> identities.add(
            EngineSourceIdentityFile.loadOrCreate(database, "ROCKSDB", "account")));
      }
      for (Future<?> open : opens) {
        open.get();
      }
    } finally {
      executor.shutdownNow();
    }

    assertEquals(1, identities.size());
  }
}
