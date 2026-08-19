package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.StoreSnapshot;
import org.tron.core.db2.common.DB;

public class LatestStateGenerationAdapterTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "properties");

  @Test
  public void currentDbAbstractionFailsClosedInsteadOfUsingOrdinaryGet() {
    Map<String, DB<byte[], byte[]>> databases = new LinkedHashMap<>();
    databases.put("account", new OrdinaryDb("account"));
    databases.put("properties", new OrdinaryDb("properties"));

    ArchivePersistenceException failure = assertThrows(ArchivePersistenceException.class,
        () -> LatestStateGenerationAdapter.fromDatabases(PARTICIPANTS, databases));
    assertTrue(failure.getMessage().contains("stable snapshot lifecycle"));
    assertFalse(((OrdinaryDb) databases.get("account")).read);
    assertFalse(((OrdinaryDb) databases.get("properties")).read);
  }

  @Test
  public void pinsExactGenerationAndSurvivesLiveSourceReplacement() throws Exception {
    FakeStore account = new FakeStore("account", "rocksdb:/state/account", bytes("old"));
    FakeStore properties = new FakeStore("properties", "leveldb:/state/properties",
        bytes("property"));
    LatestStateGenerationAdapter adapter = adapter(account, properties);
    byte[] expectedDigest = adapter.getSourceIdentityDigest();

    try (PinnedLatestState pinned = adapter.pin("generation-1", 7, hash(7), PARTICIPANTS)) {
      account.replace("rocksdb:/replacement/account", bytes("new"));
      assertArrayEquals(bytes("old"), pinned.get("account", bytes("key")).getValue());
      assertArrayEquals(expectedDigest, pinned.getSourceIdentityDigest());
      assertThrows(UnsupportedOperationException.class,
          () -> pinned.range("account", new byte[0], null));
    }
    assertEquals(1, account.closedSnapshots.get());
    assertEquals(1, properties.closedSnapshots.get());
  }

  @Test
  public void releasesPartialAcquireAndRejectsReplacementIdentity() throws Exception {
    FakeStore account = new FakeStore("account", "rocksdb:/state/account", bytes("old"));
    FakeStore properties = new FakeStore("properties", "leveldb:/state/properties",
        bytes("property"));
    properties.failPin.set(true);
    LatestStateGenerationAdapter adapter = adapter(account, properties);

    assertThrows(IOException.class,
        () -> adapter.pin("generation-1", 7, hash(7), PARTICIPANTS));
    assertEquals(1, account.closedSnapshots.get());

    properties.failPin.set(false);
    properties.replaceAfterPin.set(true);
    assertThrows(ArchivePersistenceException.class,
        () -> adapter.pin("generation-1", 7, hash(7), PARTICIPANTS));
    assertEquals(2, account.closedSnapshots.get());
    assertEquals(1, properties.closedSnapshots.get());
  }

  @Test
  public void rejectsParticipantAndSnapshotBlockIdentityMismatch() {
    FakeStore account = new FakeStore("account", "rocksdb:/state/account", bytes("old"));
    FakeStore properties = new FakeStore("properties", "leveldb:/state/properties",
        bytes("property"));
    LatestStateGenerationAdapter adapter = adapter(account, properties);

    assertThrows(ArchivePersistenceException.class,
        () -> adapter.pin("generation-1", 7, hash(7), Collections.singletonList("account")));
    properties.wrongBlock.set(true);
    assertThrows(ArchivePersistenceException.class,
        () -> adapter.pin("generation-1", 7, hash(7), PARTICIPANTS));
    assertEquals(1, account.closedSnapshots.get());
    assertEquals(1, properties.closedSnapshots.get());
  }

  @Test
  public void rejectsSourceReplacementBeforeAcquiringAnySnapshot() {
    FakeStore account = new FakeStore("account", "rocksdb:/state/account", bytes("old"));
    FakeStore properties = new FakeStore("properties", "leveldb:/state/properties",
        bytes("property"));
    LatestStateGenerationAdapter adapter = adapter(account, properties);
    account.replace("rocksdb:/replacement/account", bytes("new"));

    assertThrows(ArchivePersistenceException.class,
        () -> adapter.pin("generation-1", 7, hash(7), PARTICIPANTS));
    assertEquals(0, account.closedSnapshots.get());
    assertEquals(0, properties.closedSnapshots.get());
  }

  private static LatestStateGenerationAdapter adapter(FakeStore... stores) {
    Map<String, SnapshotCapableStore> indexed = new LinkedHashMap<>();
    for (FakeStore store : stores) {
      indexed.put(store.dbName, store);
    }
    return new LatestStateGenerationAdapter(PARTICIPANTS, indexed);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class FakeStore implements SnapshotCapableStore {
    private final String dbName;
    private final AtomicBoolean failPin = new AtomicBoolean();
    private final AtomicBoolean wrongBlock = new AtomicBoolean();
    private final AtomicBoolean replaceAfterPin = new AtomicBoolean();
    private final AtomicInteger closedSnapshots = new AtomicInteger();
    private String identity;
    private byte[] value;

    private FakeStore(String dbName, String identity, byte[] value) {
      this.dbName = dbName;
      this.identity = identity;
      this.value = Arrays.copyOf(value, value.length);
    }

    private void replace(String replacementIdentity, byte[] replacementValue) {
      identity = replacementIdentity;
      value = Arrays.copyOf(replacementValue, replacementValue.length);
    }

    @Override
    public String getDbName() {
      return dbName;
    }

    @Override
    public String getSourceIdentity() {
      return identity;
    }

    @Override
    public StoreSnapshot pin(long blockNumber, byte[] blockHash) throws IOException {
      if (failPin.get()) {
        throw new IOException("injected pin failure");
      }
      String candidateIdentity = identity;
      byte[] pinnedValue = Arrays.copyOf(value, value.length);
      if (replaceAfterPin.get()) {
        identity = identity + ":replacement";
        candidateIdentity = identity;
      }
      final String pinnedIdentity = candidateIdentity;
      long pinnedBlock = wrongBlock.get() ? blockNumber + 1 : blockNumber;
      return new StoreSnapshot() {
        private boolean closed;

        @Override
        public String getDbName() {
          return dbName;
        }

        @Override
        public String getSourceIdentity() {
          return pinnedIdentity;
        }

        @Override
        public long getBlockNumber() {
          return pinnedBlock;
        }

        @Override
        public byte[] getBlockHash() {
          return Arrays.copyOf(blockHash, blockHash.length);
        }

        @Override
        public byte[] get(byte[] physicalRawKey) {
          if (closed) {
            throw new IllegalStateException("snapshot is closed");
          }
          return Arrays.copyOf(pinnedValue, pinnedValue.length);
        }

        @Override
        public void close() {
          if (!closed) {
            closed = true;
            closedSnapshots.incrementAndGet();
          }
        }
      };
    }
  }

  private static final class OrdinaryDb implements DB<byte[], byte[]> {
    private final String dbName;
    private boolean read;

    private OrdinaryDb(String dbName) {
      this.dbName = dbName;
    }

    @Override
    public byte[] get(byte[] key) {
      read = true;
      return null;
    }

    @Override
    public void put(byte[] key, byte[] value) {
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public void remove(byte[] key) {
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      return Collections.<Map.Entry<byte[], byte[]>>emptyList().iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public String getDbName() {
      return dbName;
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], byte[]> newInstance() {
      return this;
    }
  }
}
