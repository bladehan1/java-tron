package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;

public class HistoricalAccountBalanceReaderTest {

  @Test
  public void distinguishesHistoricalBalanceZeroFromAbsentAccount() throws Exception {
    byte[] existing = address(1);
    byte[] absent = address(2);
    ArchiveReadSnapshot snapshot = snapshot(existing, account(existing, 0));

    HistoricalAccountBalanceReader.Result present =
        HistoricalAccountBalanceReader.read(snapshot, existing);
    assertTrue(present.isPresent());
    assertEquals(0, present.getBalance());
    assertEquals(7, present.getBlockNumber());

    HistoricalAccountBalanceReader.Result missing =
        HistoricalAccountBalanceReader.read(snapshot, absent);
    assertFalse(missing.isPresent());
    assertThrows(IllegalStateException.class, missing::getBalance);
    snapshot.close();
  }

  @Test
  public void rejectsMalformedOrWrongKeyAccountValues() throws Exception {
    byte[] address = address(3);
    ArchiveReadSnapshot malformed = snapshot(address, new byte[]{1, 2, 3});
    assertThrows(ArchivePersistenceException.class,
        () -> HistoricalAccountBalanceReader.read(malformed, address));
    malformed.close();

    ArchiveReadSnapshot wrongKey = snapshot(address, account(address(4), 99));
    assertThrows(ArchivePersistenceException.class,
        () -> HistoricalAccountBalanceReader.read(wrongKey, address));
    wrongKey.close();
  }

  private static ArchiveReadSnapshot snapshot(byte[] key, byte[] value) throws Exception {
    byte[] hash = hash(7);
    ServingKeyIndexGeneration serving = ServingKeyIndexGeneration.rebuild(
        "account-balance", 0, hash(0), Collections.emptyList(), ignored -> null,
        Collections.singletonList("account"),
        ServingKeyIndexGeneration.IndexLayout.prototypeDefaults());
    // Empty-prefix coverage ends at the base. Build the pinned fixture at block 0 then expose the
    // requested target through a seven-block no-change committed prefix.
    java.util.List<HistoryCommitMarker> markers = new java.util.ArrayList<>();
    java.util.Map<Long, HistoryIndexRecord> indexes = new java.util.HashMap<>();
    for (int block = 1; block <= 7; block++) {
      BlockSnapshotMeta meta = new BlockSnapshotMeta(block, block, hash(block), hash(block - 1),
          block * 3_000L);
      HistoryLocation body = new HistoryLocation(0, block * 100L, 10, block, hash(block + 20));
      HistoryIndexRecord index = new HistoryIndexRecord(meta, body, Collections.emptyList());
      HistoryIndexLocation location = new HistoryIndexLocation(block * 80L, 20,
          hash(block + 40));
      markers.add(new HistoryCommitMarker(meta, block - 1, body, location, new byte[16],
          Collections.singletonList("account")));
      indexes.put(location.getOffset(), index);
    }
    serving = ServingKeyIndexGeneration.rebuild("account-balance", 0, hash(0), markers,
        location -> indexes.get(location.getOffset()));
    ServingKeyIndexGeneration finalServing = serving;
    ArchiveReadSnapshot.PinnedLatestState latest = new ArchiveReadSnapshot.PinnedLatestState() {
      @Override
      public long getBlockNumber() {
        return 7;
      }

      @Override
      public byte[] getBlockHash() {
        return hash;
      }

      @Override
      public OldValue get(String dbName, byte[] rawKey) {
        return Arrays.equals(key, rawKey) ? OldValue.present(value) : OldValue.absent();
      }

      @Override
      public java.util.List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lower,
          byte[] upper, int maxEntries) {
        return Collections.emptyList();
      }

      @Override
      public void close() {
      }
    };
    ArchiveReadSnapshot.PinnedHistory history = new ArchiveReadSnapshot.PinnedHistory() {
      @Override
      public long getIndexedFrom() {
        return finalServing.getIndexedFrom();
      }

      @Override
      public long getIndexedThrough() {
        return 7;
      }

      @Override
      public byte[] getHeadHash() {
        return hash;
      }

      @Override
      public byte[] getAuthoritativePrefixDigest() {
        return finalServing.getAuthoritativePrefixDigest();
      }

      @Override
      public OldValue read(String dbName, byte[] rawKey, long firstChangeBlock) {
        throw new AssertionError("No key changes are expected in this fixture");
      }

      @Override
      public void close() {
      }
    };
    return ArchiveReadSnapshot.pin(7, 7, hash, serving, latest, history);
  }

  private static byte[] account(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setBalance(balance).build().toByteArray();
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }
}
