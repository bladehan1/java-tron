package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotImpl;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.store.AccountAssetStore;
import org.tron.protos.Protocol.Account;

public class SnapshotOldValueCollectorTest extends BaseMethodTest {

  @Test
  public void collectsBlockPreStateAfterNestedSessionsFinish() {
    MemoryDb memoryDb = new MemoryDb("abi");
    byte[] changed = bytes("changed");
    byte[] deleted = bytes("deleted");
    byte[] created = bytes("created");
    byte[] empty = bytes("empty");
    byte[] createThenDelete = bytes("create-then-delete");
    memoryDb.put(changed, bytes("old"));
    memoryDb.put(deleted, bytes("gone"));
    memoryDb.put(empty, new byte[0]);

    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    List<BlockReverseDiff> captured = new ArrayList<>();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), captured::add);

    byte[] hash = new byte[32];
    hash[31] = 1;
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash, new byte[32], 3_000L);
    try (ISession block = manager.buildSession()) {
      database.put(changed, bytes("intermediate"));
      try (ISession transaction = manager.buildSession()) {
        database.put(changed, bytes("new"));
        database.put(created, bytes("created-value"));
        transaction.merge();
      }
      try (ISession revertedTransaction = manager.buildSession()) {
        database.put(bytes("reverted"), bytes("not-visible"));
      }
      database.delete(deleted);
      database.put(empty, new byte[0]);
      database.put(createThenDelete, bytes("temporary"));
      database.delete(createThenDelete);
      block.commit(meta);
    }

    assertEquals(1, captured.size());
    BlockReverseDiff diff = captured.get(0);
    assertEquals(meta, diff.getMeta());
    assertEquals(meta, ((SnapshotImpl) database.getHead()).getBlockSnapshotMeta());
    assertEquals(1, diff.getGroups().size());
    DbGroup group = diff.getGroups().get(0);
    assertEquals("abi", group.getDbName());
    assertEquals(3, group.getEntries().size());

    assertArrayEquals(bytes("old"), find(group, changed).getOldValue().getValue());
    assertArrayEquals(bytes("gone"), find(group, deleted).getOldValue().getValue());
    assertFalse(find(group, created).getOldValue().isPresent());
    assertFalse(contains(group, empty));
    assertFalse(contains(group, createThenDelete));
    assertFalse(contains(group, bytes("reverted")));
    manager.shutdown();
  }

  @Test
  public void preservesPresentEmptyAndEmitsNoopBlockMetadata() {
    MemoryDb memoryDb = new MemoryDb("abi");
    byte[] key = bytes("key");
    memoryDb.put(key, new byte[0]);
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    List<BlockReverseDiff> captured = new ArrayList<>();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), captured::add);

    try (ISession block = manager.buildSession()) {
      database.put(key, bytes("value"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }
    assertTrue(find(captured.get(0).getGroups().get(0), key).getOldValue().isPresent());
    assertEquals(0,
        find(captured.get(0).getGroups().get(0), key).getOldValue().getValue().length);

    try (ISession block = manager.buildSession()) {
      block.commit(BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L));
    }
    assertEquals(2, captured.size());
    assertTrue(captured.get(1).getGroups().isEmpty());
    manager.shutdown();
  }

  @Test
  public void rejectsUnknownOrDuplicateRegisteredDatabaseNames() {
    SnapshotManager unknown = new SnapshotManager("");
    unknown.add(new Chainbase(new SnapshotRoot(new MemoryDb("new-store"))));
    IllegalStateException unknownError = assertThrows(IllegalStateException.class,
        () -> unknown.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { }));
    assertTrue(unknownError.getMessage().contains("new-store"));
    unknown.shutdown();

    SnapshotManager duplicate = new SnapshotManager("");
    duplicate.add(new Chainbase(new SnapshotRoot(new MemoryDb("abi"))));
    duplicate.add(new Chainbase(new SnapshotRoot(new MemoryDb("abi"))));
    IllegalStateException duplicateError = assertThrows(IllegalStateException.class,
        () -> duplicate.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { }));
    assertTrue(duplicateError.getMessage().contains("Duplicate"));
    duplicate.shutdown();
  }

  @Test
  public void classifiesEveryChainbaseRegisteredByTheApplication() {
    SnapshotManager applicationManager = context.getBean(SnapshotManager.class);
    ArchiveStoreScope.validate(applicationManager.getDbs());
  }

  @Test
  public void matchesReferenceStateForRandomBlockOperations() {
    MemoryDb memoryDb = new MemoryDb("abi");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    List<BlockReverseDiff> captured = new ArrayList<>();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), captured::add);

    Random random = new Random(0x5a17L);
    Map<String, byte[]> reference = new HashMap<>();
    for (int blockNumber = 1; blockNumber <= 40; blockNumber++) {
      Map<String, byte[]> before = copy(reference);
      try (ISession block = manager.buildSession()) {
        for (int operation = 0; operation < 25; operation++) {
          String key = "key-" + random.nextInt(12);
          int action = random.nextInt(4);
          boolean nested = random.nextBoolean();
          boolean keepNested = random.nextBoolean();
          if (nested) {
            try (ISession transaction = manager.buildSession()) {
              byte[] post = applyRandomOperation(database, key, action, blockNumber, operation);
              if (keepNested) {
                updateReference(reference, key, post);
                transaction.merge();
              }
            }
          } else {
            byte[] post = applyRandomOperation(database, key, action, blockNumber, operation);
            updateReference(reference, key, post);
          }
        }
        block.commit(BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
            hash(blockNumber - 1), blockNumber));
      }

      BlockReverseDiff diff = captured.get(captured.size() - 1);
      Map<String, OldValue> actual = new HashMap<>();
      if (!diff.getGroups().isEmpty()) {
        diff.getGroups().get(0).getEntries().forEach(entry -> actual.put(
            new String(entry.getKey(), java.nio.charset.StandardCharsets.UTF_8),
            entry.getOldValue()));
      }
      Set<String> keys = new HashSet<>(before.keySet());
      keys.addAll(reference.keySet());
      for (String key : keys) {
        byte[] oldValue = before.get(key);
        byte[] postValue = reference.get(key);
        if (Arrays.equals(oldValue, postValue)) {
          assertFalse("no-op key was emitted: " + key, actual.containsKey(key));
        } else {
          assertTrue("changed key was not emitted: " + key, actual.containsKey(key));
          OldValue archived = actual.get(key);
          assertEquals(oldValue != null, archived.isPresent());
          if (oldValue != null) {
            assertArrayEquals(oldValue, archived.getValue());
          }
        }
      }
      assertEquals(keys.stream().filter(key -> !Arrays.equals(before.get(key), reference.get(key)))
          .count(), actual.size());
    }
    manager.shutdown();
  }

  @Test
  public void projectsAccountAssetTransitionBeforeRootMerge() {
    byte[] address = bytes("account-address");
    byte[] token = bytes("1000001");
    Account oldAccount = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .putAssetV2("1000001", 100L)
        .build();
    Account postAccount = oldAccount.toBuilder().putAssetV2("1000001", 80L).build();

    MemoryDb memoryDb = new MemoryDb("account");
    memoryDb.put(address, oldAccount.toByteArray());
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(new HashMap<>());

    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    List<BlockReverseDiff> captured = new ArrayList<>();
    AccountAssetArchiveProjector projector = new AccountAssetArchiveProjector(assetStore,
        () -> true);
    manager.installArchiveCollector(new SnapshotOldValueCollector(projector), captured::add);

    try (ISession block = manager.buildSession()) {
      database.put(address, postAccount.toByteArray());
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    DbGroup accountGroup = captured.get(0).getGroups().stream()
        .filter(group -> "account".equals(group.getDbName()))
        .findFirst().orElseThrow(AssertionError::new);
    Account archivedAccount;
    try {
      archivedAccount = Account.parseFrom(find(accountGroup, address).getOldValue().getValue());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
    assertFalse(archivedAccount.getAssetOptimized());
    assertEquals(100L, archivedAccount.getAssetV2Map().get("1000001").longValue());

    DbGroup assetGroup = captured.get(0).getGroups().stream()
        .filter(group -> "account-asset".equals(group.getDbName()))
        .findFirst().orElseThrow(AssertionError::new);
    Entry assetEntry = find(assetGroup, Bytes.concat(address, token));
    assertFalse(assetEntry.getOldValue().isPresent());
    manager.shutdown();
  }

  @Test
  public void projectsOldPhysicalAssetValueForOptimizedAccount() {
    byte[] address = bytes("optimized-address");
    byte[] token = bytes("1000002");
    byte[] assetKey = Bytes.concat(address, token);
    Account oldAccount = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .setAssetOptimized(true)
        .build();
    Account postAccount = oldAccount.toBuilder().putAssetV2("1000002", 80L).build();

    MemoryDb memoryDb = new MemoryDb("account");
    memoryDb.put(address, oldAccount.toByteArray());
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    Map<WrappedByteArray, byte[]> persisted = new HashMap<>();
    persisted.put(WrappedByteArray.copyOf(assetKey), Longs.toByteArray(100L));
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(persisted);

    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    List<BlockReverseDiff> captured = new ArrayList<>();
    manager.installArchiveCollector(new SnapshotOldValueCollector(
        new AccountAssetArchiveProjector(assetStore, () -> true)), captured::add);

    try (ISession block = manager.buildSession()) {
      database.put(address, postAccount.toByteArray());
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    assertFalse(captured.get(0).getGroups().stream()
        .anyMatch(group -> "account".equals(group.getDbName())));
    DbGroup assetGroup = captured.get(0).getGroups().stream()
        .filter(group -> "account-asset".equals(group.getDbName()))
        .findFirst().orElseThrow(AssertionError::new);
    assertArrayEquals(Longs.toByteArray(100L),
        find(assetGroup, assetKey).getOldValue().getValue());
    manager.shutdown();
  }

  private static Entry find(DbGroup group, byte[] key) {
    return group.getEntries().stream()
        .filter(entry -> Arrays.equals(entry.getKey(), key))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static boolean contains(DbGroup group, byte[] key) {
    return group.getEntries().stream().anyMatch(entry -> Arrays.equals(entry.getKey(), key));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] applyRandomOperation(Chainbase database, String key, int action,
      int blockNumber, int operation) {
    byte[] encodedKey = bytes(key);
    if (action == 0) {
      database.delete(encodedKey);
      return null;
    }
    byte[] value = action == 1 ? new byte[0]
        : bytes("value-" + blockNumber + '-' + operation + '-' + action);
    database.put(encodedKey, value);
    return value;
  }

  private static void updateReference(Map<String, byte[]> reference, String key, byte[] value) {
    if (value == null) {
      reference.remove(key);
    } else {
      reference.put(key, Arrays.copyOf(value, value.length));
    }
  }

  private static Map<String, byte[]> copy(Map<String, byte[]> source) {
    Map<String, byte[]> copy = new HashMap<>();
    source.forEach((key, value) -> copy.put(key, Arrays.copyOf(value, value.length)));
    return copy;
  }

  private static final class MemoryDb implements DB<byte[], byte[]> {
    private final String name;
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();

    private MemoryDb(String name) {
      this.name = name;
    }

    @Override
    public byte[] get(byte[] key) {
      byte[] value = values.get(WrappedByteArray.of(key));
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public void put(byte[] key, byte[] value) {
      values.put(WrappedByteArray.copyOf(key), Arrays.copyOf(value, value.length));
    }

    @Override
    public long size() {
      return values.size();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public void remove(byte[] key) {
      values.remove(WrappedByteArray.of(key));
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
      values.forEach((key, value) -> entries.add(new AbstractMap.SimpleImmutableEntry<>(
          key.getBytes(), Arrays.copyOf(value, value.length))));
      return entries.iterator();
    }

    @Override
    public void close() {
      values.clear();
    }

    @Override
    public String getDbName() {
      return name;
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], byte[]> newInstance() {
      return new MemoryDb(name);
    }
  }
}
