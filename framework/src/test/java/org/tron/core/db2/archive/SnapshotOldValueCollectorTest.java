package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.AccountAssetPreparedBlockPayloadOwner.FrozenBatch;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotImpl;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.exception.TronError;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.CheckTmpStore;
import org.tron.protos.Protocol.Account;

public class SnapshotOldValueCollectorTest extends BaseMethodTest {

  @Test
  public void collectsBlockPreStateAfterNestedSessionsFinish() {
    MemoryDb memoryDb = new MemoryDb("code");
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
    BlockReverseDiffSink sink = mock(BlockReverseDiffSink.class);
    manager.installArchiveCollector(new SnapshotOldValueCollector(), sink);

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

    BlockReverseDiff diff = prepared(database);
    verify(sink, never()).accept(any(BlockReverseDiff.class));
    assertEquals(meta, diff.getMeta());
    assertEquals(meta, ((SnapshotImpl) database.getHead()).getBlockSnapshotMeta());
    assertEquals(1, diff.getGroups().size());
    DbGroup group = diff.getGroups().get(0);
    assertEquals("code", group.getDbName());
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
  public void preservesStorageRowPhysicalKeyWithoutLogicalProjection() {
    MemoryDb memoryDb = new MemoryDb("storage-row");
    byte[] addressHash = new byte[32];
    byte[] slotPart = new byte[32];
    for (int i = 0; i < 32; i++) {
      addressHash[i] = (byte) i;
      slotPart[i] = (byte) (0x80 + i);
    }
    byte[] physicalKey = new byte[32];
    System.arraycopy(addressHash, 0, physicalKey, 0, 16);
    System.arraycopy(slotPart, 16, physicalKey, 16, 16);
    memoryDb.put(physicalKey, bytes("old-word"));

    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });

    try (ISession block = manager.buildSession()) {
      database.put(physicalKey, bytes("new-word"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    BlockReverseDiff diff = prepared(database);
    assertEquals(1, diff.getGroups().size());
    DbGroup group = diff.getGroups().get(0);
    assertEquals("storage-row", group.getDbName());
    assertEquals(1, group.getEntries().size());
    assertArrayEquals(physicalKey, group.getEntries().get(0).getKey());
    assertArrayEquals(bytes("old-word"), group.getEntries().get(0).getOldValue().getValue());
    manager.shutdown();
  }

  @Test
  public void excludesAbiChangesFromCanonicalStateHistory() {
    SnapshotManager manager = new SnapshotManager("");
    Chainbase abi = new Chainbase(new SnapshotRoot(new MemoryDb("abi")));
    Chainbase code = new Chainbase(new SnapshotRoot(new MemoryDb("code")));
    manager.add(abi);
    manager.add(code);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });

    try (ISession block = manager.buildSession()) {
      abi.put(bytes("contract"), bytes("abi-metadata"));
      code.put(bytes("contract"), bytes("runtime-code"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    BlockReverseDiff diff = prepared(code);
    assertEquals(1, diff.getGroups().size());
    assertEquals("code", diff.getGroups().get(0).getDbName());
    assertFalse(diff.getGroups().stream().anyMatch(group -> "abi".equals(group.getDbName())));
    assertTrue(((SnapshotImpl) abi.getHead()).getPreparedArchiveBlock() == null);
    manager.shutdown();
  }

  @Test
  public void preservesPresentEmptyAndEmitsNoopBlockMetadata() {
    MemoryDb memoryDb = new MemoryDb("code");
    byte[] key = bytes("key");
    memoryDb.put(key, new byte[0]);
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });

    try (ISession block = manager.buildSession()) {
      database.put(key, bytes("value"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }
    BlockReverseDiff first = prepared(database);
    assertTrue(find(first.getGroups().get(0), key).getOldValue().isPresent());
    assertEquals(0,
        find(first.getGroups().get(0), key).getOldValue().getValue().length);

    try (ISession block = manager.buildSession()) {
      block.commit(BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L));
    }
    assertTrue(prepared(database).getGroups().isEmpty());
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
    duplicate.add(new Chainbase(new SnapshotRoot(new MemoryDb("code"))));
    duplicate.add(new Chainbase(new SnapshotRoot(new MemoryDb("code"))));
    IllegalStateException duplicateError = assertThrows(IllegalStateException.class,
        () -> duplicate.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { }));
    assertTrue(duplicateError.getMessage().contains("Duplicate"));
    duplicate.shutdown();
  }

  @Test
  public void classifiesEveryChainbaseRegisteredByTheApplication() {
    SnapshotManager applicationManager = context.getBean(SnapshotManager.class);
    ArchiveStoreScope.validate(applicationManager.getDbs());
    assertEquals(26, ArchiveStoreScope.getStateDatabases().size());
    assertFalse(ArchiveStoreScope.isStateDatabase("abi"));
    assertTrue(ArchiveStoreScope.isExcludedDatabase("abi"));
    assertTrue(ArchiveStoreScope.isClassified("abi"));
  }

  @Test
  public void matchesReferenceStateForRandomBlockOperations() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });

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

      BlockReverseDiff diff = prepared(database);
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
    AccountAssetArchiveProjector projector = new AccountAssetArchiveProjector();
    manager.installArchiveCollector(new SnapshotOldValueCollector(projector,
        accountKey -> assetStore.prefixQuery(accountKey), () -> true), diff -> { });

    try (ISession block = manager.buildSession()) {
      database.put(address, postAccount.toByteArray());
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    BlockReverseDiff diff = prepared(database);
    DbGroup accountGroup = diff.getGroups().stream()
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

    DbGroup assetGroup = diff.getGroups().stream()
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
    manager.installArchiveCollector(new SnapshotOldValueCollector(
        new AccountAssetArchiveProjector(),
        accountKey -> assetStore.prefixQuery(accountKey), () -> true), diff -> { });

    try (ISession block = manager.buildSession()) {
      database.put(address, postAccount.toByteArray());
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    BlockReverseDiff diff = prepared(database);
    assertFalse(diff.getGroups().stream()
        .anyMatch(group -> "account".equals(group.getDbName())));
    DbGroup assetGroup = diff.getGroups().stream()
        .filter(group -> "account-asset".equals(group.getDbName()))
        .findFirst().orElseThrow(AssertionError::new);
    assertArrayEquals(Longs.toByteArray(100L),
        find(assetGroup, assetKey).getOldValue().getValue());
    manager.shutdown();
  }

  @Test
  public void sharedAccountAssetProjectionUsesOneSnapshotAndStableForwardOrder() {
    byte[] address = bytes("shared-projection-address");
    byte[] firstKey = Bytes.concat(address, bytes("1000001"));
    byte[] secondKey = Bytes.concat(address, bytes("1000002"));
    Account oldAccount = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .setAssetOptimized(true)
        .build();
    Account postAccount = oldAccount.toBuilder()
        .putAssetV2("1000001", 80L)
        .putAssetV2("1000002", 0L)
        .build();
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    Map<WrappedByteArray, byte[]> persisted = new LinkedHashMap<>();
    persisted.put(WrappedByteArray.copyOf(secondKey), Longs.toByteArray(200L));
    persisted.put(WrappedByteArray.copyOf(firstKey), Longs.toByteArray(100L));
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(persisted);

    AccountAssetArchiveProjector.Projection projection =
        new AccountAssetArchiveProjector().project(address, oldAccount.toByteArray(),
            BlockChangeView.PostValue.present(postAccount.toByteArray()), false, persisted);

    verify(assetStore, never()).prefixQuery(any(byte[].class));
    assertEquals(2, projection.reverseAssets.size());
    assertEquals(2, projection.forwardAssets.size());
    assertArrayEquals(firstKey, projection.reverseAssets.get(0).getKey());
    assertArrayEquals(secondKey, projection.reverseAssets.get(1).getKey());
    assertArrayEquals(firstKey, projection.forwardAssets.get(0).getPhysicalRawKey());
    assertArrayEquals(secondKey, projection.forwardAssets.get(1).getPhysicalRawKey());
    assertArrayEquals(Longs.toByteArray(100L),
        projection.reverseAssets.get(0).getOldValue().getValue());
    assertArrayEquals(Longs.toByteArray(80L),
        projection.forwardAssets.get(0).getPostValue().getValue());
    assertFalse(projection.forwardAssets.get(1).getPostValue().isPresent());
    assertThrows(UnsupportedOperationException.class, projection.reverseAssets::clear);
    assertThrows(UnsupportedOperationException.class, projection.forwardAssets::clear);
    Account canonicalPost = parseAccount(projection.postAccount.getValue());
    assertTrue(canonicalPost.getAssetOptimized());
    assertTrue(canonicalPost.getAssetV2Map().isEmpty());
  }

  @Test
  public void pureProjectionRequiresAndCopiesExplicitOldPhysicalAssets() {
    byte[] address = bytes("pure-input-address");
    byte[] assetKey = Bytes.concat(address, bytes("1000009"));
    Account optimized = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .setAssetOptimized(true)
        .build();
    Map<WrappedByteArray, byte[]> oldPhysicalAssets = new HashMap<>();
    oldPhysicalAssets.put(WrappedByteArray.copyOf(assetKey), Longs.toByteArray(900L));
    AccountAssetArchiveProjector projector = new AccountAssetArchiveProjector();

    assertThrows(ArchivePersistenceException.class,
        () -> projector.project(address, optimized.toByteArray(),
            BlockChangeView.PostValue.absent(), true, null));
    Map<WrappedByteArray, byte[]> wrongAccountAssets = new HashMap<>();
    wrongAccountAssets.put(WrappedByteArray.copyOf(bytes("another-account-token")),
        Longs.toByteArray(1L));
    assertThrows(ArchivePersistenceException.class,
        () -> projector.project(address, optimized.toByteArray(),
            BlockChangeView.PostValue.absent(), true, wrongAccountAssets));

    AccountAssetArchiveProjector.Projection projection = projector.project(address,
        optimized.toByteArray(), BlockChangeView.PostValue.absent(), true,
        oldPhysicalAssets);
    oldPhysicalAssets.clear();

    assertEquals(1, projection.reverseAssets.size());
    assertArrayEquals(assetKey, projection.reverseAssets.get(0).getKey());
    assertArrayEquals(Longs.toByteArray(900L),
        projection.reverseAssets.get(0).getOldValue().getValue());
    assertFalse(projection.forwardAssets.get(0).getPostValue().isPresent());
  }

  @Test
  public void targetAssetOptimizationOverridesLegacySupplierAndCoversDelete() {
    byte[] address = bytes("target-activation-address");
    byte[] assetKey = Bytes.concat(address, bytes("1000003"));
    Account rawPost = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .putAssetV2("1000003", 300L)
        .build();
    AccountAssetArchiveProjector projector = new AccountAssetArchiveProjector();

    AccountAssetArchiveProjector.Projection enabled = projector.project(address, null,
        BlockChangeView.PostValue.present(rawPost.toByteArray()), true, Collections.emptyMap());
    assertTrue(parseAccount(enabled.postAccount.getValue()).getAssetOptimized());
    assertEquals(1, enabled.forwardAssets.size());
    assertArrayEquals(assetKey, enabled.forwardAssets.get(0).getPhysicalRawKey());
    assertArrayEquals(Longs.toByteArray(300L),
        enabled.forwardAssets.get(0).getPostValue().getValue());

    AccountAssetArchiveProjector.Projection disabled =
        new AccountAssetArchiveProjector().project(address, null,
            BlockChangeView.PostValue.present(rawPost.toByteArray()), false,
            Collections.emptyMap());
    assertArrayEquals(rawPost.toByteArray(), disabled.postAccount.getValue());
    assertTrue(disabled.forwardAssets.isEmpty());

    Account optimizedOld = rawPost.toBuilder()
        .setAssetOptimized(true)
        .clearAssetV2()
        .build();
    Map<WrappedByteArray, byte[]> persisted = new HashMap<>();
    persisted.put(WrappedByteArray.copyOf(assetKey), Longs.toByteArray(300L));
    AccountAssetArchiveProjector.Projection deleted = projector.project(address,
        optimizedOld.toByteArray(), BlockChangeView.PostValue.absent(), true, persisted);
    assertFalse(deleted.postAccount.isPresent());
    assertEquals(1, deleted.reverseAssets.size());
    assertEquals(1, deleted.forwardAssets.size());
    assertFalse(deleted.forwardAssets.get(0).getPostValue().isPresent());
  }

  @Test
  public void sharedProjectionUsesOuterFinalViewAfterNestedMergeAndRevoke() {
    byte[] address = bytes("nested-account-address");
    byte[] assetKey = Bytes.concat(address, bytes("1000004"));
    Account oldAccount = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .putAssetV2("1000004", 100L)
        .build();
    MemoryDb memoryDb = new MemoryDb("account");
    memoryDb.put(address, oldAccount.toByteArray());
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);

    try (ISession block = manager.buildSession()) {
      database.put(address, oldAccount.toBuilder().putAssetV2("1000004", 90L)
          .build().toByteArray());
      try (ISession merged = manager.buildSession()) {
        database.put(address, oldAccount.toBuilder().putAssetV2("1000004", 80L)
            .build().toByteArray());
        merged.merge();
      }
      try (ISession revoked = manager.buildSession()) {
        database.put(address, oldAccount.toBuilder().putAssetV2("1000004", 70L)
            .build().toByteArray());
      }
      BlockChangeView view = BlockChangeView.capture(meta,
          Collections.singletonList(database));
      BlockChangeView.Change finalChange = view.getDatabases().get(0).getChanges().get(0);
      AccountAssetArchiveProjector.Projection projection =
          new AccountAssetArchiveProjector().project(address, oldAccount.toByteArray(),
              finalChange.getPostValue(), true, Collections.emptyMap());
      assertEquals(1, projection.forwardAssets.size());
      AssetMutation mutation = projection.forwardAssets.get(0);
      assertArrayEquals(assetKey, mutation.getPhysicalRawKey());
      assertArrayEquals(Longs.toByteArray(80L), mutation.getPostValue().getValue());
    }
    manager.shutdown();
  }

  @Test
  public void sharedProjectionMatchesSnapshotRootBytesWithProposalSixtySix() {
    byte[] address = new byte[21];
    address[0] = 65;
    address[20] = 79;
    String token = "1000005";
    byte[] assetKey = Bytes.concat(address, bytes(token));
    Account rawPost = Account.newBuilder()
        .setAddress(ByteString.copyFrom(address))
        .putAssetV2(token, 500L)
        .build();
    AccountAssetArchiveProjector.Projection projection =
        new AccountAssetArchiveProjector().project(address, null,
            BlockChangeView.PostValue.present(rawPost.toByteArray()), true,
            Collections.emptyMap());

    chainBaseManager.getDynamicPropertiesStore().setAllowAccountAssetOptimization(0);
    chainBaseManager.getDynamicPropertiesStore().setAllowAssetOptimization(1);
    MemoryDb accountRootDb = new MemoryDb("account");
    SnapshotRoot accountRoot = new SnapshotRoot(accountRootDb);
    accountRoot.put(address, rawPost.toByteArray());

    assertArrayEquals(projection.postAccount.getValue(), accountRootDb.get(address));
    assertArrayEquals(Longs.toByteArray(500L),
        chainBaseManager.getAccountAssetStore().get(assetKey));
    assertEquals(1, projection.forwardAssets.size());
    assertArrayEquals(assetKey, projection.forwardAssets.get(0).getPhysicalRawKey());
    assertArrayEquals(Longs.toByteArray(500L),
        projection.forwardAssets.get(0).getPostValue().getValue());
  }

  @Test
  public void archiveDurabilityFailurePreventsCheckpointAndRefresh() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.setUnChecked(false);
    CheckTmpStore checkpoint = mock(CheckTmpStore.class);
    manager.setCheckTmpStore(checkpoint);
    DurableBlockReverseDiffSink sink = mock(DurableBlockReverseDiffSink.class);
    manager.installArchiveCollector(new SnapshotOldValueCollector(), sink);
    try (ISession block = manager.buildSession()) {
      database.put(bytes("key"), bytes("value"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }
    setFlushCount(manager, 1);
    doThrow(new ArchivePersistenceException("injected"))
        .when(sink).awaitCommitted(1L);

    assertThrows(TronError.class, manager::flush);
    verify(sink).acceptAll(Collections.singletonList(prepared(database)));
    verify(sink).awaitCommitted(1L);
    verify(checkpoint, never()).updateByBatch(any(Map.class));
    manager.shutdown();
  }

  @Test
  public void flushRetriesDurabilityAndReceiptWithoutResubmittingHistory() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.setUnChecked(false);
    CheckTmpStore checkpoint = mock(CheckTmpStore.class);
    DbSourceInter<byte[]> checkpointDb = mock(DbSourceInter.class);
    when(checkpointDb.iterator()).thenReturn(Collections.emptyIterator());
    when(checkpoint.getDbSource()).thenReturn(checkpointDb);
    manager.setCheckTmpStore(checkpoint);
    ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
        temporaryFolder.newFolder("flush-receipt-retry").toPath(), 4096,
        ArchiveStoreScope.getStateDatabases());
    FailOnceReceiptSink sink = new FailOnceReceiptSink(writer);
    manager.installArchiveCollector(new SnapshotOldValueCollector(), sink);
    AtomicReference<AccountAssetBlockProjectionBridge.PreparedBlockProjection> prepared =
        new AtomicReference<>();
    manager.installArchiveProjectionPreparer(view -> {
      AccountAssetBlockProjectionBridge.PreparedBlockProjection projection =
          sealReadyProjection(view.getMeta(), view);
      prepared.set(projection);
      return projection;
    });

    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    commitBlock(manager, database, meta, "key-1");
    setFlushCount(manager, 1);

    assertThrows(TronError.class, manager::flush);
    assertTrue(manager.hasPendingArchiveForwardFlush());
    verify(checkpoint, never()).updateByBatch(any(Map.class));

    assertThrows(TronError.class, manager::flush);
    assertTrue(manager.hasPendingArchiveForwardFlush());
    verify(checkpoint, never()).updateByBatch(any(Map.class));

    manager.flush();

    assertEquals(1, sink.acceptAllCalls);
    assertEquals(3, sink.awaitCalls);
    assertEquals(2, sink.receiptCalls);
    verify(prepared.get()).completeSeal();
    assertEquals(1, manager.claimArchiveForwardFlushPayloads().size());
    assertFalse(manager.hasPendingArchiveForwardFlush());
    manager.shutdown();
    writer.close();
  }

  @Test
  public void fastPopDiscardsPreparedPayloadWithoutRevertingDurableHistory() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    BlockReverseDiffSink sink = mock(BlockReverseDiffSink.class);
    manager.installArchiveCollector(new SnapshotOldValueCollector(), sink);

    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    try (ISession block = manager.buildSession()) {
      database.put(bytes("key"), bytes("value"));
      block.commit(meta);
    }

    assertEquals(meta, prepared(database).getMeta());
    manager.fastPop();

    assertEquals(0, manager.size());
    assertTrue(database.getHead() instanceof SnapshotRoot);
    verify(sink, never()).accept(any(BlockReverseDiff.class));
    verify(sink, never()).revert(any(BlockSnapshotMeta.class));
    manager.shutdown();
  }

  @Test
  public void collectorFailureLeavesSessionOwnedSoCloseRevokesLayer() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    OldValueCollector collector = mock(OldValueCollector.class);
    BlockReverseDiffSink sink = mock(BlockReverseDiffSink.class);
    when(collector.collect(any(BlockChangeView.class)))
        .thenThrow(new IllegalStateException("injected collector failure"));
    manager.installArchiveCollector(collector, sink);

    assertThrows(IllegalStateException.class, () -> {
      try (ISession block = manager.buildSession()) {
        database.put(bytes("key"), bytes("value"));
        block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
      }
    });

    assertEquals(0, manager.getActiveSession());
    assertEquals(0, manager.size());
    assertTrue(database.getHead() instanceof SnapshotRoot);
    verify(sink, never()).accept(any(BlockReverseDiff.class));
    manager.shutdown();
  }

  @Test
  public void sharedProjectionPreparerIsDisabledUntilExplicitlyInstalled() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    OldValueCollector collector = mock(OldValueCollector.class);
    BlockReverseDiff reverse = mock(BlockReverseDiff.class);
    when(collector.collect(any(BlockChangeView.class))).thenReturn(reverse);
    manager.installArchiveCollector(collector, diff -> { });

    try (ISession block = manager.buildSession()) {
      database.put(bytes("key"), bytes("value"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }

    verify(collector).collect(any(BlockChangeView.class));
    assertEquals(0, manager.getArchiveForwardPayloadOwnerCount());
    manager.shutdown();
  }

  @Test
  public void sharedProjectionPreparerOwnsOneCapturedViewAndReversePayload() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    OldValueCollector legacy = mock(OldValueCollector.class);
    manager.installArchiveCollector(legacy, diff -> { });
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockReverseDiff reverse = mock(BlockReverseDiff.class);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection projection =
        preparedProjection(meta, reverse);
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<BlockChangeView> captured = new AtomicReference<>();
    manager.installArchiveProjectionPreparer(view -> {
      calls.incrementAndGet();
      captured.set(view);
      return projection;
    });

    try (ISession block = manager.buildSession()) {
      database.put(bytes("key"), bytes("value"));
      block.commit(meta);
    }

    assertEquals(1, calls.get());
    assertEquals(meta, captured.get().getMeta());
    assertEquals(reverse, prepared(database));
    assertTrue(manager.hasArchiveForwardPayloadOwner(meta));
    verify(legacy, never()).collect(any(BlockChangeView.class));
    manager.fastPop();
    verify(projection).abort();
    manager.shutdown();
  }

  @Test
  public void projectionPrepareFailureLeavesSessionOwnedAndRegistryEmpty() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    manager.installArchiveProjectionPreparer(view -> {
      throw new ArchivePersistenceException("injected prepare failure");
    });

    assertThrows(ArchivePersistenceException.class, () -> {
      try (ISession block = manager.buildSession()) {
        database.put(bytes("key"), bytes("value"));
        block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
      }
    });

    assertEquals(0, manager.getActiveSession());
    assertEquals(0, manager.size());
    assertEquals(0, manager.getArchiveForwardPayloadOwnerCount());
    assertTrue(database.getHead() instanceof SnapshotRoot);
    manager.shutdown();
  }

  @Test
  public void projectionAttachFailureAbortsUnownedPayloadAndRevokesLayer() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta target = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection mismatched =
        preparedProjection(BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L),
            mock(BlockReverseDiff.class));
    manager.installArchiveProjectionPreparer(view -> mismatched);

    assertThrows(ArchivePersistenceException.class, () -> {
      try (ISession block = manager.buildSession()) {
        database.put(bytes("key"), bytes("value"));
        block.commit(target);
      }
    });

    verify(mismatched).abort();
    assertEquals(0, manager.getActiveSession());
    assertEquals(0, manager.size());
    assertEquals(0, manager.getArchiveForwardPayloadOwnerCount());
    assertTrue(database.getHead() instanceof SnapshotRoot);
    manager.shutdown();
  }

  @Test
  public void shortReorgDiscardsOnlySameMetaUnfrozenOwners() {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta firstMeta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockSnapshotMeta secondMeta = BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection first =
        preparedProjection(firstMeta, mock(BlockReverseDiff.class));
    AccountAssetBlockProjectionBridge.PreparedBlockProjection second =
        preparedProjection(secondMeta, mock(BlockReverseDiff.class));
    manager.installArchiveProjectionPreparer(
        view -> firstMeta.equals(view.getMeta()) ? first : second);

    try (ISession block = manager.buildSession()) {
      database.put(bytes("key-1"), bytes("value-1"));
      block.commit(firstMeta);
    }
    try (ISession block = manager.buildSession()) {
      database.put(bytes("key-2"), bytes("value-2"));
      block.commit(secondMeta);
    }

    assertEquals(2, manager.getArchiveForwardPayloadOwnerCount());
    manager.fastPop();
    verify(second).abort();
    verify(first, never()).abort();
    assertTrue(manager.hasArchiveForwardPayloadOwner(firstMeta));
    assertFalse(manager.hasArchiveForwardPayloadOwner(secondMeta));
    manager.fastPop();
    verify(first).abort();
    assertEquals(0, manager.getArchiveForwardPayloadOwnerCount());
    manager.shutdown();
  }

  @Test
  public void oldestForwardFlushRangeFreezesOnceAndExcludesFastPop() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta firstMeta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockSnapshotMeta secondMeta = BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection first =
        preparedProjection(firstMeta, mock(BlockReverseDiff.class));
    AccountAssetBlockProjectionBridge.PreparedBlockProjection second =
        preparedProjection(secondMeta, mock(BlockReverseDiff.class));
    manager.installArchiveProjectionPreparer(
        view -> firstMeta.equals(view.getMeta()) ? first : second);
    commitBlock(manager, database, firstMeta, "key-1");
    commitBlock(manager, database, secondMeta, "key-2");
    setFlushCount(manager, 1);

    FrozenBatch pending = manager.freezeArchiveForwardFlushRange();

    assertEquals(Collections.singletonList(firstMeta), pending.getExpectedMetas());
    assertSame(pending, manager.freezeArchiveForwardFlushRange());
    assertTrue(manager.hasPendingArchiveForwardFlush());
    assertEquals(1, manager.getArchiveForwardPayloadOwnerCount());
    manager.fastPop();
    verify(second).abort();
    verify(first, never()).abort();
    assertThrows(IllegalStateException.class, manager::fastPop);
    assertEquals(1, manager.size());

    manager.shutdown();
    verify(first).abort();
  }

  @Test
  public void forwardFlushRegistryMismatchFailsBeforeOwnershipTransfer() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta firstMeta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockSnapshotMeta secondMeta = BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection first =
        preparedProjection(firstMeta, mock(BlockReverseDiff.class));
    AccountAssetBlockProjectionBridge.PreparedBlockProjection second =
        preparedProjection(secondMeta, mock(BlockReverseDiff.class));
    manager.installArchiveProjectionPreparer(
        view -> firstMeta.equals(view.getMeta()) ? first : second);
    commitBlock(manager, database, firstMeta, "key-1");
    commitBlock(manager, database, secondMeta, "key-2");
    setFlushCount(manager, 1);
    Map<BlockSnapshotMeta, AccountAssetPreparedBlockPayloadOwner> owners = forwardOwners(manager);

    AccountAssetPreparedBlockPayloadOwner removed = owners.remove(firstMeta);
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    assertEquals(1, owners.size());
    assertTrue(removed.isAttachedTo(firstMeta));
    owners.put(firstMeta, removed);

    BlockSnapshotMeta extraMeta = BlockSnapshotMeta.forBlock(3, hash(3), hash(2), 3L);
    AccountAssetPreparedBlockPayloadOwner extra =
        new AccountAssetPreparedBlockPayloadOwner(extraMeta);
    extra.attach(preparedProjection(extraMeta, mock(BlockReverseDiff.class)));
    owners.put(extraMeta, extra);
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    assertEquals(3, owners.size());
    assertTrue(removed.isAttachedTo(firstMeta));
    assertTrue(owners.get(secondMeta).isAttachedTo(secondMeta));

    manager.shutdown();
  }

  @Test
  public void forwardFlushRejectsTopologyGapAndUnattachedOwnerWithoutPartialTransfer()
      throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta firstMeta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockSnapshotMeta secondMeta = BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection first =
        preparedProjection(firstMeta, mock(BlockReverseDiff.class));
    AccountAssetBlockProjectionBridge.PreparedBlockProjection second =
        preparedProjection(secondMeta, mock(BlockReverseDiff.class));
    manager.installArchiveProjectionPreparer(
        view -> firstMeta.equals(view.getMeta()) ? first : second);
    commitBlock(manager, database, firstMeta, "key-1");
    commitBlock(manager, database, secondMeta, "key-2");
    setFlushCount(manager, 1);

    SnapshotImpl newest = (SnapshotImpl) database.getHead();
    setBlockMeta(newest, firstMeta);
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    setBlockMeta(newest, BlockSnapshotMeta.forBlock(3, hash(3), hash(2), 3L));
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    setBlockMeta(newest, secondMeta);

    Map<BlockSnapshotMeta, AccountAssetPreparedBlockPayloadOwner> owners = forwardOwners(manager);
    owners.get(secondMeta).discard();
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    assertEquals(2, owners.size());
    assertTrue(owners.get(firstMeta).isAttachedTo(firstMeta));
    verify(first, never()).abort();

    manager.shutdown();
    verify(first).abort();
    verify(second).abort();
  }

  @Test
  public void pendingForwardFlushSealRetriesAndClaimsOrderedPayloadsOnce() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta firstMeta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    BlockSnapshotMeta secondMeta = BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L);
    BlockChangeView firstView = mock(BlockChangeView.class);
    BlockChangeView secondView = mock(BlockChangeView.class);
    when(firstView.getMeta()).thenReturn(firstMeta);
    when(secondView.getMeta()).thenReturn(secondMeta);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection first =
        sealReadyProjection(firstMeta, firstView);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection second =
        sealReadyProjection(secondMeta, secondView);
    manager.installArchiveProjectionPreparer(
        captured -> firstMeta.equals(captured.getMeta()) ? first : second);
    commitBlock(manager, database, firstMeta, "key-1");
    commitBlock(manager, database, secondMeta, "key-2");
    setFlushCount(manager, 2);
    FrozenBatch frozen = manager.freezeArchiveForwardFlushRange();

    assertThrows(ArchivePersistenceException.class,
        () -> manager.sealPendingArchiveForwardFlush(
            Arrays.asList(marker(firstMeta), marker(BlockSnapshotMeta.forBlock(
                3, hash(3), hash(2), 3L)))));
    assertSame(frozen, manager.freezeArchiveForwardFlushRange());
    verify(first, never()).completeSeal();
    verify(second, never()).completeSeal();

    manager.sealPendingArchiveForwardFlush(Arrays.asList(marker(firstMeta), marker(secondMeta)));

    verify(first).completeSeal();
    verify(second).completeSeal();
    assertTrue(manager.hasPendingArchiveForwardFlush());
    assertThrows(IllegalStateException.class, manager::freezeArchiveForwardFlushRange);
    assertThrows(IllegalStateException.class, manager::fastPop);
    List<ArchiveBlockForwardPayload> claimed =
        manager.claimArchiveForwardFlushPayloads();
    assertEquals(2, claimed.size());
    assertEquals(firstMeta, claimed.get(0).getMeta());
    assertSame(firstView, claimed.get(0).getView());
    assertEquals(secondMeta, claimed.get(1).getMeta());
    assertSame(secondView, claimed.get(1).getView());
    assertFalse(manager.hasPendingArchiveForwardFlush());
    assertThrows(IllegalStateException.class, manager::claimArchiveForwardFlushPayloads);
    manager.shutdown();
    verify(first, never()).abort();
    verify(second, never()).abort();
  }

  @Test
  public void durableReceiptFailureKeepsFrozenSlotAndShutdownReleasesSealedSlot()
      throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.installArchiveCollector(new SnapshotOldValueCollector(), diff -> { });
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L);
    HistoryCommitMarker committed = marker(meta);
    BlockChangeView view = mock(BlockChangeView.class);
    when(view.getMeta()).thenReturn(meta);
    AccountAssetBlockProjectionBridge.PreparedBlockProjection projection =
        sealReadyProjection(meta, view);
    manager.installArchiveProjectionPreparer(captured -> projection);
    commitBlock(manager, database, meta, "key-1");
    setFlushCount(manager, 1);
    FrozenBatch frozen = manager.freezeArchiveForwardFlushRange();
    boolean[] substitute = {true};
    DurableHistoryMarkerRangeReceipt receipt = new DurableHistoryMarkerRangeReceipt(
        new DurableHistoryMarkerRangeReceipt.Source() {
          @Override
          public HistoryCommitMarker marker(long epoch) {
            return substitute[0]
                ? SnapshotOldValueCollectorTest.marker(BlockSnapshotMeta.forBlock(
                    2, hash(2), hash(1), 2L)) : committed;
          }

          @Override
          public BlockReverseDiff readCommitted(long epoch) {
            return new BlockReverseDiff(meta, Collections.emptyList());
          }
        }, 1);

    assertThrows(ArchivePersistenceException.class,
        () -> manager.sealPendingArchiveForwardFlush(receipt));
    assertSame(frozen, manager.freezeArchiveForwardFlushRange());
    substitute[0] = false;
    manager.sealPendingArchiveForwardFlush(receipt);
    assertTrue(manager.hasPendingArchiveForwardFlush());

    manager.shutdown();

    assertFalse(manager.hasPendingArchiveForwardFlush());
    assertThrows(IllegalStateException.class, manager::claimArchiveForwardFlushPayloads);
    verify(projection).completeSeal();
    verify(projection, never()).abort();
  }

  @Test
  public void flushPublishesOnlyTheNonRevertibleRange() throws Exception {
    MemoryDb memoryDb = new MemoryDb("code");
    SnapshotManager manager = new SnapshotManager("");
    Chainbase database = new Chainbase(new SnapshotRoot(memoryDb));
    manager.add(database);
    manager.enable();
    manager.setUnChecked(false);
    CheckTmpStore checkpoint = mock(CheckTmpStore.class);
    DbSourceInter<byte[]> checkpointDb = mock(DbSourceInter.class);
    when(checkpointDb.iterator()).thenReturn(Collections.emptyIterator());
    when(checkpoint.getDbSource()).thenReturn(checkpointDb);
    manager.setCheckTmpStore(checkpoint);
    DurableBlockReverseDiffSink sink = mock(DurableBlockReverseDiffSink.class);
    manager.installArchiveCollector(new SnapshotOldValueCollector(), sink);

    try (ISession block = manager.buildSession()) {
      database.put(bytes("key-1"), bytes("value-1"));
      block.commit(BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1L));
    }
    BlockReverseDiff first = prepared(database);
    try (ISession block = manager.buildSession()) {
      database.put(bytes("key-2"), bytes("value-2"));
      block.commit(BlockSnapshotMeta.forBlock(2, hash(2), hash(1), 2L));
    }
    BlockReverseDiff second = prepared(database);
    setFlushCount(manager, 1);

    manager.flush();

    verify(sink).acceptAll(Collections.singletonList(first));
    verify(sink, never()).acceptAll(Collections.singletonList(second));
    verify(sink).awaitCommitted(1L);
    verify(sink).releaseThrough(1L);
    manager.shutdown();
  }

  private static Entry find(DbGroup group, byte[] key) {
    return group.getEntries().stream()
        .filter(entry -> Arrays.equals(entry.getKey(), key))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static Account parseAccount(byte[] value) {
    try {
      return Account.parseFrom(value);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }

  private static BlockReverseDiff prepared(Chainbase database) {
    return ((SnapshotImpl) database.getHead()).getPreparedArchiveBlock();
  }

  private static void commitBlock(SnapshotManager manager, Chainbase database,
      BlockSnapshotMeta meta, String key) {
    try (ISession block = manager.buildSession()) {
      database.put(bytes(key), bytes("value-" + key));
      block.commit(meta);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<BlockSnapshotMeta, AccountAssetPreparedBlockPayloadOwner> forwardOwners(
      SnapshotManager manager) throws Exception {
    java.lang.reflect.Field field = SnapshotManager.class.getDeclaredField(
        "archiveForwardPayloadOwners");
    field.setAccessible(true);
    return (Map<BlockSnapshotMeta, AccountAssetPreparedBlockPayloadOwner>) field.get(manager);
  }

  private static void setBlockMeta(SnapshotImpl snapshot, BlockSnapshotMeta meta)
      throws Exception {
    java.lang.reflect.Field field = SnapshotImpl.class.getDeclaredField("blockSnapshotMeta");
    field.setAccessible(true);
    field.set(snapshot, meta);
  }

  private static AccountAssetBlockProjectionBridge.PreparedBlockProjection preparedProjection(
      BlockSnapshotMeta meta, BlockReverseDiff reverse) {
    AccountAssetBlockProjectionBridge.PreparedBlockProjection projection =
        mock(AccountAssetBlockProjectionBridge.PreparedBlockProjection.class);
    when(projection.getMeta()).thenReturn(meta);
    when(projection.getReverseDiff()).thenReturn(reverse);
    return projection;
  }

  private static AccountAssetBlockProjectionBridge.PreparedBlockProjection sealReadyProjection(
      BlockSnapshotMeta meta, BlockChangeView view) {
    AccountAssetBlockProjectionBridge.PreparedBlockProjection projection =
        preparedProjection(meta, new BlockReverseDiff(meta, Collections.emptyList()));
    when(projection.previewSealPayload(any(HistoryCommitMarker.class))).thenAnswer(invocation -> {
      HistoryCommitMarker target = invocation.getArgument(0);
      if (!meta.equals(target.getMeta())) {
        throw new ArchivePersistenceException("Prepared block projection target mismatch");
      }
      return new ArchiveBlockForwardPayload(target, view,
          new AccountAssetForwardMutationManifest(target, Collections.emptyList()));
    });
    return projection;
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta) {
    int epoch = (int) meta.getEpoch();
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100L, 100, epoch, hash(epoch + 20)),
        new HistoryIndexLocation(epoch * 50L, 50, hash(epoch + 30)),
        Arrays.copyOf(hash(epoch + 40), 16), participants);
  }

  private static void setFlushCount(SnapshotManager manager, int count) throws Exception {
    java.lang.reflect.Field flushCount = SnapshotManager.class.getDeclaredField("flushCount");
    flushCount.setAccessible(true);
    flushCount.setInt(manager, count);
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

  private static final class FailOnceReceiptSink implements DurableBlockReverseDiffSink {
    private final ArchiveHistoryWriter writer;
    private int acceptAllCalls;
    private int awaitCalls;
    private int receiptCalls;

    private FailOnceReceiptSink(ArchiveHistoryWriter writer) {
      this.writer = writer;
    }

    @Override
    public void accept(BlockReverseDiff diff) {
      writer.accept(diff);
    }

    @Override
    public void acceptAll(List<BlockReverseDiff> diffs) {
      acceptAllCalls++;
      writer.acceptAll(diffs);
    }

    @Override
    public void revert(BlockSnapshotMeta meta) {
      writer.revert(meta);
    }

    @Override
    public void awaitCommitted(long epoch) {
      awaitCalls++;
      if (awaitCalls == 1) {
        throw new ArchivePersistenceException("injected durable wait failure");
      }
      writer.awaitCommitted(epoch);
    }

    @Override
    public DurableHistoryMarkerRangeReceipt createMarkerRangeReceipt(int maxMarkers) {
      receiptCalls++;
      if (receiptCalls == 1) {
        return new DurableHistoryMarkerRangeReceipt(
            new DurableHistoryMarkerRangeReceipt.Source() {
              @Override
              public HistoryCommitMarker marker(long epoch) {
                return null;
              }

              @Override
              public BlockReverseDiff readCommitted(long epoch) {
                return writer.readCommitted(epoch);
              }
            }, maxMarkers);
      }
      return writer.createMarkerRangeReceipt(maxMarkers);
    }

    @Override
    public void releaseThrough(long epoch) {
      writer.releaseThrough(epoch);
    }
  }

  private static final class MemoryDb implements DB<byte[], byte[]>, Flusher {
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
    public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
      batch.forEach((key, value) -> {
        if (value == null || value.getBytes() == null) {
          values.remove(key);
        } else {
          values.put(WrappedByteArray.copyOf(key.getBytes()), value.getBytes());
        }
      });
    }

    @Override
    public void reset() {
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
