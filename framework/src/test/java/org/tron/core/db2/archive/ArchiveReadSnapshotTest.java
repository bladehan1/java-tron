package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.runtime.vm.DataWord;
import org.tron.common.utils.ByteArray;
import org.tron.core.actuator.VMActuator;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.db.TransactionContext;
import org.tron.core.db.TransactionContext.ExecutionMode;
import org.tron.core.db2.archive.ArchiveReadContext.HistoricalStore;
import org.tron.core.db2.archive.ArchiveReadContext.StoreAdapter;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.core.vm.HistoricalCapabilityException;
import org.tron.core.vm.HistoricalExecutionGuard;
import org.tron.core.vm.OperationActions;
import org.tron.core.vm.PrecompiledContracts;
import org.tron.core.vm.config.ConfigLoader;
import org.tron.core.vm.config.VMConfig;
import org.tron.core.vm.program.Program;
import org.tron.core.vm.repository.HistoricalRepositoryProvider;
import org.tron.core.vm.repository.Repository;
import org.tron.core.vm.repository.RepositoryImpl;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.contract.SmartContractOuterClass.CreateSmartContract;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;
import org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract;

public class ArchiveReadSnapshotTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void readsPointAndRangeFromOnePinnedPhysicalKeyGeneration() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("snapshot").toPath())) {
      fixture.append(diff(1, entry("p/c", OldValue.absent())));
      fixture.append(diff(2,
          entry("p/a", "a1"), entry("p/b", "b1"), entry("p/e", OldValue.absent()),
          entry("p/f", OldValue.present(new byte[0]))));
      fixture.append(diff(3, entry("p/a", "a2")));
      fixture.sync();

      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put("p/a", bytes("a3"));
      latestValues.put("p/c", bytes("c1"));
      latestValues.put("p/e", bytes("e2"));
      InMemoryLatest latest = new InMemoryLatest(3, hash(3), latestValues);
      try (ArchiveReadSnapshot snapshot = fixture.snapshot(1, latest)) {
        assertValue(snapshot.get("account", bytes("p/a")), "a1");
        assertValue(snapshot.get("account", bytes("p/b")), "b1");
        assertValue(snapshot.get("account", bytes("p/c")), "c1");
        assertFalse(snapshot.get("account", bytes("p/e")).isPresent());
        assertArrayEquals(new byte[0],
            snapshot.get("account", bytes("p/f")).getValue());

        List<HistoricalRangeOverlay.Entry> range = snapshot.range("account",
            KeyRange.prefix(bytes("p/")), new Limits(10, 10, 10));
        assertEquals(Arrays.asList("p/a", "p/b", "p/c", "p/f"), keys(range));
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void rejectsMixedGenerationOrCoverageBeforeReading() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("identity").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      ServingKeyIndexGeneration serving = fixture.serving();
      CommittedHistoryReader history = fixture.history();
      InMemoryLatest wrongHash = new InMemoryLatest(1, hash(99), Collections.emptyMap());

      assertThrows(IllegalArgumentException.class, () -> ArchiveReadSnapshot.pin(
          0, 1, hash(1), serving, wrongHash, history));
      assertTrue(wrongHash.closed);

      CommittedHistoryReader secondHistory = fixture.history();
      InMemoryLatest wrongCoverage = new InMemoryLatest(0, hash(0), Collections.emptyMap());
      assertThrows(IllegalArgumentException.class, () -> ArchiveReadSnapshot.pin(
          0, 0, hash(0), serving, wrongCoverage, secondHistory));
      assertTrue(wrongCoverage.closed);
    }
  }

  @Test
  public void rejectsWrongKeyBetweenAuthoritativeIndexAndBody() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("wrong-key").toPath())) {
      BlockReverseDiff body = diff(1, entry("actual", "old"));
      HistoryLocation location = fixture.bodies.append(body);
      HistoryIndexRecord wrongIndex = new HistoryIndexRecord(body.getMeta(), location,
          Collections.singletonList(new KeyGroup("account",
              Collections.singletonList(bytes("indexed")))));
      HistoryIndexLocation indexLocation = fixture.index.append(wrongIndex);
      fixture.markers.add(marker(body.getMeta(), location, indexLocation));
      fixture.sync();

      try (ArchiveReadSnapshot snapshot = fixture.snapshot(0,
          new InMemoryLatest(1, hash(1), Collections.emptyMap()))) {
        assertThrows(ArchivePersistenceException.class,
            () -> snapshot.get("account", bytes("indexed")));
      }
    }
  }

  @Test
  public void rejectsDigestMismatchAndMissingSegment() throws Exception {
    assertUnreadableBody("digest", location -> new HistoryLocation(location.getSegmentId(),
        location.getOffset(), location.getRecordLength(), location.getBodyChecksum(), hash(99)),
        IllegalArgumentException.class);
    assertUnreadableBody("missing", location -> new HistoryLocation(99, location.getOffset(),
        location.getRecordLength(), location.getBodyChecksum(), location.getBodyDigest()),
        IOException.class);
  }

  @Test
  public void releasesHistoryPinWhenCommittedPrefixValidationFails() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("failed-history-pin").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      AtomicBoolean released = new AtomicBoolean();
      ServingKeyIndexGeneration.AuthoritativeIndexReader failingIndex = location -> {
        throw new IOException("injected index failure");
      };

      assertThrows(IOException.class, () -> new CommittedHistoryReader(0, hash(0),
          fixture.markers, failingIndex, fixture.bodies::read, () -> released.set(true)));
      assertTrue(released.get());
    }
  }

  @Test
  public void bindsEveryVersionedPhysicalStoreToOneRequestSnapshot() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("read-context").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put("key", bytes("new"));
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), latestValues);
      AdapterSet adapterSet = rawAdapters();

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapterSet.adapters)) {
        HistoricalStore<byte[]> account = context.store(adapterSet.account);
        assertArrayEquals(bytes("old"), account.get(bytes("key")).orElseThrow(AssertionError::new));
        assertFalse(account.has(bytes("missing")));
        assertEquals(0, context.getTargetBlock());
        assertEquals(1, context.getPinnedBlock());
        assertTrue(context.getAdapterDbNames().contains("account-asset"));
        assertFalse(context.getAdapterDbNames().contains("accountTrie"));
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void contextResolvesHistoricalAccountAssetBeforePinnedHead() throws Exception {
    byte[] address = address(41);
    String tokenId = "1000001";
    byte[] account = account(address, false, tokenId, 17L);
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, tokenId);
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-account-asset-context").toPath())) {
      fixture.append(diff(1,
          new DbGroup("properties", Collections.singletonList(new Entry(
              HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey(),
              OldValue.present(ByteArray.fromLong(0L))))),
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(account)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.absent())))));
      fixture.sync();
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), Collections.emptyMap());

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), rawAdapters().adapters)) {
        HistoricalAccountAssetBalanceResolver.Result result =
            context.resolveAccountAsset(address, tokenId);
        assertEquals(0L, result.getBlockNumber());
        assertEquals(Phase.P66_OFF, result.getPhase());
        assertEquals(17L, result.getBalance());
        assertArrayEquals(account, result.getAccountValue());
        byte[] callerCopy = result.getAccountValue();
        callerCopy[0] ^= 1;
        assertArrayEquals(account, result.getAccountValue());

        HistoricalAccountAssetPrefixResolver.Result all = context.resolveAccountAssets(address,
            new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000));
        assertEquals(1, all.getBalances().size());
        assertEquals(tokenId, all.getBalances().get(0).getTokenId());
        assertEquals(17L, all.getBalances().get(0).getBalance());
      }
      assertTrue(latest.closed);
    }
  }

  @Test
  public void resolvesFixedP66TransitionVectorsAcrossCommittedSnapshots() throws Exception {
    byte[] address = address(43);
    String tokenId = "1000007";
    byte[] directKey = new P66AccountAssetCodec().assetPhysicalKey(address, tokenId);
    byte[] offAtZero = account(address, false, tokenId, 10L);
    byte[] offAtOne = account(address, false, tokenId, 20L);
    byte[] activationAtTwo = optimizedAccount(address, 2_000L);
    byte[] onAtThree = optimizedAccount(address, 3_000L);
    byte[] onAtFour = optimizedAccount(address, 4_000L);
    HistoricalAccountAssetPrefixResolver.Limits limits =
        new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000);

    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("p66-transition-vectors").toPath())) {
      fixture.append(diff(1,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(offAtZero))))));
      BlockReverseDiff activation = diff(2,
          new DbGroup("properties", Collections.singletonList(new Entry(
              HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey(),
              OldValue.present(ByteArray.fromLong(0L))))),
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(offAtOne)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.absent()))));
      fixture.append(activation);
      fixture.append(diff(3,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(activationAtTwo)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.present(ByteArray.fromLong(30L)))))));
      fixture.append(diff(4,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(onAtThree)))),
          new DbGroup("account-asset", Collections.singletonList(
              new Entry(directKey, OldValue.present(ByteArray.fromLong(40L)))))));
      fixture.sync();

      Entry activationReverseAsset = activation.getGroups().stream()
          .filter(group -> "account-asset".equals(group.getDbName()))
          .findFirst().orElseThrow(AssertionError::new).getEntries().get(0);
      assertArrayEquals(directKey, activationReverseAsset.getKey());
      assertFalse(activationReverseAsset.getOldValue().isPresent());

      Map<String, Map<String, byte[]>> latestValues = new HashMap<>();
      latestValues.put("properties", Collections.singletonMap(
          text(HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey()),
          ByteArray.fromLong(1L)));
      latestValues.put("account", Collections.singletonMap(text(address), onAtFour));
      latestValues.put("account-asset", Collections.singletonMap(
          text(directKey), ByteArray.fromLong(50L)));

      assertAccountAssetVector(fixture, latestValues, 1L, Phase.P66_OFF, offAtOne,
          tokenId, 20L, limits);
      // The request API reports the activation target as P66_ON because both use the same
      // canonical direct-row layout; the durable mutation plan retains P66_ACTIVATION.
      assertAccountAssetVector(fixture, latestValues, 2L, Phase.P66_ON, activationAtTwo,
          tokenId, 30L, limits);
      assertAccountAssetVector(fixture, latestValues, 3L, Phase.P66_ON, onAtThree,
          tokenId, 40L, limits);
    }
  }

  @Test
  public void accountAssetContextRejectsForeignAdaptersAndUseAfterClose() throws Exception {
    byte[] address = address(42);
    String tokenId = "1000001";
    Map<String, byte[]> latestValues = new HashMap<>();
    latestValues.put(text(HistoricalAccountAssetBalanceResolver.proposal66PhysicalKey()),
        ByteArray.fromLong(1L));
    latestValues.put(text(address), account(address, true, null, 0L));
    InMemoryLatest latest = new InMemoryLatest(0, hash(0), latestValues);
    AdapterSet adapters = rawAdapters();
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("closed-account-asset-context").toPath())) {
      ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapters.adapters);
      StoreAdapter<byte[]> foreignAccount = StoreAdapter.define("account", value -> value);
      assertThrows(IllegalArgumentException.class, () -> context.store(foreignAccount));
      StoreAdapter<byte[]> foreignAbi = StoreAdapter.define("abi", value -> value);
      assertThrows(IllegalArgumentException.class, () -> context.store(foreignAbi));
      assertEquals(0L, context.resolveAccountAsset(address, tokenId).getBalance());

      context.close();
      assertTrue(latest.closed);
      assertThrows(IllegalStateException.class,
          () -> context.resolveAccountAsset(address, tokenId));
      assertThrows(IllegalStateException.class,
          () -> context.resolveAccountAssets(address,
              new HistoricalAccountAssetPrefixResolver.Limits(10, 10, 10, 64, 8, 1_000)));
    }
  }

  @Test
  public void rejectsIncompleteOrDerivedStoreAdaptersAndReleasesSnapshot() throws Exception {
    assertThrows(IllegalArgumentException.class,
        () -> StoreAdapter.define("accountTrie", value -> value));
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("adapter-set").toPath())) {
      fixture.append(diff(1, entry("key", "old")));
      fixture.sync();
      InMemoryLatest latest = new InMemoryLatest(1, hash(1), Collections.emptyMap());
      AdapterSet adapterSet = rawAdapters();
      adapterSet.adapters.remove(adapterSet.account);

      assertThrows(IllegalArgumentException.class, () -> ArchiveReadContext.open(
          fixture.snapshot(0, latest), adapterSet.adapters));
      assertTrue(latest.closed);
    }
  }

  @Test
  public void resolvesLogicalStorageWithHistoricalContractFromTheSameContext() throws Exception {
    byte[] address = Hex.decode("410102030405060708090a0b0c0d0e0f1011121314");
    byte[] slot = Hex.decode(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    byte[] transactionHash = Hex.decode(
        "f0e0d0c0b0a090807060504030201000112233445566778899aabbccddeeff00");
    byte[] physicalKey = Hex.decode(
        "9397a7a785754542ff19d0968c0f92d4dea5e526567e92b0321816a4e895bd2d");
    SmartContract historicalContract = SmartContract.newBuilder().setVersion(1)
        .setTrxHash(ByteString.copyFrom(transactionHash)).build();
    SmartContract latestContract = SmartContract.newBuilder().setVersion(0).build();

    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("logical-storage").toPath())) {
      fixture.append(diff(1,
          new DbGroup("contract", Collections.singletonList(
              new Entry(address, OldValue.present(historicalContract.toByteArray())))),
          new DbGroup("storage-row", Collections.singletonList(
              new Entry(physicalKey, OldValue.present(bytes("historical-word")))))));
      fixture.sync();
      Map<String, byte[]> latestValues = new HashMap<>();
      latestValues.put(text(address), latestContract.toByteArray());
      AdapterSet adapterSet = rawAdapters();

      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(1, hash(1), latestValues)),
          adapterSet.adapters)) {
        assertArrayEquals(bytes("historical-word"),
            context.getStorage(address, slot).orElseThrow(AssertionError::new));
      }
    }
  }

  @Test
  public void historicalQuerySessionOwnsExactTypedViewAndReleasesGateLease() throws Exception {
    byte[] address = address(51);
    byte[] slot = Hex.decode(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    byte[] transactionHash = hash(91);
    SmartContract contract = SmartContract.newBuilder().setVersion(1)
        .setTrxHash(ByteString.copyFrom(transactionHash)).build();
    byte[] physicalKey = StorageRowKeyCodec.physicalKey(address, slot,
        contract.getVersion(), transactionHash);
    byte[] historicalAccount = optimizedAccount(address, 77L);
    byte[] historicalCode = bytes("historical-code");
    byte[] historicalStorage = hash(17);

    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-query-session").toPath())) {
      fixture.append(diff(1,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(historicalAccount)))),
          new DbGroup("contract", Collections.singletonList(
              new Entry(address, OldValue.present(contract.toByteArray())))),
          new DbGroup("code", Collections.singletonList(
              new Entry(address, OldValue.present(historicalCode)))),
          new DbGroup("storage-row", Collections.singletonList(
              new Entry(physicalKey, OldValue.present(historicalStorage)))),
          new DbGroup("properties", historicalVmProperties())));
      fixture.sync();
      InMemoryLatest latest = InMemoryLatest.scoped(1, hash(1), Collections.emptyMap());
      ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(
          target -> fixture.snapshot(target, latest));
      HistoricalQuerySession session = HistoricalQuerySession.open(gate.pin(0), hash(0));

      assertEquals(1, gate.getActiveLeaseCount());
      assertEquals(0L, session.getTargetBlock());
      assertArrayEquals(hash(0), session.getTargetBlockHash());
      assertEquals(77L, session.getAccount(address).orElseThrow(AssertionError::new)
          .getBalance());
      assertEquals(contract, session.getContract(address).orElseThrow(AssertionError::new));
      assertArrayEquals(historicalCode,
          session.getCode(address).orElseThrow(AssertionError::new));
      assertArrayEquals(historicalStorage,
          session.getStorage(address, slot).orElseThrow(AssertionError::new));

      session.close();
      assertEquals(0, gate.getActiveLeaseCount());
      assertTrue(latest.closed);
      assertThrows(IllegalStateException.class, session::getTargetBlock);
      gate.close();
    }
  }

  @Test
  public void historicalQuerySessionFailsClosedWhenReadBudgetIsExceeded() throws Exception {
    byte[] address = address(55);
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-query-budget").toPath())) {
      fixture.append(diff(1, new DbGroup("code", Collections.singletonList(
          new Entry(address, OldValue.present(bytes("old-code")))))));
      fixture.sync();
      InMemoryLatest latest = InMemoryLatest.scoped(1, hash(1), Collections.emptyMap());
      ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(
          target -> fixture.snapshot(target, latest));
      try (HistoricalQuerySession session = HistoricalQuerySession.open(gate.pin(0), hash(0),
          new HistoricalQuerySession.Limits(1, 1024, 10_000))) {
        assertArrayEquals(bytes("old-code"),
            session.getCode(address).orElseThrow(AssertionError::new));
        assertThrows(HistoricalQueryBudgetException.class, () -> session.getCode(address));
      }
      assertTrue(latest.closed);
      gate.close();
    }
  }

  @Test
  public void historicalRepositoryUsesOneSourceAndKeepsWritesInOverlay() throws Exception {
    byte[] address = address(52);
    byte[] slot = hash(31);
    byte[] historicalStorage = hash(32);
    byte[] overlayStorage = hash(33);
    byte[] historicalCode = bytes("historical-repository-code");
    SmartContract contract = SmartContract.newBuilder().setVersion(0).build();
    byte[] physicalKey = StorageRowKeyCodec.physicalKey(address, slot, 0, null);

    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-repository").toPath())) {
      fixture.append(diff(1,
          new DbGroup("account", Collections.singletonList(
              new Entry(address, OldValue.present(optimizedAccount(address, 81L))))),
          new DbGroup("contract", Collections.singletonList(
              new Entry(address, OldValue.present(contract.toByteArray())))),
          new DbGroup("code", Collections.singletonList(
              new Entry(address, OldValue.present(historicalCode)))),
          new DbGroup("storage-row", Collections.singletonList(
              new Entry(physicalKey, OldValue.present(historicalStorage)))),
          new DbGroup("properties", historicalVmProperties())));
      fixture.sync();
      InMemoryLatest latest = InMemoryLatest.scoped(1, hash(1), Collections.emptyMap());
      ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(
          target -> fixture.snapshot(target, latest));

      try (HistoricalQuerySession session = HistoricalQuerySession.open(gate.pin(0), hash(0))) {
        Repository root = RepositoryImpl.createHistoricalRoot(null, session);
        assertTrue(root.isHistorical());
        assertEquals(81L, root.getBalance(address));
        assertArrayEquals(historicalCode, root.getCode(address));
        assertEquals(new DataWord(historicalStorage),
            root.getStorageValue(address, new DataWord(slot)));

        Repository child = root.newRepositoryChild();
        child.putStorageValue(address, new DataWord(slot), new DataWord(overlayStorage));
        assertEquals(new DataWord(overlayStorage),
            child.getStorageValue(address, new DataWord(slot)));
        child.commit();
        assertEquals(new DataWord(overlayStorage),
            root.getStorageValue(address, new DataWord(slot)));
        ConfigLoader.load(root);
        assertTrue(VMConfig.allowTvmConstantinople());
        assertTrue(VMConfig.getEnergyLimitHardFork());
        VMConfig.clearLocalSnapshot();
        assertThrows(HistoricalCapabilityException.class,
            root::getDynamicPropertiesStore);

        Program program = mock(Program.class);
        when(program.getContractState()).thenReturn(root);
        assertThrows(HistoricalCapabilityException.class,
            () -> OperationActions.suicideAction(program));
        verify(program, never()).stackPop();
        HistoricalExecutionGuard.requirePrecompileAllowed(
            new PrecompiledContracts.Identity());
        assertThrows(HistoricalCapabilityException.class,
            () -> HistoricalExecutionGuard.requirePrecompileAllowed(
                new PrecompiledContracts.RewardBalance()));
        assertThrows(IllegalStateException.class, root::commit);
      }
      assertTrue(latest.closed);
      gate.close();
    }
  }

  private static List<Entry> historicalVmProperties() {
    List<Entry> properties = new ArrayList<>();
    String[] enabled = {
        "ALLOW_MULTI_SIGN", "ALLOW_TVM_TRANSFER_TRC10", "ALLOW_TVM_CONSTANTINOPLE",
        "ALLOW_TVM_SOLIDITY_059", "ALLOW_SHIELDED_TRC20_TRANSACTION",
        "ALLOW_TVM_ISTANBUL", "ALLOW_TVM_FREEZE", "ALLOW_TVM_VOTE", "ALLOW_TVM_LONDON",
        "ALLOW_TVM_COMPATIBLE_EVM", "ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX",
        "ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID", "ALLOW_DYNAMIC_ENERGY",
        "ALLOW_TVM_SHANGHAI", "ALLOW_ENERGY_ADJUSTMENT", "ALLOW_STRICT_MATH",
        "ALLOW_TVM_CANCUN", "CONSENSUS_LOGIC_OPTIMIZATION", "ALLOW_TVM_BLOB",
        "ALLOW_TVM_SELFDESTRUCT_RESTRICTION", "ALLOW_TVM_OSAKA",
        "ALLOW_HARDEN_RESOURCE_CALCULATION", "ALLOW_CREATION_OF_CONTRACTS"
    };
    for (String key : enabled) {
      properties.add(new Entry(bytes(key), OldValue.present(ByteArray.fromLong(1L))));
    }
    properties.add(new Entry(bytes("latest_block_header_number"),
        OldValue.present(ByteArray.fromLong(Long.MAX_VALUE))));
    String[] values = {
        "UNFREEZE_DELAY_DAYS", "DYNAMIC_ENERGY_THRESHOLD",
        "DYNAMIC_ENERGY_INCREASE_FACTOR", "DYNAMIC_ENERGY_MAX_FACTOR", "ENERGY_FEE",
        "MAX_FEE_LIMIT", "MAX_CPU_TIME_OF_ONE_TX", "CURRENT_CYCLE_NUMBER"
    };
    for (String key : values) {
      properties.add(new Entry(bytes(key), OldValue.present(ByteArray.fromLong(100L))));
    }
    return properties;
  }

  @Test
  public void historicalRepositoryAppliesEveryTypedDynamicPropertyDefault() throws Exception {
    Map<String, Long> defaults = DynamicPropertiesStore.getLongPropertyDefaults();
    Set<String> expectedKeys = new java.util.HashSet<>(Arrays.asList(
        "WITNESS_127_PAY_PER_BLOCK", "CURRENT_CYCLE_NUMBER", "ALLOW_TVM_SHANGHAI",
        "ALLOW_CANCEL_ALL_UNFREEZE_V2", "MAX_DELEGATE_LOCK_PERIOD",
        "ALLOW_OLD_REWARD_OPT", "ALLOW_ENERGY_ADJUSTMENT", "MAX_CREATE_ACCOUNT_TX_SIZE",
        "ALLOW_STRICT_MATH", "CONSENSUS_LOGIC_OPTIMIZATION", "ALLOW_TVM_CANCUN",
        "ALLOW_TVM_BLOB", "ALLOW_TVM_SELFDESTRUCT_RESTRICTION", "PROPOSAL_EXPIRE_TIME",
        "ALLOW_TVM_OSAKA", "ALLOW_TVM_PRAGUE", "BLOCK_HASH_HISTORY_INSTALLED",
        "ALLOW_HARDEN_RESOURCE_CALCULATION", "ALLOW_HARDEN_EXCHANGE_CALCULATION",
        "TURKISH_KEY_MIGRATION_DONE"));
    assertEquals(expectedKeys, defaults.keySet());

    List<Entry> requiredProperties = historicalVmProperties().stream()
        .filter(entry -> !defaults.containsKey(
            new String(entry.getKey(), StandardCharsets.UTF_8)))
        .collect(Collectors.toList());
    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-dynamic-defaults").toPath())) {
      fixture.append(diff(1, new DbGroup("properties", requiredProperties)));
      fixture.sync();
      InMemoryLatest latest = InMemoryLatest.scoped(1, hash(1), Collections.emptyMap());
      ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(
          target -> fixture.snapshot(target, latest));

      try (HistoricalQuerySession session = HistoricalQuerySession.open(gate.pin(0), hash(0))) {
        Repository root = RepositoryImpl.createHistoricalRoot(null, session);
        for (Map.Entry<String, Long> defaultEntry : defaults.entrySet()) {
          assertEquals(defaultEntry.getKey(), defaultEntry.getValue().longValue(),
              root.getDynamicPropertyLong(defaultEntry.getKey()));
        }
        assertThrows(IllegalArgumentException.class,
            () -> root.getDynamicPropertyLong("UNKNOWN_REQUIRED_PROPERTY"));

        ConfigLoader.load(root);
        assertEquals(defaults.get("ALLOW_TVM_OSAKA").longValue(),
            VMConfig.allowTvmOsaka() ? 1L : 0L);
        assertEquals(defaults.get("ALLOW_HARDEN_RESOURCE_CALCULATION").longValue(),
            VMConfig.allowHardenResourceCalculation() ? 1L : 0L);
        VMConfig.clearLocalSnapshot();
      }
      assertTrue(latest.closed);
      gate.close();
    }
  }

  @Test
  public void historicalVmExecutesTriggerAgainstPinnedState() throws Exception {
    byte[] owner = address(53);
    byte[] contractAddress = address(54);
    byte[] code = Hex.decode("60005460005260206000f3");
    byte[] storageValue = new DataWord(7).getData();
    byte[] physicalStorageKey = StorageRowKeyCodec.physicalKey(
        contractAddress, new byte[32], 0, null);
    SmartContract contract = SmartContract.newBuilder().setVersion(0).build();

    try (Fixture fixture = new Fixture(
        temporaryFolder.newFolder("historical-vm-trigger").toPath())) {
      fixture.append(diff(1,
          new DbGroup("account", Collections.singletonList(
              new Entry(contractAddress,
                  OldValue.present(optimizedAccount(contractAddress, 0L))))),
          new DbGroup("contract", Collections.singletonList(
              new Entry(contractAddress, OldValue.present(contract.toByteArray())))),
          new DbGroup("code", Collections.singletonList(
              new Entry(contractAddress, OldValue.present(code)))),
          new DbGroup("storage-row", Collections.singletonList(
              new Entry(physicalStorageKey, OldValue.present(storageValue)))),
          new DbGroup("properties", historicalVmProperties())));
      fixture.sync();
      InMemoryLatest latest = InMemoryLatest.scoped(1, hash(1), Collections.emptyMap());
      ArchiveRuntimeQueryGate gate = new ArchiveRuntimeQueryGate(
          target -> fixture.snapshot(target, latest));

      try (HistoricalQuerySession session = HistoricalQuerySession.open(gate.pin(0), hash(0))) {
        TriggerSmartContract trigger = TriggerSmartContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(owner))
            .setContractAddress(ByteString.copyFrom(contractAddress)).build();
        TransactionCapsule transaction = new TransactionCapsule(trigger,
            Protocol.Transaction.Contract.ContractType.TriggerSmartContract);
        TransactionContext context = new TransactionContext(
            new BlockCapsule(Protocol.Block.newBuilder().build()), transaction, null, true, false,
            ExecutionMode.HISTORICAL_CONSTANT, session);
        VMActuator actuator = new VMActuator(true, HistoricalRepositoryProvider.INSTANCE);
        long previousConstantCallTimeoutMs =
            CommonParameter.getInstance().getConstantCallTimeoutMs();
        CommonParameter.getInstance().setConstantCallTimeoutMs(5_000L);
        try {
          actuator.validate(context);
          actuator.execute(context);
          if (context.getProgramResult().getException() != null) {
            throw context.getProgramResult().getException();
          }
          assertEquals(new DataWord(7), new DataWord(context.getProgramResult().getHReturn()));

          TransactionCapsule createTransaction = new TransactionCapsule(
              CreateSmartContract.getDefaultInstance(),
              Protocol.Transaction.Contract.ContractType.CreateSmartContract);
          TransactionContext createContext = new TransactionContext(
              new BlockCapsule(Protocol.Block.newBuilder().build()), createTransaction, null,
              true, false, ExecutionMode.HISTORICAL_CONSTANT, session);
          VMActuator createActuator = new VMActuator(
              true, HistoricalRepositoryProvider.INSTANCE);
          ContractValidateException rejected = assertThrows(ContractValidateException.class,
              () -> createActuator.validate(createContext));
          assertEquals("Historical execution only supports TriggerSmartContract",
              rejected.getMessage());
        } finally {
          CommonParameter.getInstance().setConstantCallTimeoutMs(previousConstantCallTimeoutMs);
          VMConfig.clearLocalSnapshot();
        }
      }
      assertTrue(latest.closed);
      gate.close();
    }
  }

  @Test
  public void logicalStorageFailsClosedForMissingCorruptOrClosedContractContext()
      throws Exception {
    byte[] address = Hex.decode("410102030405060708090a0b0c0d0e0f1011121314");
    byte[] slot = new byte[32];
    AdapterSet adapterSet = rawAdapters();
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("missing-contract").toPath())) {
      ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(0, hash(0), Collections.emptyMap())),
          adapterSet.adapters);
      assertThrows(ArchivePersistenceException.class, () -> context.getStorage(address, slot));
      context.close();
      assertThrows(IllegalStateException.class, () -> context.getStorage(address, slot));
    }

    adapterSet = rawAdapters();
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("corrupt-contract").toPath())) {
      fixture.append(diff(1, new DbGroup("contract", Collections.singletonList(
          new Entry(address, OldValue.present(new byte[]{(byte) 0x80}))))));
      fixture.sync();
      try (ArchiveReadContext context = ArchiveReadContext.open(
          fixture.snapshot(0, new InMemoryLatest(1, hash(1), Collections.emptyMap())),
          adapterSet.adapters)) {
        assertThrows(ArchivePersistenceException.class, () -> context.getStorage(address, slot));
      }
    }
  }

  private void assertUnreadableBody(String name, LocationMutation mutation,
      Class<? extends Throwable> error) throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder(name).toPath())) {
      BlockReverseDiff body = diff(1, entry("key", "old"));
      HistoryLocation actual = fixture.bodies.append(body);
      HistoryLocation referenced = mutation.apply(actual);
      HistoryIndexLocation indexLocation = fixture.index.append(
          new HistoryIndexRecord(body.getMeta(), referenced,
              Collections.singletonList(new KeyGroup("account",
                  Collections.singletonList(bytes("key"))))));
      fixture.markers.add(marker(body.getMeta(), referenced, indexLocation));
      fixture.sync();

      try (ArchiveReadSnapshot snapshot = fixture.snapshot(0,
          new InMemoryLatest(1, hash(1), Collections.emptyMap()))) {
        assertThrows(error, () -> snapshot.get("account", bytes("key")));
      }
    }
  }

  private static BlockReverseDiff diff(int block, Entry... entries) {
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block),
        hash(block - 1), block * 3_000L), Collections.singletonList(
        new DbGroup("account", Arrays.asList(entries))));
  }

  private static BlockReverseDiff diff(int block, DbGroup... groups) {
    return new BlockReverseDiff(new BlockSnapshotMeta(block, block, hash(block),
        hash(block - 1), block * 3_000L), Arrays.asList(groups));
  }

  private static Entry entry(String key, String oldValue) {
    return entry(key, OldValue.present(bytes(oldValue)));
  }

  private static Entry entry(String key, OldValue oldValue) {
    return new Entry(bytes(key), oldValue);
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta, HistoryLocation body,
      HistoryIndexLocation index) {
    return new HistoryCommitMarker(meta, meta.getEpoch() - 1, body, index, new byte[16],
        new ArrayList<>(ArchiveStoreScope.getStateDatabases()));
  }

  private static void assertValue(OldValue value, String expected) {
    assertTrue(value.isPresent());
    assertArrayEquals(bytes(expected), value.getValue());
  }

  private static List<String> keys(List<HistoricalRangeOverlay.Entry> entries) {
    List<String> result = new ArrayList<>();
    entries.forEach(entry -> result.add(text(entry.getKey())));
    return result;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] account(byte[] address, boolean optimized, String tokenId, long balance) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(optimized);
    if (tokenId != null) {
      builder.putAsset("asset-name", balance).putAssetV2(tokenId, balance);
    }
    return builder.build().toByteArray();
  }

  private static byte[] optimizedAccount(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setType(AccountType.Normal).setAssetOptimized(true).setBalance(balance)
        .build().toByteArray();
  }

  private static void assertAccountAssetVector(Fixture fixture,
      Map<String, Map<String, byte[]>> latestValues, long target, Phase expectedPhase,
      byte[] expectedAccount,
      String tokenId, long expectedBalance,
      HistoricalAccountAssetPrefixResolver.Limits limits) throws Exception {
    InMemoryLatest latest = InMemoryLatest.scoped(4, hash(4), latestValues);
    ArchiveReadSnapshot snapshot = fixture.snapshot(target, latest);
    assertArrayEquals(hash(4), snapshot.getPinnedHash());
    snapshot.requirePinnedIdentity();
    try (ArchiveReadContext context = ArchiveReadContext.open(
        snapshot, rawAdapters().adapters)) {
      assertEquals(target, context.getTargetBlock());
      assertEquals(4L, context.getPinnedBlock());
      HistoricalAccountAssetBalanceResolver.Result exact =
          context.resolveAccountAsset(address(43), tokenId);
      assertEquals(expectedPhase, exact.getPhase());
      assertArrayEquals(expectedAccount, exact.getAccountValue());
      assertEquals(expectedBalance, exact.getBalance());

      HistoricalAccountAssetPrefixResolver.Result prefix =
          context.resolveAccountAssets(address(43), limits);
      assertEquals(expectedPhase, prefix.getPhase());
      assertArrayEquals(expectedAccount, prefix.getAccountValue());
      assertEquals(1, prefix.getBalances().size());
      assertEquals(tokenId, prefix.getBalances().get(0).getTokenId());
      assertEquals(expectedBalance, prefix.getBalances().get(0).getBalance());
      snapshot.requirePinnedIdentity();
    }
    assertTrue(latest.closed);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String text(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  private static AdapterSet rawAdapters() {
    List<StoreAdapter<?>> adapters = new ArrayList<>();
    StoreAdapter<byte[]> account = StoreAdapter.define("account",
        value -> Arrays.copyOf(value, value.length));
    for (String dbName : ArchiveStoreScope.getStateDatabases()) {
      adapters.add("account".equals(dbName) ? account : StoreAdapter.define(dbName,
          value -> Arrays.copyOf(value, value.length)));
    }
    return new AdapterSet(account, adapters);
  }

  @FunctionalInterface
  private interface LocationMutation {
    HistoryLocation apply(HistoryLocation location);
  }

  private static final class AdapterSet {
    private final StoreAdapter<byte[]> account;
    private final List<StoreAdapter<?>> adapters;

    private AdapterSet(StoreAdapter<byte[]> account, List<StoreAdapter<?>> adapters) {
      this.account = account;
      this.adapters = adapters;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final HistorySegmentStore bodies;
    private final HistoryIndexStore index;
    private final List<HistoryCommitMarker> markers = new ArrayList<>();

    private Fixture(Path archive) throws IOException {
      bodies = new HistorySegmentStore(archive, new BlockHistoryCodec(), 4096);
      index = new HistoryIndexStore(archive, new HistoryIndexCodec());
    }

    private void append(BlockReverseDiff diff) throws IOException {
      HistoryLocation body = bodies.append(diff);
      HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
      List<String> databases = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
      markers.add(new HistoryCommitMarker(diff.getMeta(), diff.getMeta().getEpoch() - 1, body,
          indexLocation, new byte[16], databases));
    }

    private void sync() throws IOException {
      bodies.sync();
      index.sync();
    }

    private ServingKeyIndexGeneration serving() throws IOException {
      return ServingKeyIndexGeneration.rebuild(
          "read-generation", 0, hash(0), markers, index::read,
          new ArrayList<>(ArchiveStoreScope.getStateDatabases()),
          ServingKeyIndexGeneration.IndexLayout.prototypeDefaults());
    }

    private CommittedHistoryReader history() throws IOException {
      return new CommittedHistoryReader(0, hash(0), markers, index::read, bodies::read,
          new ArrayList<>(ArchiveStoreScope.getStateDatabases()));
    }

    private ArchiveReadSnapshot snapshot(long target, InMemoryLatest latest) throws IOException {
      return ArchiveReadSnapshot.pin(target, markers.size(), hash(markers.size()), serving(),
          latest, history());
    }

    @Override
    public void close() throws Exception {
      index.close();
      bodies.close();
    }
  }

  private static final class InMemoryLatest implements PinnedLatestState {
    private final long block;
    private final byte[] hash;
    private final Map<String, byte[]> values;
    private final Map<String, Map<String, byte[]>> scopedValues;
    private boolean closed;

    private InMemoryLatest(long block, byte[] hash, Map<String, byte[]> values) {
      this.block = block;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.values = new HashMap<>();
      this.scopedValues = null;
      values.forEach((key, value) -> this.values.put(key, Arrays.copyOf(value, value.length)));
    }

    private InMemoryLatest(long block, byte[] hash,
        Map<String, Map<String, byte[]>> scopedValues, boolean scoped) {
      this.block = block;
      this.hash = Arrays.copyOf(hash, hash.length);
      this.values = Collections.emptyMap();
      this.scopedValues = new HashMap<>();
      scopedValues.forEach((dbName, rows) -> {
        Map<String, byte[]> copy = new HashMap<>();
        rows.forEach((key, value) -> copy.put(key, Arrays.copyOf(value, value.length)));
        this.scopedValues.put(dbName, copy);
      });
    }

    private static InMemoryLatest scoped(long block, byte[] hash,
        Map<String, Map<String, byte[]>> values) {
      return new InMemoryLatest(block, hash, values, true);
    }

    @Override
    public long getBlockNumber() {
      return block;
    }

    @Override
    public byte[] getBlockHash() {
      return Arrays.copyOf(hash, hash.length);
    }

    @Override
    public OldValue get(String dbName, byte[] physicalRawKey) {
      Map<String, byte[]> rows = scopedValues == null ? values
          : scopedValues.getOrDefault(dbName, Collections.emptyMap());
      return OldValue.fromNullable(rows.get(text(physicalRawKey)));
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive, int maxEntries) {
      List<HistoricalRangeOverlay.Entry> result = new ArrayList<>();
      Map<String, byte[]> rows = scopedValues == null ? values
          : scopedValues.getOrDefault(dbName, Collections.emptyMap());
      rows.forEach((key, value) -> {
        byte[] rawKey = bytes(key);
        if (BlockReverseDiff.compareUnsigned(rawKey, lowerInclusive) >= 0
            && (upperExclusive == null
            || BlockReverseDiff.compareUnsigned(rawKey, upperExclusive) < 0)) {
          result.add(new HistoricalRangeOverlay.Entry(rawKey, value));
        }
      });
      result.sort(Comparator.comparing(HistoricalRangeOverlay.Entry::getKey,
          BlockReverseDiff::compareUnsigned));
      return result;
    }

    @Override
    public void close() {
      closed = true;
    }

  }
}
