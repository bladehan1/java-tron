package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.googlecode.jsonrpc4j.JsonRpcServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.arch.Arch;
import org.tron.common.runtime.vm.DataWord;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointFile;
import org.tron.core.db2.core.CommonCheckpointMaterializer;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointRedoCoordinator;
import org.tron.core.db2.core.CommonCheckpointRuntime;
import org.tron.core.db2.core.CommonCheckpointRuntimeAttachment;
import org.tron.core.db2.core.CommonCheckpointRuntimeOwner;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.core.services.NodeInfoService;
import org.tron.core.services.jsonrpc.JsonRpcErrorResolver;
import org.tron.core.services.jsonrpc.TronJsonRpc;
import org.tron.core.services.jsonrpc.TronJsonRpcImpl;
import org.tron.core.store.StorageRowKeyCodec;
import org.tron.core.store.StorageRowStore;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;
import org.tron.protos.contract.SmartContractOuterClass.SmartContractDataWrapper;

public class StateArchiveJsonRpcIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final byte[] ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000001");
  private static final String RPC_ADDRESS = "0x0000000000000000000000000000000000000001";
  private static final byte[] CALL_ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000003");
  private static final String RPC_CALL_ADDRESS =
      "0x0000000000000000000000000000000000000003";
  private static final byte[] CAPABILITY_ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000004");
  private static final String RPC_CAPABILITY_ADDRESS =
      "0x0000000000000000000000000000000000000004";
  private static final byte[] REVERT_ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000005");
  private static final String RPC_REVERT_ADDRESS =
      "0x0000000000000000000000000000000000000005";
  private static final byte[] LOOP_ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000006");
  private static final String RPC_LOOP_ADDRESS =
      "0x0000000000000000000000000000000000000006";
  private static final byte[] BASELINE_CALL_ADDRESS = ByteArray.fromHexString(
      "410000000000000000000000000000000000000007");
  private static final String RPC_BASELINE_CALL_ADDRESS =
      "0x0000000000000000000000000000000000000007";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 30000)
  public void numberAndHashReadHistoricalBalanceCodeAndStorageThroughManager() throws Exception {
    for (Engine engine : availableEngines()) {
      try (Fixture fixture = new Fixture(temporaryFolder.newFolder(engine.name()).toPath(),
          engine)) {
        fixture.ready();
        for (Object selector : Arrays.asList("0xa", map("blockNumber", "0xa"),
            map("blockHash", fixture.hash(10)))) {
          assertEquals("0xb", fixture.result("eth_getBalance", RPC_ADDRESS, selector));
          assertEquals("0x0b", fixture.result("eth_getCode", RPC_ADDRESS, selector));
          assertEquals(word(11), fixture.result("eth_getStorageAt", RPC_ADDRESS, "0x0", selector));
        }
        assertEquals("0xc", fixture.result("eth_getBalance", RPC_ADDRESS, "0xb"));
        assertEquals("0x0c", fixture.result("eth_getCode", RPC_ADDRESS, "0xb"));
        assertEquals(word(12), fixture.result("eth_getStorageAt", RPC_ADDRESS, "0x0", "0xb"));
        String absent = "0x0000000000000000000000000000000000000002";
        assertEquals("0x0", fixture.result("eth_getBalance", absent, "0xa"));
        assertEquals("0x", fixture.result("eth_getCode", absent, "0xa"));
        assertEquals(word(0), fixture.result("eth_getStorageAt", absent, "0x0", "0xa"));
        Map<String, Object> canonical = map("blockHash", fixture.hash(10));
        canonical.put("requireCanonical", true);
        assertEquals("0x0b", fixture.result("eth_getCode", RPC_ADDRESS, canonical));
        verify(fixture.wallet, never()).getAccount(any());
        verify(fixture.wallet, never()).getContract(any());
        verify(fixture.wallet, never()).getContractInfo(any());
        verify(fixture.storage, never()).get(any(byte[].class));
        assertEquals("0x63", fixture.result("eth_getBalance", RPC_ADDRESS, "latest"));
        assertEquals("0x63", fixture.result("eth_getCode", RPC_ADDRESS, "latest"));
        assertEquals(word(99), fixture.result("eth_getStorageAt", RPC_ADDRESS, "0x0", "latest"));
        assertEquals(fixture.pins, fixture.releases);
      }
    }
  }

  @Test(timeout = 30000)
  public void rejectsUncoveredNoncanonicalAndMalformedSelectorsWithoutLatestFallback()
      throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("negative").toPath(),
        Engine.ROCKSDB)) {
      fixture.error("eth_getCode", RPC_ADDRESS, "0xa"); // BULK
      fixture.ready();
      BlockCapsule fork = new BlockCapsule(10, Sha256Hash.ZERO_HASH, 99999,
          ByteString.EMPTY);
      fixture.byHash.put(ByteArray.toJsonHex(fork.getBlockId().getBytes()), fork);
      Map<String, Object> noncanonical = map("blockHash",
          ByteArray.toJsonHex(fork.getBlockId().getBytes()));
      noncanonical.put("requireCanonical", false);
      Map<String, Object> ambiguous = map("blockNumber", "latest");
      ambiguous.put("blockHash", fixture.hash(10));
      Map<String, Object> wrongFlag = map("blockHash", fixture.hash(10));
      wrongFlag.put("requireCanonical", "true");
      for (Object selector : Arrays.asList("0x9", "0xd", "pending", null, 10,
          noncanonical, ambiguous, wrongFlag, map("blockNumber", 10),
          map("blockHash", "0x01"), Collections.emptyMap())) {
        fixture.error("eth_getBalance", RPC_ADDRESS, selector);
        fixture.error("eth_getCode", RPC_ADDRESS, selector);
        fixture.error("eth_getStorageAt", RPC_ADDRESS, "0x0", selector);
        fixture.error("eth_call", map("to", RPC_ADDRESS), selector);
      }
      verify(fixture.wallet, never()).getAccount(any());
      verify(fixture.wallet, never()).getContractInfo(any());
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  @Test(timeout = 30000)
  public void latestRemainsAvailableWhenArchiveIsDisabled() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("disabled").toPath(),
        Engine.ROCKSDB)) {
      ReflectionTestUtils.setField(fixture.manager, "commonCheckpointRuntime", null);
      assertEquals("0x63", fixture.result("eth_getBalance", RPC_ADDRESS, "latest"));
      assertEquals("0x63", fixture.result("eth_getCode", RPC_ADDRESS, "latest"));
      assertEquals(word(99), fixture.result("eth_getStorageAt", RPC_ADDRESS, "0x0", "latest"));
      fixture.error("eth_getCode", RPC_ADDRESS, "0xa");
      assertEquals(0, fixture.pins);
    }
  }

  @Test(timeout = 30000)
  public void canonicalChangeDuringLatestAccessIsAllowedByPrototypeScope()
      throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("canonical-race").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      BlockCapsule canonical = fixture.blocks.get(10L);
      BlockCapsule fork = new BlockCapsule(10, Sha256Hash.ZERO_HASH, 99999, ByteString.EMPTY);
      fixture.onPin = () -> fixture.blocks.put(10L, fork);
      assertEquals("0x", fixture.result("eth_getCode",
          "410000000000000000000000000000000000000099", fixture.hashSelector(10)));
      assertEquals(1, fixture.pins);
      assertEquals(fixture.pins, fixture.releases);
      fixture.onPin = () -> { };
      fixture.blocks.put(10L, canonical);
      assertEquals("0x0b", fixture.result("eth_getCode", RPC_ADDRESS, fixture.hashSelector(10)));
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  @Test(timeout = 30000)
  public void historicalCallExecutesPinnedVmStateAndReleasesRequest() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("historical-call").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      fixture.enableHistoricalVm();
      Map<String, Object> call = map("to", RPC_CALL_ADDRESS);
      call.put("from", RPC_ADDRESS);
      call.put("data", "0x");
      for (Object selector : Arrays.asList("0xa", map("blockNumber", "0xa"),
          fixture.hashSelector(10))) {
        assertEquals(word(7), fixture.result("eth_call", call, selector));
      }
      assertEquals(word(7), fixture.result("eth_getStorageAt",
          RPC_CALL_ADDRESS, "0x0", "0xa"));
      assertEquals(word(0), fixture.result("eth_getStorageAt",
          RPC_CALL_ADDRESS, "0x1", "0xa"));
      assertArrayEquals(digest(99),
          fixture.baseline.get(key("storage-row", fixture.callLatestSlot)));
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  @Test(timeout = 30000)
  public void historicalCallDoesNotBlockCommonCheckpointPublication() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("lock-free-call").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      fixture.enableHistoricalVm();
      CountDownLatch accessStarted = new CountDownLatch(1);
      CountDownLatch releaseAccess = new CountDownLatch(1);
      AtomicBoolean blockFirstAccess = new AtomicBoolean(true);
      fixture.onPin = () -> {
        if (!blockFirstAccess.compareAndSet(true, false)) {
          return;
        }
        accessStarted.countDown();
        try {
          assertTrue(releaseAccess.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new AssertionError(failure);
        }
      };
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<String> call = executor.submit(() -> fixture.result("eth_call",
            call(RPC_BASELINE_CALL_ADDRESS), "0xa"));
        assertTrue(accessStarted.await(5, TimeUnit.SECONDS));
        Future<?> checkpoint = executor.submit(() -> {
          fixture.owner.apply(fixture.payload);
          return null;
        });
        checkpoint.get(5, TimeUnit.SECONDS);
        releaseAccess.countDown();
        assertEquals(word(7), call.get(5, TimeUnit.SECONDS));
        assertEquals(fixture.pins, fixture.releases);
      } finally {
        releaseAccess.countDown();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
  }

  @Test(timeout = 30000)
  public void historicalCallPreservesVmAndCapabilityErrorClassification() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("historical-call-errors").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      fixture.enableHistoricalVm();
      fixture.error(-32000, "SELFDESTRUCT is not supported by historical execution",
          "eth_call", call(RPC_CAPABILITY_ADDRESS), "0xa");
      fixture.error(-32000, "REVERT opcode executed",
          "eth_call", call(RPC_REVERT_ADDRESS), "0xa");
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  @Test(timeout = 30000)
  public void concurrentHistoricalPointAndVmRequestsReleaseEveryPin() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("historical-concurrent").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      fixture.enableHistoricalVm();
      ExecutorService executor = Executors.newFixedThreadPool(4);
      try {
        List<Future<String>> results = new ArrayList<>();
        for (int request = 0; request < 12; request++) {
          boolean vm = request % 2 == 0;
          results.add(executor.submit(() -> vm
              ? fixture.result("eth_call", call(RPC_CALL_ADDRESS), "0xa")
              : fixture.result("eth_getBalance", RPC_ADDRESS, "0xa")));
        }
        for (int request = 0; request < results.size(); request++) {
          assertEquals(request % 2 == 0 ? word(7) : "0xb",
              results.get(request).get(10, TimeUnit.SECONDS));
        }
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
      assertEquals(12, fixture.pins);
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  @Test(timeout = 30000)
  public void historicalVmCancellationStopsPureComputeAndReleasesPin() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("historical-cancel").toPath(),
        Engine.ROCKSDB)) {
      fixture.ready();
      fixture.enableHistoricalVm();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<JsonNode> response = executor.submit(
            () -> fixture.request("eth_call", call(RPC_LOOP_ADDRESS), "0xa"));
        long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (fixture.pins == 0 && System.nanoTime() < waitUntil) {
          Thread.yield();
        }
        assertTrue("historical call did not acquire a pin", fixture.pins > 0);
        HistoricalQueryAdmission admission = (HistoricalQueryAdmission)
            ReflectionTestUtils.getField(fixture.manager, "historicalQueryAdmission");
        admission.stop();
        JsonNode cancelled = response.get(5, TimeUnit.SECONDS);
        assertEquals(cancelled.toString(), -32000,
            cancelled.get("error").get("code").asInt());
        assertTrue(cancelled.toString(),
            cancelled.get("error").get("message").asText().contains("cancelled"));
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
      assertEquals(fixture.pins, fixture.releases);
    }
  }

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB} : Engine.values();
  }

  private static Map<String, Object> map(String key, Object value) {
    Map<String, Object> result = new HashMap<>();
    result.put(key, value);
    return result;
  }

  private static Map<String, Object> call(String address) {
    Map<String, Object> result = map("to", address);
    result.put("from", RPC_ADDRESS);
    result.put("data", "0x");
    return result;
  }

  private static String word(int value) {
    return ByteArray.toJsonHex(new DataWord(value).getData());
  }

  private static byte[] digest(int value) {
    return new DataWord(value).getData();
  }

  private static Account account(long balance) {
    return account(ADDRESS, balance);
  }

  private static Account account(byte[] address, long balance) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setBalance(balance).build();
  }

  private static final class Fixture implements AutoCloseable {
    private final Map<Long, BlockCapsule> blocks = new HashMap<>();
    private final Map<String, BlockCapsule> byHash = new HashMap<>();
    private final Map<String, byte[]> baseline = new HashMap<>();
    private final Wallet wallet = mock(Wallet.class);
    private final StorageRowStore storage = mock(StorageRowStore.class);
    private final Manager manager = new Manager();
    private final StateArchiveAppendCheckpointMaterializerV5 archive;
    private final CommonCheckpointRuntimeAttachment attachment;
    private final CommonCheckpointRuntimeOwner owner;
    private final CommonCheckpointPayload payload;
    private final CommonCheckpointTarget target;
    private final TronJsonRpcImpl rpc;
    private final JsonRpcServer server;
    private final byte[] callLatestSlot;
    private final boolean originalSupportConstant = Args.getInstance().isSupportConstant();
    private final long originalConstantCallTimeoutMs =
        Args.getInstance().getConstantCallTimeoutMs();
    private Runnable onPin = () -> { };
    private volatile int pins;
    private volatile int releases;

    private Fixture(Path root, Engine engine) throws Exception {
      Sha256Hash parent = Sha256Hash.ZERO_HASH;
      for (long number = 9; number <= 13; number++) {
        BlockCapsule block = new BlockCapsule(number, parent, number * 3000, ByteString.EMPTY);
        blocks.put(number, block);
        byHash.put(ByteArray.toJsonHex(block.getBlockId().getBytes()), block);
        parent = block.getBlockId();
      }
      SmartContract oldContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(ADDRESS)).setVersion(0)
          .setTrxHash(ByteString.copyFrom(digest(10))).build();
      SmartContract latestContract = oldContract.toBuilder().setVersion(1)
          .setTrxHash(ByteString.copyFrom(digest(99))).build();
      byte[] oldSlot = StorageRowKeyCodec.physicalKey(ADDRESS, digest(0), 0, digest(10));
      byte[] latestSlot = StorageRowKeyCodec.physicalKey(ADDRESS, digest(0), 1, digest(99));
      byte[] callOldSlot = StorageRowKeyCodec.physicalKey(CALL_ADDRESS, digest(0), 0, null);
      callLatestSlot = callOldSlot;
      byte[] callCode = ByteArray.fromHexString("600054600860015560005260206000f3");
      SmartContract callContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(CALL_ADDRESS)).setVersion(0).build();
      SmartContract capabilityContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(CAPABILITY_ADDRESS)).setVersion(0).build();
      SmartContract revertContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(REVERT_ADDRESS)).setVersion(0).build();
      SmartContract loopContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(LOOP_ADDRESS)).setVersion(0).build();
      SmartContract baselineCallContract = SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(BASELINE_CALL_ADDRESS)).setVersion(0).build();
      byte[] baselineCallSlot = StorageRowKeyCodec.physicalKey(BASELINE_CALL_ADDRESS, digest(0),
          0, null);
      baseline.put(key("account", ADDRESS), account(99).toByteArray());
      baseline.put(key("code", ADDRESS), new byte[]{99});
      baseline.put(key("contract", ADDRESS), latestContract.toByteArray());
      baseline.put(key("storage-row", oldSlot), digest(88));
      baseline.put(key("storage-row", latestSlot), digest(99));
      baseline.put(key("account", CALL_ADDRESS), account(CALL_ADDRESS, 0).toByteArray());
      baseline.put(key("code", CALL_ADDRESS), new byte[]{0});
      baseline.put(key("contract", CALL_ADDRESS), callContract.toByteArray());
      baseline.put(key("storage-row", callLatestSlot), digest(99));
      baseline.put(key("account", CAPABILITY_ADDRESS),
          account(CAPABILITY_ADDRESS, 0).toByteArray());
      baseline.put(key("code", CAPABILITY_ADDRESS), new byte[]{0});
      baseline.put(key("contract", CAPABILITY_ADDRESS), capabilityContract.toByteArray());
      baseline.put(key("account", REVERT_ADDRESS), account(REVERT_ADDRESS, 0).toByteArray());
      baseline.put(key("code", REVERT_ADDRESS), new byte[]{0});
      baseline.put(key("contract", REVERT_ADDRESS), revertContract.toByteArray());
      baseline.put(key("account", LOOP_ADDRESS), account(LOOP_ADDRESS, 0).toByteArray());
      baseline.put(key("code", LOOP_ADDRESS), new byte[]{0});
      baseline.put(key("contract", LOOP_ADDRESS), loopContract.toByteArray());
      baseline.put(key("account", BASELINE_CALL_ADDRESS),
          account(BASELINE_CALL_ADDRESS, 0).toByteArray());
      baseline.put(key("code", BASELINE_CALL_ADDRESS), callCode);
      baseline.put(key("contract", BASELINE_CALL_ADDRESS),
          baselineCallContract.toByteArray());
      baseline.put(key("storage-row", baselineCallSlot), digest(7));
      List<BlockReverseDiff> diffs = new ArrayList<>();
      for (int number = 11; number <= 12; number++) {
        BlockCapsule block = blocks.get((long) number);
        diffs.add(new BlockReverseDiff(BlockSnapshotMeta.forBlock(number,
            block.getBlockId().getBytes(), blocks.get((long) number - 1).getBlockId().getBytes(),
            number * 3000L), Arrays.asList(
                group("account",
                    new Entry(ADDRESS, OldValue.present(account(number).toByteArray())),
                    new Entry(CALL_ADDRESS,
                        OldValue.present(account(CALL_ADDRESS, 0).toByteArray())),
                    new Entry(CAPABILITY_ADDRESS,
                        OldValue.present(account(CAPABILITY_ADDRESS, 0).toByteArray())),
                    new Entry(REVERT_ADDRESS,
                        OldValue.present(account(REVERT_ADDRESS, 0).toByteArray())),
                    new Entry(LOOP_ADDRESS,
                        OldValue.present(account(LOOP_ADDRESS, 0).toByteArray()))),
                group("code",
                    new Entry(ADDRESS, OldValue.present(new byte[]{(byte) number})),
                    new Entry(CALL_ADDRESS, OldValue.present(callCode)),
                    new Entry(CAPABILITY_ADDRESS,
                        OldValue.present(ByteArray.fromHexString("6000ff"))),
                    new Entry(REVERT_ADDRESS,
                        OldValue.present(ByteArray.fromHexString("60006000fd"))),
                    new Entry(LOOP_ADDRESS,
                        OldValue.present(ByteArray.fromHexString("5b600056")))),
                group("contract",
                    new Entry(ADDRESS, OldValue.present(oldContract.toByteArray())),
                    new Entry(CALL_ADDRESS, OldValue.present(callContract.toByteArray())),
                    new Entry(CAPABILITY_ADDRESS,
                        OldValue.present(capabilityContract.toByteArray())),
                    new Entry(REVERT_ADDRESS, OldValue.present(revertContract.toByteArray())),
                    new Entry(LOOP_ADDRESS, OldValue.present(loopContract.toByteArray()))),
                group("storage-row",
                    new Entry(oldSlot, OldValue.present(digest(number))),
                    new Entry(callOldSlot, OldValue.present(digest(7)))),
                new DbGroup("properties", historicalVmProperties()))));
      }
      archive = new StateArchiveAppendCheckpointMaterializerV5(root.resolve("archive"),
          digest(77), engine, 11,
          StateArchiveAppendCheckpointMaterializerV5.baselineHistoryDigest(11,
              blocks.get(10L).getBlockId().getBytes()),
          1500, number -> {
        BlockCapsule block = blocks.get(number);
        return block == null ? null : BlockSnapshotMeta.forBlock(number,
            block.getBlockId().getBytes(),
            blocks.get(number - 1).getBlockId().getBytes(), number * 3000L);
      });
      StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
      payload = payload(diffs, descriptor);
      target = archive.prepare(CommonCheckpointCapture.create(payload, diffs, descriptor));
      archive.publish(target);
      owner = new CommonCheckpointRuntimeOwner(
          new CommonCheckpointRedoCoordinator(new CommonCheckpointFile(root.resolve("wal")),
              authority(Authority.CHAINBASE), authority(Authority.PATH_STATE), archive));
      CommonCheckpointRuntime runtime = new CommonCheckpointRuntime(owner,
          Collections.singletonList(mock(Chainbase.class)), root.resolve("archive"), digest(77),
          engine, this::pin, ignored -> () -> { }, archive);
      attachment = CommonCheckpointRuntimeAttachment.open(true, () -> runtime);
      ChainBaseManager chainbase = mock(ChainBaseManager.class);
      when(chainbase.getBlockByNum(anyLong())).thenAnswer(call -> blocks.get(call.getArgument(0)));
      when(chainbase.getStorageRowStore()).thenReturn(storage);
      ReflectionTestUtils.setField(manager, "chainBaseManager", chainbase);
      ReflectionTestUtils.setField(manager, "commonCheckpointRuntime", attachment);
      ReflectionTestUtils.setField(wallet, "chainBaseManager", chainbase);
      when(wallet.getBlockByNum(anyLong())).thenAnswer(call -> {
        BlockCapsule block = blocks.get(call.getArgument(0));
        return block == null ? null : block.getInstance();
      });
      when(wallet.getBlockById(any())).thenAnswer(call -> {
        ByteString requested = call.getArgument(0);
        BlockCapsule block = byHash.get(ByteArray.toJsonHex(requested.toByteArray()));
        return block == null ? null : block.getInstance();
      });
      when(wallet.getAccount(any())).thenReturn(account(99));
      when(wallet.getContract(any())).thenReturn(latestContract);
      when(wallet.getContractInfo(any())).thenReturn(SmartContractDataWrapper.newBuilder()
          .setRuntimecode(ByteString.copyFrom(new byte[]{99})).build());
      when(storage.get(any(byte[].class))).thenAnswer(call -> {
        byte[] rawKey = call.getArgument(0);
        byte[] value = baseline.get(key("storage-row", rawKey));
        return value == null ? null : new StorageRowCapsule(rawKey, value);
      });
      rpc = new TronJsonRpcImpl(mock(NodeInfoService.class), wallet);
      rpc.setManager(manager);
      server = new JsonRpcServer(rpc, TronJsonRpc.class);
      server.setErrorResolver(JsonRpcErrorResolver.INSTANCE);
    }

    private void enableHistoricalVm() throws Exception {
      Args.getInstance().supportConstant = true;
      Args.getInstance().setConstantCallTimeoutMs(5_000L);
      doCallRealMethod().when(wallet).createTransactionCapsule(any(),
          org.mockito.ArgumentMatchers.eq(ContractType.TriggerSmartContract));
      doCallRealMethod().when(wallet).callHistoricalConstantContract(any(), any(), any(), any());
    }

    private ArchiveReadSnapshot.PinnedLatestState pin(long number, byte[] hash) {
      assertEquals(12, number);
      assertTrue(Arrays.equals(blocks.get(12L).getBlockId().getBytes(), hash));
      pins++;
      onPin.run();
      ArchiveReadSnapshot.PinnedLatestState snapshot =
          mock(ArchiveReadSnapshot.PinnedLatestState.class);
      when(snapshot.getBlockNumber()).thenReturn(number);
      when(snapshot.getBlockHash()).thenReturn(hash);
      try {
        when(snapshot.get(any(), any())).thenAnswer(call -> OldValue.fromNullable(
            baseline.get(key(call.getArgument(0), call.getArgument(1)))));
        org.mockito.Mockito.doAnswer(call -> {
          synchronized (this) {
            releases++;
          }
          return null;
        }).when(snapshot).close();
      } catch (java.io.IOException impossible) {
        throw new AssertionError(impossible);
      }
      return snapshot;
    }

    private void ready() throws Exception {
      archive.completeServingInitialSync(target);
    }

    private String hash(long number) {
      return ByteArray.toJsonHex(blocks.get(number).getBlockId().getBytes());
    }

    private Object hashSelector(long number) {
      return map("blockHash", hash(number));
    }

    private JsonNode request(String method, Object... params) throws Exception {
      Map<String, Object> request = map("method", method);
      request.put("jsonrpc", "2.0");
      request.put("id", 1);
      request.put("params", params);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      server.handleRequest(new ByteArrayInputStream(JSON.writeValueAsBytes(request)), output);
      return JSON.readTree(output.toByteArray());
    }

    private String result(String method, Object... params) throws Exception {
      JsonNode response = request(method, params);
      assertFalse(response.toString(), response.has("error"));
      return response.get("result").asText();
    }

    private void error(String method, Object... params) throws Exception {
      error(-32602, null, method, params);
    }

    private void error(int expectedCode, String messageFragment, String method,
        Object... params) throws Exception {
      JsonNode response = request(method, params);
      assertTrue(response.toString(), response.has("error"));
      assertEquals(response.toString(), expectedCode, response.get("error").get("code").asInt());
      JsonNode data = response.get("error").get("data");
      assertTrue(response.toString(),
          data.isObject() ? data.isEmpty() : "{}".equals(data.asText()));
      if (messageFragment != null) {
        assertTrue(response.toString(),
            response.get("error").get("message").asText().contains(messageFragment));
      }
      assertFalse(response.has("result"));
    }

    @Override
    public void close() throws Exception {
      Args.getInstance().supportConstant = originalSupportConstant;
      Args.getInstance().setConstantCallTimeoutMs(originalConstantCallTimeoutMs);
      rpc.close();
      attachment.close();
    }
  }

  private static String key(String db, byte[] raw) {
    return db + ":" + ByteArray.toHexString(raw);
  }

  private static DbGroup group(String db, byte[] key, byte[] value) {
    return new DbGroup(db, Collections.singletonList(new Entry(key, OldValue.present(value))));
  }

  private static DbGroup group(String db, Entry... entries) {
    return new DbGroup(db, Arrays.asList(entries));
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
    for (String property : enabled) {
      properties.add(new Entry(property.getBytes(StandardCharsets.UTF_8),
          OldValue.present(ByteArray.fromLong(1L))));
    }
    properties.add(new Entry("latest_block_header_number".getBytes(StandardCharsets.UTF_8),
        OldValue.present(ByteArray.fromLong(Long.MAX_VALUE))));
    String[] values = {
        "UNFREEZE_DELAY_DAYS", "DYNAMIC_ENERGY_THRESHOLD",
        "DYNAMIC_ENERGY_INCREASE_FACTOR", "DYNAMIC_ENERGY_MAX_FACTOR", "ENERGY_FEE",
        "MAX_FEE_LIMIT", "MAX_CPU_TIME_OF_ONE_TX", "CURRENT_CYCLE_NUMBER"
    };
    for (String property : values) {
      properties.add(new Entry(property.getBytes(StandardCharsets.UTF_8),
          OldValue.present(ByteArray.fromLong(100L))));
    }
    return properties;
  }

  private static CommonCheckpointMaterializer authority(Authority authority) throws Exception {
    CommonCheckpointMaterializer result = mock(CommonCheckpointMaterializer.class);
    when(result.authority()).thenReturn(authority);
    when(result.inspect(any())).thenReturn(Status.PUBLISHED);
    return result;
  }

  private static CommonCheckpointPayload payload(List<BlockReverseDiff> diffs,
      StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> blocks = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      int number = (int) diff.getMeta().getBlockNumber();
      PathStateFlushTarget.BlockBinding block = mock(PathStateFlushTarget.BlockBinding.class);
      when(block.getMeta()).thenReturn(diff.getMeta());
      when(block.getParentStateRoot()).thenReturn(digest(number));
      when(block.getStateRoot()).thenReturn(digest(number + 1));
      when(block.getTransitionPayloadDigest()).thenReturn(digest(90));
      blocks.add(block);
    }
    PathStateFlushTarget path = mock(PathStateFlushTarget.class);
    when(path.getBlocks()).thenReturn(blocks);
    when(path.getParentStateRoot()).thenReturn(digest(11));
    when(path.getStateRoot()).thenReturn(digest(13));
    when(path.getStores()).thenReturn(Collections.emptyList());
    when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.createV2(digest(77), path, descriptor, Collections.emptyList());
  }
}
