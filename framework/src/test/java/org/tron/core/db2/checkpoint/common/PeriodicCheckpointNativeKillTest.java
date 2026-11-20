package org.tron.core.db2.checkpoint.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.config.args.Args;
import org.tron.core.db2.NativeCheckpointTestSupport;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.contract.CheckpointPointReader;
import org.tron.core.db2.archive.history.hot.StateArchiveHotBatchDescriptor;
import org.tron.core.db2.archive.history.v5.StateArchiveAppendCheckpointMaterializerV5;
import org.tron.core.db2.archive.serving.ServingIndexService;
import org.tron.core.db2.checkpoint.common.materialize.CommonCheckpointTarget;
import org.tron.core.db2.checkpoint.common.payload.CommonCheckpointCapture;
import org.tron.core.db2.checkpoint.common.payload.CommonCheckpointPayload;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.ChainbaseCheckpointMaterializer;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.db2.stateroot.PathStateCheckpointMaterializer;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStatePhysicalStoreSet;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;
import org.tron.core.db2.stateroot.commitment.PathStateCanonicalizer;
import org.tron.core.db2.stateroot.commitment.PathStateParticipantScope;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** SIGKILL coverage of periodic redo using real Chainbase, PathState and V5 authorities. */
public class PeriodicCheckpointNativeKillTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test(timeout = 600_000)
  public void multiBatchRedoRecoversAtPublicationAndMaterializationBoundaries() throws Exception {
    for (String point : new String[]{"MULTI_REDO", "BEFORE_REDO", "AFTER_REDO",
        "AFTER_CHAINBASE_MATERIALIZE", "AFTER_PATH_STATE_MATERIALIZE",
        "AFTER_CHAINBASE_PUBLISH", "AFTER_PATH_STATE_PUBLISH", "AFTER_ARCHIVE_PUBLISH",
        "AFTER_CHECKPOINT_RETIRE"}) {
      scenario(point, false);
    }
  }

  @Test(timeout = 360_000)
  public void partialBarriersAndAtomicPrunePreserveRecoverableData() throws Exception {
    for (String point : new String[]{"CHAIN_SYNC", "PATH_SYNC", "BEFORE_PRUNE", "AFTER_PRUNE"}) {
      scenario(point, false);
    }
  }

  @Test(timeout = 120_000)
  public void killedRecoverySealsWithoutNewBlocksAndConvergesAgain() throws Exception {
    scenario("AFTER_CHECKPOINT_RETIRE", true);
  }

  private void scenario(String point, boolean killRecovery) throws Exception {
    String name = point + (killRecovery ? "-recovery" : "");
    Path root = temporary.newFolder(name).toPath();
    int height = point.equals("MULTI_REDO") || point.equals("BEFORE_REDO") ? 2 : 3;
    boolean pruned = point.equals("AFTER_PRUNE");
    try {
      child(root, "apply", point, true);
      assertWindow(root, height, pruned);
      if (!pruned) {
        // Explicitly model loss of unsynced rows, preserving all publication markers.
        // Never damage rows covered by a durable base: the latest anchor is not a full snapshot.
        eraseChainbaseRows(root);
      }
      if (killRecovery) {
        child(root, "recover", "PATH_SYNC", true);
        assertWindow(root, height, false);
        eraseChainbaseRows(root);
      }
      child(root, "verify", Integer.toString(height), false);
      child(root, "verify", Integer.toString(height), false);
      assertWindow(root, height, true);
      Files.write(root.resolve("result.txt"), ("PASS " + name + " height=" + height
          + " prunedBeforeRecovery=" + pruned + " secondRecovery=true\n")
          .getBytes(StandardCharsets.UTF_8));
      System.out.println("PERIODIC_CRASH_PASS " + name + " height=" + height);
    } finally {
      preserveLogs(root, name);
    }
  }

  private static void assertWindow(Path root, int height, boolean pruned) throws Exception {
    try (CommonCheckpointFile file = CommonCheckpointFile.openDatabase(
        root.resolve("wal"), Engine.ROCKSDB)) {
      assertEquals(height, CommonCheckpointTarget.from(file.latestCommitted())
          .getLastBlock().getBlockNumber());
      Files.write(root.resolve("window-" + System.nanoTime() + ".txt"),
          ("height=" + height + " retained=" + file.retained().size()
              + " flushCount=" + file.windowUsage().getFlushCount()
              + " encodedBytes=" + file.windowUsage().getEncodedBytes()
              + " pending=" + file.isPresent() + "\n").getBytes(StandardCharsets.UTF_8));
      assertEquals(pruned ? 1 : height, file.retained().size());
      assertEquals(pruned ? 0 : height, file.windowUsage().getFlushCount());
      if (pruned) {
        assertEquals(0, file.windowUsage().getEncodedBytes());
      } else {
        assertTrue(file.windowUsage().getEncodedBytes() > 0);
      }
    }
  }

  private static void eraseChainbaseRows(Path root) {
    Args.setParam(new String[]{"--output-directory", root.resolve("parent-config").toString()},
        "config-test.conf");
    DB<byte[], byte[]> db = NativeCheckpointTestSupport.database(root.resolve("business"), "code");
    try {
      for (int key = 1; key <= 4; key++) {
        db.remove(bytes(key));
        assertNull(db.get(bytes(key)));
      }
    } finally {
      db.close();
    }
  }

  private static void child(Path root, String mode, String point, boolean kill) throws Exception {
    java.lang.reflect.Method method = CommonCheckpointWalKillProcessTest.class
        .getDeclaredMethod("runtimeClasspath");
    method.setAccessible(true);
    Path waiting = root.resolve("waiting");
    Files.deleteIfExists(waiting);
    Path log = root.resolve(mode + "-" + System.nanoTime() + ".log");
    Files.createDirectories(root.resolve("tmp"));
    Process process = new ProcessBuilder(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx512m", "-Djava.io.tmpdir=" + root.resolve("tmp"),
        "-Dstorage.db.engine=ROCKSDB", "-cp", (String) method.invoke(null),
        PeriodicCheckpointNativeKillTest.class.getName(), root.toString(), mode, point)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (kill) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (!Files.exists(waiting) && process.isAlive() && System.nanoTime() < deadline) {
          Thread.sleep(10);
        }
        assertTrue("fault point not reached " + mode + ":" + point + "\n" + read(log),
            Files.exists(waiting));
        assertEquals(point, read(waiting));
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        assertEquals(137, process.exitValue());
      } else {
        assertTrue("verify timeout", process.waitFor(45, TimeUnit.SECONDS));
        assertEquals("verification failed\n" + read(log), 0, process.exitValue());
      }
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]);
    String mode = args[1];
    String point = args[2];
    boolean apply = mode.equals("apply");
    Args.setParam(new String[]{"--output-directory", root.resolve("config").toString()},
        "config-test.conf");
    byte[] format = hash(88);
    PathStateParticipantScope scope = new PathStateCanonicalizer().participantScope();
    DB<byte[], byte[]> db = NativeCheckpointTestSupport.database(root.resolve("business"), "code");
    Chainbase chainbase = new Chainbase(new SnapshotRoot(db));
    CommonCheckpointFile file = CommonCheckpointFile.openDatabase(root.resolve("wal"),
        Engine.ROCKSDB);
    CommonCheckpointPayload latest = file.latestCommitted();
    CommonCheckpointTarget recovery = latest == null ? null : CommonCheckpointTarget.from(latest);
    try (PathStatePhysicalStoreSet stores = PathStatePhysicalStoreSet.open(
        root.resolve("path"), scope, Engine.ROCKSDB);
        StateArchiveAppendCheckpointMaterializerV5 archive =
            StateArchiveAppendCheckpointMaterializerV5.openForCommon(root.resolve("history"),
                format, Engine.ROCKSDB, 1,
                StateArchiveAppendCheckpointMaterializerV5.baselineHistoryDigest(1, hash(0)),
                10000, block -> BlockSnapshotMeta.forBlock(block, hash((int) block),
                    hash((int) block - 1), block * 3000), recovery)) {
      ChainbaseCheckpointMaterializer cb = new ChainbaseCheckpointMaterializer(
          root.resolve("chainbase"), format, Collections.singletonList(chainbase));
      PathStateCheckpointMaterializer ps = new PathStateCheckpointMaterializer(stores, scope,
          format);
      if (point.equals("CHAIN_SYNC")) {
        cb = spy(cb);
        doAnswer(call -> {
          call.callRealMethod();
          waitForKill(root, point);
          return null;
        }).when(cb).syncCheckpoint(any());
      }
      if (point.equals("PATH_SYNC")) {
        ps = spy(ps);
        doAnswer(call -> {
          call.callRealMethod();
          waitForKill(root, point);
          return null;
        }).when(ps).syncCheckpoint(any());
      }
      if (apply && (point.equals("BEFORE_REDO") || point.equals("AFTER_REDO"))) {
        file = spy(file);
        doAnswer(call -> {
          CommonCheckpointPayload payload = call.getArgument(0);
          if (lastHeight(payload) == 3 && point.equals("BEFORE_REDO")) {
            waitForKill(root, point);
          }
          call.callRealMethod();
          if (lastHeight(payload) == 3 && point.equals("AFTER_REDO")) {
            waitForKill(root, point);
          }
          return null;
        }).when(file).publish(any());
      }
      if (apply && (point.equals("BEFORE_PRUNE") || point.equals("AFTER_PRUNE"))) {
        file = spy(file);
        doAnswer(call -> {
          if (point.equals("BEFORE_PRUNE")) {
            waitForKill(root, point);
          }
          call.callRealMethod();
          waitForKill(root, point);
          return null;
        }).when(file).advanceDurableBase();
      }
      java.util.concurrent.atomic.AtomicInteger admittedHeight =
          new java.util.concurrent.atomic.AtomicInteger();
      CommonCheckpointRedoCoordinator coordinator = new CommonCheckpointRedoCoordinator(
          file, cb, ps, archive, stage -> {
        if (apply && stage.name().equals(point) && admittedHeight.get() == 3) {
          waitForKill(root, point);
        }
      });
      coordinator.enablePeriodicConsolidation(3, 0);
      if (apply) {
        for (int block = 1; block <= 3; block++) {
          admittedHeight.set(block);
          CommonCheckpointPayload admitted = payload(format, scope, archive, block);
          Files.write(root.resolve("expected-payload-" + block),
              CommonCheckpointTarget.from(admitted).getPayloadDigest());
          coordinator.apply(admitted);
          if (block == 2 && point.equals("MULTI_REDO")) {
            waitForKill(root, point);
          }
        }
        throw new AssertionError("requested fault point was not reached: " + point);
      } else {
        coordinator.recover();
        if (mode.equals("recover")) {
          throw new AssertionError("recovery fault point was not reached: " + point);
        }
        int height = Integer.parseInt(point);
        CommonCheckpointTarget target = CommonCheckpointTarget.from(file.latestCommitted());
        assertEquals(height, target.getLastBlock().getBlockNumber());
        assertArrayEquals(hash(height), target.getLastBlock().getBlockHash());
        assertArrayEquals(hash(10 + height), target.getStateRoot());
        assertArrayEquals(Files.readAllBytes(root.resolve("expected-payload-" + height)),
            target.getPayloadDigest());
        coordinator.requirePublished(target);
        verifyRows(db::get, height, 10);
        verifyRows(stores.participant("account")::getFlat, height, 40);
        verifyRows(stores.participant("account").nodeStore()::get, height, 70);
        verifyRows(stores.superStore().nodeStore()::get, height, 100);
        verifyHistory(archive, target, height);
        assertEquals(1, file.retained().size());
        assertEquals(0, file.windowUsage().getFlushCount());
        coordinator.recover();
        coordinator.requirePublished(target);
        verifyRows(db::get, height, 10);
        verifyRows(stores.participant("account")::getFlat, height, 40);
        verifyRows(stores.participant("account").nodeStore()::get, height, 70);
        verifyRows(stores.superStore().nodeStore()::get, height, 100);
        coordinator.close();
      }
    } finally {
      db.close();
    }
  }

  private static void verifyHistory(StateArchiveAppendCheckpointMaterializerV5 archive,
      CommonCheckpointTarget target, int height) throws Exception {
    archive.startServingAfterRecovery();
    archive.requestFastServingCatchUp();
    java.lang.reflect.Field field = archive.getClass().getDeclaredField("servingService");
    field.setAccessible(true);
    ServingIndexService service = (ServingIndexService) field.get(archive);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (service.status().getIndexedThrough() != height && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(height, service.status().getIndexedThrough());
    archive.completeServingInitialSync(target);
    try (CheckpointPointReader reader = archive.pinHistory(target)) {
      assertEquals(height, reader.getIndexedThrough());
      assertEquals(OldValue.absent(), reader.findOldValueAfter("code", bytes(1), 0).get());
      assertEquals(OldValue.present(bytes(11)),
          reader.findOldValueAfter("code", bytes(1), 1).get());
      assertEquals(OldValue.present(bytes(21)),
          reader.findOldValueAfter("code", bytes(2), 1).get());
      assertEquals(OldValue.absent(), reader.findOldValueAfter("code", bytes(4), 1).get());
      if (height == 3) {
        assertEquals(OldValue.present(bytes(12)),
            reader.findOldValueAfter("code", bytes(1), 2).get());
        assertEquals(OldValue.absent(), reader.findOldValueAfter("code", bytes(2), 2).get());
      }
    }
  }

  private static void verifyRows(Function<byte[], byte[]> get, int height, int offset) {
    assertArrayEquals(bytes(offset + height), get.apply(bytes(1)));
    if (height == 2) {
      assertNull(get.apply(bytes(2)));
    } else {
      assertArrayEquals(bytes(offset + 10 + height), get.apply(bytes(2)));
    }
    assertArrayEquals(bytes(offset + 21), get.apply(bytes(3)));
    assertArrayEquals(bytes(offset + 31), get.apply(bytes(4)));
  }

  private static CommonCheckpointPayload payload(byte[] format, PathStateParticipantScope scope,
      StateArchiveAppendCheckpointMaterializerV5 archive, int block) throws Exception {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1),
        block * 3000L);
    PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
    when(binding.getMeta()).thenReturn(meta);
    when(binding.getParentStateRoot()).thenReturn(hash(9 + block));
    when(binding.getStateRoot()).thenReturn(hash(10 + block));
    when(binding.getTransitionPayloadDigest()).thenReturn(hash(50 + block));
    PathStateFlushTarget.StoreTarget account = mock(PathStateFlushTarget.StoreTarget.class);
    when(account.getStoreId()).thenReturn(scope.require("account").getStoreId());
    when(account.getDbName()).thenReturn("account");
    when(account.getStoreRoot()).thenReturn(hash(60 + block));
    List<PathStateSnapshotDelta.Mutation> flat = pathMutations(block, 40);
    List<PathStateSnapshotDelta.Mutation> nodes = pathMutations(block, 70);
    List<PathStateSnapshotDelta.Mutation> superNodes = pathMutations(block, 100);
    when(account.getFlatMutations()).thenReturn(flat);
    when(account.getNodeMutations()).thenReturn(nodes);
    PathStateFlushTarget path = mock(PathStateFlushTarget.class);
    when(path.getBlocks()).thenReturn(Collections.singletonList(binding));
    when(path.getParentStateRoot()).thenReturn(hash(9 + block));
    when(path.getStateRoot()).thenReturn(hash(10 + block));
    when(path.getStores()).thenReturn(Collections.singletonList(account));
    when(path.getSuperNodeMutations()).thenReturn(superNodes);
    List<CommonCheckpointPayload.Mutation> mutations = new ArrayList<>();
    mutations.add(new CommonCheckpointPayload.Mutation(bytes(1), bytes(10 + block)));
    mutations.add(new CommonCheckpointPayload.Mutation(bytes(2),
        block == 2 ? null : bytes(20 + block)));
    if (block == 1) {
      mutations.add(new CommonCheckpointPayload.Mutation(bytes(3), bytes(31)));
    }
    if (block == 2) {
      mutations.add(new CommonCheckpointPayload.Mutation(bytes(4), bytes(41)));
    }
    List<Entry> old = new ArrayList<>();
    old.add(new Entry(bytes(1), block == 1 ? OldValue.absent()
        : OldValue.present(bytes(9 + block))));
    old.add(new Entry(bytes(2), block == 2 ? OldValue.present(bytes(21)) : OldValue.absent()));
    if (block == 1) {
      old.add(new Entry(bytes(3), OldValue.absent()));
    }
    if (block == 2) {
      old.add(new Entry(bytes(4), OldValue.absent()));
    }
    List<BlockReverseDiff> diffs = Collections.singletonList(new BlockReverseDiff(meta,
        Collections.singletonList(new DbGroup("code", old))));
    StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
    CommonCheckpointPayload payload = CommonCheckpointPayload.createV2(format, path, descriptor,
        Collections.singletonList(new CommonCheckpointPayload.StoreMutations("code", mutations)));
    archive.prepare(CommonCheckpointCapture.create(payload, diffs, descriptor));
    return payload;
  }

  private static List<PathStateSnapshotDelta.Mutation> pathMutations(int block, int offset) {
    List<PathStateSnapshotDelta.Mutation> result = new ArrayList<>();
    result.add(mutation(1, bytes(offset + block)));
    result.add(mutation(2, block == 2 ? null : bytes(offset + 10 + block)));
    if (block == 1) {
      result.add(mutation(3, bytes(offset + 21)));
    }
    if (block == 2) {
      result.add(mutation(4, bytes(offset + 31)));
    }
    return result;
  }

  private static PathStateSnapshotDelta.Mutation mutation(int key, byte[] value) {
    PathStateSnapshotDelta.Mutation mutation = mock(PathStateSnapshotDelta.Mutation.class);
    when(mutation.getKey()).thenReturn(bytes(key));
    when(mutation.getValue()).thenReturn(value);
    return mutation;
  }

  private static long lastHeight(CommonCheckpointPayload payload) {
    return payload.getBlocks().get(payload.getBlocks().size() - 1).getMeta().getBlockNumber();
  }

  private static byte[] bytes(int value) {
    return new byte[]{(byte) value};
  }

  private static byte[] hash(int seed) {
    byte[] value = new byte[32];
    Arrays.fill(value, (byte) seed);
    return value;
  }

  private static void waitForKill(Path root, String point) throws java.io.IOException {
    Files.write(root.resolve("waiting"), point.getBytes(StandardCharsets.UTF_8));
    for (;;) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException interrupted) {
        throw new java.io.IOException(interrupted);
      }
    }
  }

  private static String read(Path file) throws Exception {
    return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
  }

  private static void preserveLogs(Path root, String name) throws Exception {
    Path dest = Paths.get("build", "task058-periodic-crash-evidence", name);
    Files.createDirectories(dest);
    try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(root)) {
      for (Path file : files) {
        if (Files.isRegularFile(file)) {
          Files.copy(file, dest.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }
}
