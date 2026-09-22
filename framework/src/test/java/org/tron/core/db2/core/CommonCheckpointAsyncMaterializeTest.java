package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.config.args.Args;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Covers the asynchronous phase split of the common-checkpoint runtime. */
public class CommonCheckpointAsyncMaterializeTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void configure() {
    Args.setParam(new String[]{}, TestConstants.TEST_CONF);
  }

  @AfterClass
  public static void clearConfiguration() {
    Args.clearParam();
  }

  @Test(timeout = 60000)
  public void checkpointReturnsBeforeMaterializeAndPublishesAfterDrain() throws Exception {
    Path root = temporaryFolder.newFolder("async-return").toPath();
    Fixture fixture = new Fixture(root);
    CountDownLatch materializeEntered = new CountDownLatch(1);
    CountDownLatch releaseMaterialize = new CountDownLatch(1);
    CommonCheckpointRuntime runtime = fixture.runtime(stage -> {
      if (stage == CommonCheckpointRedoCoordinator.Stage.AFTER_CHAINBASE_MATERIALIZE) {
        materializeEntered.countDown();
        await(releaseMaterialize, "release materialize");
      }
    });
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          runtime.recoverBeforeServing());
      SnapshotImpl layer = fixture.appendBlock(1, hash(0), hash(1));

      CommonCheckpointTarget target = runtime.checkpointAndRebase(1);
      assertEquals(1, target.getLastBlock().getBlockNumber());
      assertTrue("background materialize must start after the caller returns",
          materializeEntered.await(5, TimeUnit.SECONDS));
      // The WAL is durable but the authorities are not published and memory is not rebased yet.
      assertEquals(CommonCheckpointRuntimeOwner.State.CHECKPOINTING, runtime.getState());
      assertTrue(Files.isRegularFile(root.resolve("wal")
          .resolve(CommonCheckpointFile.FILE_NAME)));
      assertFalse(Files.exists(root.resolve("chainbase")
          .resolve(ChainbaseCheckpointMaterializer.CURRENT_FILE)));
      assertSame(layer, fixture.database.getHead());

      releaseMaterialize.countDown();
      runtime.awaitQuiescent();
      assertEquals(CommonCheckpointRuntimeOwner.State.READY, runtime.getState());
      assertFalse(Files.exists(root.resolve("wal")
          .resolve(CommonCheckpointFile.FILE_NAME)));
      assertSame(fixture.database.getHead().getRoot(), fixture.database.getHead());
      assertEquals(1, fixture.code.syncedFlushes);
      assertArrayEquals(new byte[]{1}, fixture.code.get(new byte[]{1}));
    } finally {
      releaseMaterialize.countDown();
      runtime.close();
    }
  }

  @Test(timeout = 60000)
  public void backgroundFailureSurfacesAtNextCheckpointAndFailsClosed() throws Exception {
    Path root = temporaryFolder.newFolder("async-failure").toPath();
    Fixture fixture = new Fixture(root);
    CountDownLatch failed = new CountDownLatch(1);
    CommonCheckpointRuntime runtime = fixture.runtime(stage -> {
      if (stage == CommonCheckpointRedoCoordinator.Stage.AFTER_CHAINBASE_MATERIALIZE) {
        failed.countDown();
        throw new IOException("injected background materialize failure");
      }
    });
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          runtime.recoverBeforeServing());
      fixture.appendBlock(1, hash(0), hash(1));

      // Phase A succeeds: the WAL is durable and the caller returns before the failure.
      runtime.checkpointAndRebase(1);
      assertTrue(failed.await(5, TimeUnit.SECONDS));
      assertTrue("unretired WAL must survive for restart redo",
          Files.isRegularFile(root.resolve("wal").resolve(CommonCheckpointFile.FILE_NAME)));

      fixture.appendBlock(2, hash(1), hash(2));
      IOException failure = assertThrows(IOException.class, () -> runtime.checkpointAndRebase(1));
      assertEquals("injected background materialize failure", failure.getMessage());
      assertEquals(CommonCheckpointRuntimeOwner.State.FAILED, runtime.getState());
      assertTrue("unretired WAL must survive for restart redo",
          Files.isRegularFile(root.resolve("wal").resolve(CommonCheckpointFile.FILE_NAME)));
    } finally {
      runtime.close();
    }
  }

  @Test(timeout = 60000)
  public void backToBackCheckpointsRebaseBeforeCaptureAndKeepTargetSequence() throws Exception {
    Path root = temporaryFolder.newFolder("back-to-back").toPath();
    Fixture fixture = new Fixture(root);
    CommonCheckpointRuntime runtime = fixture.runtime(stage -> { });
    try {
      assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
          runtime.recoverBeforeServing());
      for (int number = 1; number <= 3; number++) {
        fixture.appendBlock(number, hash(number - 1), hash(number));
        CommonCheckpointTarget target = runtime.checkpointAndRebase(1);
        // A stale rebase would recapture the previous layer and break this identity.
        assertEquals(number, target.getLastBlock().getBlockNumber());
        assertArrayEquals(hash(number), target.getLastBlock().getBlockHash());
      }
      runtime.awaitQuiescent();
      assertEquals(CommonCheckpointRuntimeOwner.State.READY, runtime.getState());
      assertFalse(Files.exists(root.resolve("wal")
          .resolve(CommonCheckpointFile.FILE_NAME)));
      assertSame(fixture.database.getHead().getRoot(), fixture.database.getHead());
      assertArrayEquals(new byte[]{3}, fixture.code.get(new byte[]{1}));
      assertEquals(3, fixture.code.syncedFlushes);
    } finally {
      runtime.close();
    }
  }

  @Test(timeout = 60000)
  public void closeDrainsInFlightMaterializeBeforeReleasingAuthorities() throws Exception {
    Path root = temporaryFolder.newFolder("close-drain").toPath();
    Fixture fixture = new Fixture(root);
    CountDownLatch materializeEntered = new CountDownLatch(1);
    CountDownLatch releaseMaterialize = new CountDownLatch(1);
    CommonCheckpointRuntime runtime = fixture.runtime(stage -> {
      if (stage == CommonCheckpointRedoCoordinator.Stage.AFTER_CHAINBASE_MATERIALIZE) {
        materializeEntered.countDown();
        await(releaseMaterialize, "release materialize");
      }
    });
    assertEquals(CommonCheckpointRedoCoordinator.RecoveryAction.NO_CHECKPOINT,
        runtime.recoverBeforeServing());
    fixture.appendBlock(1, hash(0), hash(1));
    runtime.checkpointAndRebase(1);
    assertTrue(materializeEntered.await(5, TimeUnit.SECONDS));

    CountDownLatch closed = new CountDownLatch(1);
    AtomicReference<Throwable> closeFailure = new AtomicReference<>();
    Thread closer = new Thread(() -> {
      try {
        runtime.close();
      } catch (Throwable failure) {
        closeFailure.set(failure);
      } finally {
        closed.countDown();
      }
    });
    closer.start();
    try {
      assertFalse("close must drain the in-flight materialize first",
          closed.await(300, TimeUnit.MILLISECONDS));
      releaseMaterialize.countDown();
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      closer.join(5_000);
      assertNull(closeFailure.get());
      assertFalse(Files.exists(root.resolve("wal")
          .resolve(CommonCheckpointFile.FILE_NAME)));
      assertEquals(CommonCheckpointRuntimeOwner.State.CLOSED, runtime.getState());
      assertEquals(1, fixture.code.syncedFlushes);
      assertArrayEquals(new byte[]{1}, fixture.code.get(new byte[]{1}));
    } finally {
      releaseMaterialize.countDown();
      closer.join(5_000);
      runtime.close();
    }
  }

  private static void await(CountDownLatch latch, String name) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out waiting for " + name);
      }
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted waiting for " + name, failure);
    }
  }

  private static byte[] hash(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static final class Fixture {

    private final Path root;
    private final byte[] format = hash(80);
    private final MemoryDb code = new MemoryDb("code");
    private final Chainbase database = new Chainbase(new SnapshotRoot(code));

    private Fixture(Path root) {
      this.root = root;
    }

    private SnapshotImpl appendBlock(long number, byte[] parentHash, byte[] blockHash) {
      BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(number, blockHash, parentHash,
          number * 3_000L);
      PathStateSnapshotDelta path = mock(PathStateSnapshotDelta.class);
      when(path.getMeta()).thenReturn(meta);
      when(path.getParentStateRoot()).thenReturn(hash(10 + (int) number - 1));
      when(path.getStateRoot()).thenReturn(hash(10 + (int) number));
      when(path.getTransitionPayloadDigest()).thenReturn(hash(50 + (int) number));
      when(path.getStores()).thenReturn(Collections.emptyList());
      when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
      BlockReverseDiff archive = new BlockReverseDiff(meta, Collections.emptyList());
      SnapshotImpl layer = (SnapshotImpl) database.getHead().advance();
      layer.attachBlockArtifacts(meta, archive, path);
      database.setHead(layer);
      layer.put(new byte[]{1}, new byte[]{(byte) number});
      return layer;
    }

    private CommonCheckpointRuntime runtime(CommonCheckpointRedoCoordinator.FaultHook hook) {
      ChainbaseCheckpointMaterializer chainbase = new ChainbaseCheckpointMaterializer(
          root.resolve("chainbase"), format, Collections.singletonList(database));
      CommonCheckpointRedoCoordinator coordinator = new CommonCheckpointRedoCoordinator(
          new CommonCheckpointFile(root.resolve("wal")), chainbase,
          fake(Authority.PATH_STATE), fake(Authority.STATE_ARCHIVE), hook);
      return new CommonCheckpointRuntime(new CommonCheckpointRuntimeOwner(coordinator),
          Collections.singletonList(database), root.resolve("archive"), format, Engine.LEVELDB,
          (blockNumber, blockHash) -> {
            throw new IOException("latest state is intentionally unavailable");
          }, target -> () -> { });
    }

    private static CommonCheckpointMaterializer fake(Authority authority) {
      return new CommonCheckpointMaterializer() {
        private CommonCheckpointTarget materialized;
        private CommonCheckpointTarget published;

        @Override
        public Authority authority() {
          return authority;
        }

        @Override
        public Status inspect(CommonCheckpointTarget target) {
          if (target.equals(published)) {
            return Status.PUBLISHED;
          }
          return target.equals(materialized) ? Status.MATERIALIZED
              : Status.NEEDS_MATERIALIZATION;
        }

        @Override
        public void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target) {
          materialized = target;
        }

        @Override
        public void publish(CommonCheckpointTarget target) {
          published = target;
        }
      };
    }
  }

  private static final class MemoryDb implements DB<byte[], byte[]>, Flusher {

    private final String name;
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();
    private int syncedFlushes;

    private MemoryDb(String name) {
      this.name = name;
    }

    @Override
    public byte[] get(byte[] key) {
      return values.get(WrappedByteArray.of(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
      values.put(WrappedByteArray.of(key), value);
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
      Map<byte[], byte[]> copy = new LinkedHashMap<>();
      values.forEach((key, value) -> copy.put(key.getBytes(), value));
      return copy.entrySet().iterator();
    }

    @Override
    public void close() {
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
      throw new UnsupportedOperationException();
    }

    @Override
    public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
      apply(batch);
    }

    @Override
    public void flushSynced(Map<WrappedByteArray, WrappedByteArray> batch) {
      syncedFlushes++;
      apply(batch);
    }

    @Override
    public void reset() {
      values.clear();
    }

    private void apply(Map<WrappedByteArray, WrappedByteArray> batch) {
      batch.forEach((key, value) -> {
        if (value.getBytes() == null) {
          values.remove(key);
        } else {
          values.put(key, value.getBytes());
        }
      });
    }
  }
}
